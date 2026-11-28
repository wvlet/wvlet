package wvlet.lang.server

import wvlet.lang.api.SourceLocation
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.frontend.FrontendApi.QueryInfoRequest
import wvlet.lang.api.v1.frontend.FrontendApi.QueryResponse
import wvlet.lang.api.v1.query.Column
import wvlet.lang.api.v1.query.QueryError
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QueryResult as ApiQueryResult
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.lang.api.v1.query.QueryStatus.QUEUED
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.runner.PlanResult
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.QueryResultList
import wvlet.lang.runner.TableRows
import wvlet.uni.log.LogSupport
import wvlet.uni.util.ThreadUtil
import wvlet.uni.util.ULID

import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import scala.collection.mutable
import scala.util.control.NonFatal

/** Query counts of a [[QueryService]] */
case class QueryServiceStats(running: Int, queued: Int, finished: Int)

/**
  * Runs submitted queries with bounded admission:
  *
  *   - At most `maxRunningQueries` queries run at once; others wait QUEUED, and submissions past
  *     `maxQueuedQueries` fail with RESOURCE_EXHAUSTED.
  *   - Queries of one session run one at a time, in submission order (the compiler of a session's
  *     runner is not thread-safe). A busy session never holds a worker: the dispatcher starts the
  *     oldest queued query whose session is idle.
  *   - Each query reports CREATED, STARTED, and COMPLETED [[QueryEvent]]s to the listener.
  *   - Finished queries stay readable by id for `finishedQueryRetention`, and at most
  *     `maxFinishedQueries` of them are kept.
  *   - Resubmitting a request id returns the query it started, while that query is kept.
  */
class QueryService(
    sessions: ScriptRunnerSessions,
    config: QueryManagerConfig,
    listener: QueryEventListener,
    idGenerator: QueryIdGenerator
) extends LogSupport
    with AutoCloseable:

  /** The default limits, with events logged to the debug log only */
  def this(sessions: ScriptRunnerSessions) = this(
    sessions,
    QueryManagerConfig(),
    QueryEventListener.noOp,
    QueryIdGenerator()
  )

  private case class Submitted(
      queryId: String,
      request: QueryRequest,
      sessionKey: String,
      createdAt: Instant
  )

  private val workers: ExecutorService = Executors.newFixedThreadPool(
    config.maxRunningQueries,
    ThreadUtil.newDaemonThreadFactory("wvlet-query-service")
  )

  // Readable without the lock; status transitions go through `transition`
  private val queryMap = ConcurrentHashMap[String, QueryInfo]()

  // Guarded by `lock`
  private val lock         = Object()
  private val queued       = java.util.LinkedHashMap[String, Submitted]()
  private val running      = mutable.HashMap.empty[String, java.util.concurrent.Future[?]]
  private val busySessions = mutable.HashSet.empty[String]
  private val finished     = mutable.Queue.empty[(String, Instant)]
  private val requestIndex = mutable.HashMap.empty[ULID, String]
  private val requestOf    = mutable.HashMap.empty[String, ULID]

  override def close(): Unit =
    workers.shutdownNow()
    listener.close()

  /** Current running, queued, and kept finished query counts */
  def stats: QueryServiceStats = lock.synchronized {
    QueryServiceStats(running = running.size, queued = queued.size, finished = finished.size)
  }

  /**
    * Admit a query: it waits QUEUED until a slot and its session are free.
    *
    * @throws WvletLangException
    *   RESOURCE_EXHAUSTED if `maxQueuedQueries` queries are already waiting
    */
  def enqueue(request: QueryRequest): QueryResponse =
    val now      = Instant.now()
    val admitted = lock.synchronized {
      evictFinished(now)
      requestIndex.get(request.requestId) match
        case Some(existing) =>
          debug(s"Request ${request.requestId} was already submitted as query ${existing}")
          Left(existing)
        case None =>
          if queued.size >= config.maxQueuedQueries then
            throw StatusCode
              .RESOURCE_EXHAUSTED
              .newException(
                s"Too many queued queries (${queued.size}). Retry after running queries finish"
              )
          val queryId = idGenerator.next()
          queryMap.put(
            queryId,
            QueryInfo(
              queryId = queryId,
              pageToken = "0",
              status = QUEUED,
              statusCode = StatusCode.OK,
              createdAt = now
            )
          )
          requestIndex += request.requestId -> queryId
          requestOf += queryId              -> request.requestId
          Right(Submitted(queryId, request, sessionKeyOf(request), now))
    }

    admitted match
      case Left(existing) =>
        QueryResponse(queryId = existing, requestId = request.requestId)
      case Right(s) =>
        // CREATED goes out before the query can be dispatched, so listeners see events in order.
        // A cancel landing in between marks it CANCELED; the worker then skips it and reports
        // its completion
        emit(event(QueryEventType.CREATED, s, queryMap.get(s.queryId), query = Some(request.query)))
        lock.synchronized {
          queued.put(s.queryId, s)
          dispatch()
        }
        QueryResponse(queryId = s.queryId, requestId = request.requestId)

  end enqueue

  def fetchNext(request: QueryInfoRequest): QueryInfo =
    trace(s"Fetching query info: ${request}")
    infoOf(request.queryId)

  /**
    * Best-effort cancellation: the query's terminal status becomes CANCELED right away (later
    * completion of the underlying statement is discarded) and the worker thread is interrupted. A
    * queued query is dropped from the queue and never starts. A running query keeps its slot until
    * its statement returns. Cancelling an already-finished query returns its final state unchanged.
    */
  def cancel(queryId: String): QueryInfo =
    val dropped = lock.synchronized {
      Option(queued.remove(queryId))
    }
    val now      = Instant.now()
    val canceled =
      transition(queryId) { info =>
        Option.when(!info.status.isFinished)(
          info.copy(pageToken = "2", status = QueryStatus.CANCELED, completedAt = Some(now))
        )
      }
    dropped match
      case Some(s) =>
        // Never dispatched, so no worker reports its completion
        emit(event(QueryEventType.COMPLETED, s, canceled))
        lock.synchronized(markFinished(queryId, now))
      case None =>
        lock.synchronized(running.get(queryId)).foreach(_.cancel(true))
    canceled

  private def infoOf(queryId: String): QueryInfo = Option(queryMap.get(queryId)).getOrElse {
    throw StatusCode.INVALID_ARGUMENT.newException(s"Query not found: ${queryId}")
  }

  /**
    * Atomically replace a query's info with `f(current)` when it returns Some; returns the info
    * after the call
    */
  private def transition(queryId: String)(f: QueryInfo => Option[QueryInfo]): QueryInfo =
    val updated = queryMap.computeIfPresent(queryId, (_, current) => f(current).getOrElse(current))
    if updated == null then
      throw StatusCode.INVALID_ARGUMENT.newException(s"Query not found: ${queryId}")
    updated

  /** Record a terminal state unless the query was already cancelled */
  private def complete(queryId: String)(f: QueryInfo => QueryInfo): QueryInfo =
    transition(queryId)(current => Option.when(current.status != QueryStatus.CANCELED)(f(current)))

  // Same key as ScriptRunnerSessions.runnerFor, except a profile equal to the server default stays
  // explicit: both keys then share one runner, which `runner.synchronized` still serializes
  private def sessionKeyOf(request: QueryRequest): String =
    val base = request.sessionId.getOrElse(ScriptRunnerSessions.DefaultSessionKey)
    request.profile.map(p => s"${base}@${p}").getOrElse(base)

  /** Start queued queries while slots are free; the caller holds `lock` */
  private def dispatch(): Unit =
    var more = !workers.isShutdown
    while more && running.size < config.maxRunningQueries do
      val it   = queued.values().iterator()
      var next = Option.empty[Submitted]
      while next.isEmpty && it.hasNext do
        val s = it.next()
        if !busySessions.contains(s.sessionKey) then
          next = Some(s)
      next match
        case Some(s) =>
          queued.remove(s.queryId)
          busySessions += s.sessionKey
          val task: Runnable = () => runAndRelease(s)
          running += s.queryId -> workers.submit(task)
        case None =>
          more = false

  private def runAndRelease(s: Submitted): Unit =
    try
      runQuery(s)
    catch
      case NonFatal(e) =>
        warn(s"Query ${s.queryId} failed unexpectedly: ${e.getMessage}", e)
        failQuery(s.queryId, Seq(e))
    finally
      val now = Instant.now()
      emit(event(QueryEventType.COMPLETED, s, queryMap.get(s.queryId)))
      lock.synchronized {
        running -= s.queryId
        busySessions -= s.sessionKey
        markFinished(s.queryId, now)
        dispatch()
      }

  /** The caller holds `lock` */
  private def markFinished(queryId: String, now: Instant): Unit =
    finished.enqueue(queryId -> now)
    evictFinished(now)

  /** Drop finished queries past the retention or the count limit; the caller holds `lock` */
  private def evictFinished(now: Instant): Unit =
    val cutoff = now.minusMillis(config.finishedQueryRetention.toMillis)
    while finished.nonEmpty &&
      (finished.size > config.maxFinishedQueries || finished.head._2.isBefore(cutoff))
    do
      val (queryId, _) = finished.dequeue()
      queryMap.remove(queryId)
      requestOf.remove(queryId).foreach(requestIndex.remove)

  private def runQuery(s: Submitted)(using
      queryProgressMonitor: QueryProgressMonitor = QueryProgressMonitor.noOp
  ): Unit =
    val queryId = s.queryId
    val request = s.request
    // A query cancelled after dispatch but before this point must not start
    val started =
      complete(queryId)(
        _.copy(pageToken = "1", status = QueryStatus.RUNNING, startedAt = Some(Instant.now()))
      )
    if started.status != QueryStatus.RUNNING then
      return
    emit(event(QueryEventType.STARTED, s, started))

    // Route the query to the runner owning the client's session (and requested profile), so
    // `use` statements switch the engine/catalog only for that session. The dispatcher already
    // runs one query per session at a time; the lock also covers two session keys that resolve
    // to the same runner
    val runner      = sessions.runnerFor(request.sessionId, request.profile)
    val queryResult = runner.synchronized {
      runner.runStatement(request)
    }
    if queryResult.isSuccess then
      val preview = queryResult.toPrettyBox()
      // The final tabular result (row count already bounded by the runner's row limit /
      // request.maxRows); test and command outcomes have no tabular shape
      val result = lastTableRows(queryResult).map(toApiResult)
      complete(queryId)(
        _.copy(
          pageToken = "2",
          status = QueryStatus.FINISHED,
          completedAt = Some(Instant.now()),
          result = result,
          preview = Some(preview)
        )
      )
    else
      failQuery(queryId, queryResult.getAllErrors)

  end runQuery

  private def failQuery(queryId: String, errors: Seq[Throwable]): Unit =
    val errorReport: List[QueryError] =
      errors
        .map {
          case e: WvletLangException =>
            QueryError(e.statusCode, e.getMessage, e.sourceLocation, Some(e))
          case other: Throwable =>
            QueryError(
              StatusCode.NON_RETRYABLE_INTERNAL_ERROR,
              other.getMessage,
              SourceLocation.NoSourceLocation,
              Some(other)
            )
        }
        .toList
    complete(queryId)(
      _.copy(
        pageToken = "2",
        status = QueryStatus.FAILED,
        statusCode = errorReport.headOption.map(_.statusCode).getOrElse(StatusCode.OK),
        completedAt = Some(Instant.now()),
        errors = errorReport
      )
    )

  private def event(
      eventType: QueryEventType,
      s: Submitted,
      info: QueryInfo,
      query: Option[String] = None
  ): QueryEvent =
    def millis(from: Instant, to: Instant): Long = Duration.between(from, to).toMillis
    // A query leaves the queue when it starts, or when it is cancelled before starting
    val leftQueueAt = info.startedAt.orElse(info.completedAt)
    QueryEvent(
      eventType = eventType,
      queryId = s.queryId,
      requestId = s.request.requestId.toString,
      sessionId = s.request.sessionId,
      profile = s.request.profile,
      status = info.status,
      statusCode = info.statusCode,
      createdAt = s.createdAt,
      startedAt = info.startedAt,
      completedAt = info.completedAt,
      queuedMillis = leftQueueAt.map(millis(s.createdAt, _)),
      runMillis =
        for
          st <- info.startedAt
          c  <- info.completedAt
        yield millis(st, c),
      resultRows = info.result.map(_.totalRows),
      errorMessage = info.errors.headOption.map(_.message),
      query = query
    )

  end event

  private def emit(e: QueryEvent): Unit =
    try
      listener.onEvent(e)
    catch
      case NonFatal(ex) =>
        warn(s"Query event listener failed on ${e.eventType} of ${e.queryId}: ${ex.getMessage}")

  private def lastTableRows(r: QueryResult): Option[TableRows] =
    r match
      case t: TableRows =>
        Some(t)
      case l: QueryResultList =>
        l.list.reverseIterator.flatMap(x => lastTableRows(x)).nextOption()
      case p: PlanResult =>
        lastTableRows(p.result)
      case _ =>
        None

  private def toApiResult(t: TableRows): ApiQueryResult =
    val fields = t.schema.fields
    ApiQueryResult(
      schema = fields.map(f => Column(f.name.name, f.dataType.typeDescription)).toSeq,
      rows = t.rows.map(row => fields.map(f => row.getOrElse(f.name.name, null)).toSeq).toSeq,
      actualTotalRows = Some(t.totalRows)
    )

end QueryService
