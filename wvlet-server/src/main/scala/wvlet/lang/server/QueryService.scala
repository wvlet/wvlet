package wvlet.lang.server

import wvlet.lang.api.SourceLocation
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.frontend.FrontendApi.QueryInfoRequest
import wvlet.lang.api.v1.frontend.FrontendApi.QueryResponse
import wvlet.lang.api.v1.query.Column
import wvlet.lang.api.v1.query.FacetCount
import wvlet.lang.api.v1.query.QueryDetail
import wvlet.lang.api.v1.query.QueryFacets
import wvlet.lang.api.v1.query.QueryError
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.lang.api.v1.query.QueryList
import wvlet.lang.api.v1.query.QueryListRequest
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QueryResult as ApiQueryResult
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.lang.api.v1.query.QueryStatus.QUEUED
import wvlet.lang.api.v1.query.QuerySummary
import wvlet.lang.engine.exec.MemoryPool
import wvlet.lang.runner.PlanResult
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.QueryResultList
import wvlet.lang.runner.TableRows
import wvlet.uni.log.LogSupport
import wvlet.uni.util.ThreadUtil
import wvlet.uni.util.ULID

import java.time.Duration
import java.time.Instant
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
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
  *   - While the in-process engine's queries hold more than `memoryPool`'s capacity, queued queries
  *     wait for running ones to finish; with none running, the next one starts anyway, since only
  *     running queries release memory.
  *   - With the pool's low-memory killer on, a timer checks the pool every
  *     [[QueryService.KillerCheckMillis]] ms, so a query holding memory without reserving more is
  *     still killed once the pool stays over its capacity.
  */
class QueryService(
    sessions: ScriptRunnerSessions,
    val config: QueryManagerConfig,
    listener: QueryEventListener,
    idGenerator: QueryIdGenerator,
    val memoryPool: MemoryPool = MemoryPool.process
) extends LogSupport
    with AutoCloseable:

  /** The default limits, with events dropped */
  def this(sessions: ScriptRunnerSessions) = this(
    sessions,
    QueryManagerConfig(),
    QueryEventListener.noOp,
    QueryIdGenerator()
  )

  /** Id of the server instance issuing this service's query ids */
  def serverId: String = idGenerator.serverId

  // `seq` orders queries by admission (ids can tie within a second); `recorder` collects the SQL,
  // engine plans, and memory the query's statements report
  private case class Submitted(
      queryId: String,
      seq: Long,
      request: QueryRequest,
      sessionKey: String,
      createdAt: Instant,
      recorder: QueryRecorder
  ):
    // For the list's case-insensitive text filter, computed once per query
    lazy val lowerQuery: String = request.query.toLowerCase(Locale.ROOT)

  private val workers: ExecutorService = Executors.newFixedThreadPool(
    config.maxRunningQueries,
    ThreadUtil.newDaemonThreadFactory("wvlet-query-service")
  )

  // Checks the pool's low-memory killer: reservations check it too, but a query that holds its
  // memory without reserving more never does
  private val killerTimer: Option[ScheduledExecutorService] = memoryPool
    .killDelay
    .map { _ =>
      val t = Executors.newSingleThreadScheduledExecutor(
        ThreadUtil.newDaemonThreadFactory("wvlet-low-memory-killer")
      )
      val check: Runnable =
        () =>
          try
            memoryPool
              .killLargestIfStillOver()
              .foreach(c =>
                warn(s"Low-memory killer: failing the query holding ${c.reservedBytes} bytes")
              )
          catch
            case NonFatal(e) =>
              warn(s"Low-memory killer check failed: ${e.getMessage}", e)
      t.scheduleWithFixedDelay(
        check,
        QueryService.KillerCheckMillis,
        QueryService.KillerCheckMillis,
        TimeUnit.MILLISECONDS
      )
      t
    }

  // Readable without the lock; status transitions go through `transition`
  private val queryMap = ConcurrentHashMap[String, QueryInfo]()
  // The request of each query in `queryMap`, readable without the lock
  private val submittedMap = ConcurrentHashMap[String, Submitted]()
  // Guarded by `lock`
  private var nextSeq = 0L

  // Guarded by `lock`
  private val lock   = Object()
  private val queued = java.util.LinkedHashMap[String, Submitted]()
  // Dispatched query -> its worker thread, once the worker picks the query up
  private val running      = mutable.HashMap.empty[String, Option[Thread]]
  private val busySessions = mutable.HashSet.empty[String]
  private val finished     = mutable.Queue.empty[(String, Instant)]
  private val requestIndex = mutable.HashMap.empty[ULID, String]
  private val requestOf    = mutable.HashMap.empty[String, ULID]
  // Admitted, but not yet in `queued` while their CREATED event goes out
  private var admitting = 0

  override def close(): Unit =
    killerTimer.foreach(_.shutdownNow())
    workers.shutdownNow()
    // Let interrupted workers report their completion before the listener closes
    workers.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
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
          val waiting = queued.size + admitting
          if waiting >= config.maxQueuedQueries then
            throw StatusCode
              .RESOURCE_EXHAUSTED
              .newException(
                s"Too many queued queries (${waiting}). Retry after running queries finish"
              )
          val queryId = idGenerator.next()
          val info    = QueryInfo(
            queryId = queryId,
            pageToken = "0",
            status = QUEUED,
            statusCode = StatusCode.OK,
            createdAt = now
          )
          val s = Submitted(queryId, nextSeq, request, sessionKeyOf(request), now, QueryRecorder())
          nextSeq += 1
          submittedMap.put(queryId, s)
          queryMap.put(queryId, info)
          requestIndex += request.requestId -> queryId
          requestOf += queryId              -> request.requestId
          admitting += 1
          Right((s, info))
    }

    admitted match
      case Left(existing) =>
        QueryResponse(queryId = existing, requestId = request.requestId)
      case Right((s, info)) =>
        // CREATED goes out before the query can be dispatched, so listeners see events in order.
        // A cancel landing in between marks it CANCELED; the worker then skips it and reports
        // its completion
        emit(event(QueryEventType.CREATED, s, info, query = Some(request.query)))
        lock.synchronized {
          admitting -= 1
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
        // Interrupt the worker only once it runs the query: a dispatched query not yet picked up
        // sees CANCELED when it starts, and its worker still releases the slot
        lock.synchronized(running.get(queryId).flatten.foreach(_.interrupt()))
    canceled

  /**
    * The kept queries matching `request`'s filters, newest first, one page at a time. The page
    * token is the admission number of the page's last query, so pages stay stable while new queries
    * arrive
    */
  def list(request: QueryListRequest): QueryList =
    val limit = request.limit.max(1).min(QueryListRequest.MaxLimit)
    val after = request
      .pageToken
      .map { t =>
        t.toLongOption
          .getOrElse {
            throw StatusCode.INVALID_ARGUMENT.newException(s"Invalid page token: ${t}")
          }
      }
    val text = request.text.map(_.toLowerCase(Locale.ROOT)).filter(_.nonEmpty)
    val now  = Instant.now()
    val kept = mutable.ArrayBuffer.empty[(Submitted, QueryInfo)]
    submittedMap.forEach { (id, s) =>
      val info = queryMap.get(id)
      if info != null then
        kept += s -> info
    }
    val matched = kept.filter((s, info) => matches(request, text, s, info))
    val sorted  = matched.sortBy(-_._1.seq)
    val page    = sorted.iterator.filter((s, _) => after.forall(s.seq < _)).take(limit).toSeq
    val more    = page.nonEmpty && sorted.last._1.seq < page.last._1.seq
    QueryList(
      queries = page.map((s, info) => summaryOf(s, info, now)),
      totalMatched = sorted.size,
      nextPageToken = Option.when(more)(page.last._1.seq.toString),
      facets = facetsOf(request, text, kept)
    )

  end list

  /**
    * Counts per facet value over the queries matching every filter but the facet's own, in one pass
    * over the kept queries
    */
  private def facetsOf(
      r: QueryListRequest,
      text: Option[String],
      kept: collection.Seq[(Submitted, QueryInfo)]
  ): QueryFacets =
    val statusCounts  = mutable.HashMap.empty[String, Int]
    val sessionCounts = mutable.HashMap.empty[String, Int]
    val profileCounts = mutable.HashMap.empty[String, Int]
    def add(m: mutable.HashMap[String, Int], key: Option[String]): Unit = key.foreach(k =>
      m.update(k, m.getOrElse(k, 0) + 1)
    )
    kept.foreach { (s, info) =>
      val statusOk  = r.statuses.isEmpty || r.statuses.contains(info.status)
      val sessionOk = r.sessionId.forall(id => s.request.sessionId.contains(id))
      val profileOk = r.profile.forall(p => s.request.profile.contains(p))
      // Time and text apply to every facet
      if matches(r.copy(statuses = Nil, sessionId = None, profile = None), text, s, info) then
        if sessionOk && profileOk then
          add(statusCounts, Some(info.status.toString))
        if statusOk && profileOk then
          add(sessionCounts, s.request.sessionId)
        if statusOk && sessionOk then
          add(profileCounts, s.request.profile)
    }
    def sorted(m: mutable.HashMap[String, Int]): Seq[FacetCount] = m
      .toSeq
      .sortBy((v, n) => (-n, v))
      .map((v, n) => FacetCount(v, n))
    QueryFacets(
      statuses = sorted(statusCounts),
      sessions = sorted(sessionCounts).take(QueryFacets.MaxValues),
      profiles = sorted(profileCounts).take(QueryFacets.MaxValues)
    )

  end facetsOf

  private def matches(
      r: QueryListRequest,
      text: Option[String],
      s: Submitted,
      info: QueryInfo
  ): Boolean =
    (r.statuses.isEmpty || r.statuses.contains(info.status)) &&
      r.sessionId.forall(id => s.request.sessionId.contains(id)) &&
      r.profile.forall(p => s.request.profile.contains(p)) &&
      r.createdAfter.forall(t => !s.createdAt.isBefore(t)) &&
      r.createdBefore.forall(t => s.createdAt.isBefore(t)) &&
      text.forall(t => s.lowerQuery.contains(t))

  /**
    * The query's text, SQL, engine plans, errors, and memory
    *
    * @throws WvletLangException
    *   INVALID_ARGUMENT for an unknown or evicted query
    */
  def detail(queryId: String): QueryDetail =
    val info = infoOf(queryId)
    val s    = Option(submittedMap.get(queryId)).getOrElse {
      throw StatusCode.INVALID_ARGUMENT.newException(s"Query not found: ${queryId}")
    }
    QueryDetail(
      summary = summaryOf(s, info, Instant.now()),
      requestId = s.request.requestId.toString,
      query = s.request.query,
      sql = s.recorder.sql,
      enginePlans = s.recorder.enginePlans,
      errors = info.errors
    )

  private def summaryOf(s: Submitted, info: QueryInfo, now: Instant): QuerySummary =
    def millis(from: Instant, to: Instant): Long = Duration.between(from, to).toMillis
    val leftQueueAt                              = info.startedAt.orElse(info.completedAt)
    QuerySummary(
      queryId = s.queryId,
      status = info.status,
      sessionId = s.request.sessionId,
      profile = s.request.profile,
      queryHead = s.request.query.take(QuerySummary.HeadLength),
      createdAt = s.createdAt,
      startedAt = info.startedAt,
      completedAt = info.completedAt,
      queuedMillis = millis(s.createdAt, leftQueueAt.getOrElse(now)),
      runMillis = info.startedAt.map(st => millis(st, info.completedAt.getOrElse(now))),
      resultRows = info.result.map(_.totalRows),
      errorMessage = info.errors.headOption.map(_.message),
      memory = s.recorder.memory
    )

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

  /** Start queued queries while slots and memory are free; the caller holds `lock` */
  private def dispatch(): Unit =
    var more = !workers.isShutdown
    while more && running.size < config.maxRunningQueries &&
      (running.isEmpty || !memoryPool.isOverCapacity)
    do
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
          running += s.queryId -> None
          val task: Runnable = () => runAndRelease(s)
          workers.execute(task)
        case None =>
          more = false

  private def runAndRelease(s: Submitted): Unit =
    lock.synchronized(running(s.queryId) = Some(Thread.currentThread()))
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
        // An interrupt from a cancel that landed after the statement returned must not leak
        Thread.interrupted()
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
      submittedMap.remove(queryId)
      requestOf.remove(queryId).foreach(requestIndex.remove)

  private def runQuery(s: Submitted): Unit =
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
      // The recorder collects the SQL, engine plans, and memory of the query's statements
      runner.runStatement(request)(using s.recorder)
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
        statusCode = errorReport
          .headOption
          .map(_.statusCode)
          .getOrElse(StatusCode.NON_RETRYABLE_INTERNAL_ERROR),
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

object QueryService:
  /** How often the low-memory killer's timer checks the pool */
  val KillerCheckMillis: Long = 500L
