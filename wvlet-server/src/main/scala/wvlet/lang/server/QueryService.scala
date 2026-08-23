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

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import scala.jdk.CollectionConverters.*

class QueryService(sessions: ScriptRunnerSessions) extends LogSupport with AutoCloseable:

  private val threadManager = Executors.newCachedThreadPool(
    ThreadUtil.newDaemonThreadFactory("wvlet-query-service")
  )

  private val queryMap       = ConcurrentHashMap[ULID, QueryInfo]().asScala
  private val runningQueries = ConcurrentHashMap[ULID, java.util.concurrent.Future[?]]().asScala

  override def close(): Unit =
    // Close the query service
    threadManager.shutdownNow()

  def enqueue(request: QueryRequest): QueryResponse =
    // Enqueue the query request
    val queryId        = ULID.newULID
    val firstQueryInfo = QueryInfo(
      queryId = queryId,
      pageToken = "0",
      status = QUEUED,
      statusCode = StatusCode.OK,
      createdAt = Instant.now()
    )
    queryMap += queryId -> firstQueryInfo
    val future = threadManager.submit(
      new Runnable:
        override def run: Unit =
          try runQuery(queryId, request)
          finally runningQueries -= queryId
    )
    runningQueries += queryId -> future
    QueryResponse(queryId = queryId, requestId = request.requestId)

  def fetchNext(request: QueryInfoRequest): QueryInfo =
    trace(s"Fetching query info: ${request}")
    // Fetch the query info
    queryMap
      .get(request.queryId)
      .getOrElse {
        throw StatusCode.INVALID_ARGUMENT.newException(s"Query not found: ${request.queryId}")
      }

  /**
    * Best-effort cancellation: the query's terminal status becomes CANCELED right away (later
    * completion of the underlying statement is discarded by [[completeQuery]]'s guard) and the
    * worker thread is interrupted. Cancelling an already-finished query returns its final state
    * unchanged.
    */
  def cancel(queryId: ULID): QueryInfo =
    val info = queryMap
      .get(queryId)
      .getOrElse {
        throw StatusCode.INVALID_ARGUMENT.newException(s"Query not found: ${queryId}")
      }
    if info.status.isFinished then
      info
    else
      val canceled = info.copy(
        pageToken = "2",
        status = QueryStatus.CANCELED,
        completedAt = Some(Instant.now())
      )
      queryMap += queryId -> canceled
      runningQueries.get(queryId).foreach(_.cancel(true))
      canceled

  /**
    * Record the terminal state of a query unless it was already cancelled — a cancelled query's
    * CANCELED status must survive the racing completion of its worker thread
    */
  private def completeQuery(queryId: ULID, f: QueryInfo => QueryInfo): Unit = queryMap
    .get(queryId)
    .foreach { current =>
      if current.status != QueryStatus.CANCELED then
        queryMap += queryId -> f(current)
    }

  private def runQuery(queryId: ULID, request: QueryRequest)(using
      queryProgressMonitor: QueryProgressMonitor = QueryProgressMonitor.noOp
  ): Unit =
    // A query cancelled while still queued must not start (and the guarded transition keeps
    // the CANCELED status from being overwritten by this RUNNING update)
    if queryMap.get(queryId).exists(_.status == QueryStatus.CANCELED) then
      return
    completeQuery(
      queryId,
      _.copy(pageToken = "1", status = QueryStatus.RUNNING, startedAt = Some(Instant.now()))
    )

    // Route the query to the runner owning the client's session (and requested profile), so
    // `use` statements switch the engine/catalog only for that session. Statements of ONE
    // session are serialized on its runner: the compiler inside is not thread-safe, and two
    // concurrent statements in the same session used to race it (queries of different
    // sessions still run in parallel)
    val runner      = sessions.runnerFor(request.sessionId, request.profile)
    val queryResult = runner.synchronized {
      runner.runStatement(request)
    }
    if queryResult.isSuccess then
      val preview = queryResult.toPrettyBox()
      // The final tabular result (row count already bounded by the runner's row limit /
      // request.maxRows); test and command outcomes have no tabular shape
      val result = lastTableRows(queryResult).map(toApiResult)
      completeQuery(
        queryId,
        _.copy(
          pageToken = "2",
          status = QueryStatus.FINISHED,
          completedAt = Some(Instant.now()),
          result = result,
          preview = Some(preview)
        )
      )
    else
      val errors: Seq[Throwable]        = queryResult.getAllErrors
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

      completeQuery(
        queryId,
        _.copy(
          pageToken = "2",
          status = QueryStatus.FAILED,
          statusCode = errorReport.head.statusCode,
          completedAt = Some(Instant.now()),
          errors = errorReport
        )
      )
    end if

  end runQuery

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
