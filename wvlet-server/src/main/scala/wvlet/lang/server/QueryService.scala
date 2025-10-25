package wvlet.lang.server

import wvlet.airframe.control.ThreadUtil
import wvlet.airframe.ulid.ULID
import wvlet.lang.api.{SourceLocation, StatusCode, WvletLangException}
import wvlet.lang.api.v1.frontend.FrontendApi.{QueryInfoRequest, QueryResponse}
import wvlet.lang.api.v1.query.{QueryError, QueryInfo, QueryRequest, QueryResult, QueryStatus}
import wvlet.lang.api.v1.query.QueryStatus.{QUEUED, RUNNING}
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.runner.QueryExecutor
import wvlet.lang.runner.WvletScriptRunner
import wvlet.lang.runner.connector.DBConnector
import wvlet.log.LogSupport

import java.time.Instant
import java.util.concurrent.{ConcurrentHashMap, Executors}
import scala.jdk.CollectionConverters.*

class QueryService(scriptRunner: WvletScriptRunner) extends LogSupport with AutoCloseable:

  private val threadManager = Executors.newCachedThreadPool(
    ThreadUtil.newDaemonThreadFactory("wvlet-query-service")
  )

  private val queryMap = ConcurrentHashMap[ULID, QueryInfo]().asScala

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
    threadManager.submit(
      new Runnable:
        override def run: Unit = runQuery(queryId, request)
    )
    QueryResponse(queryId = queryId, requestId = request.requestId)

  def fetchNext(request: QueryInfoRequest): QueryInfo =
    trace(s"Fetching query info: ${request}")
    // Fetch the query info
    queryMap
      .get(request.queryId)
      .getOrElse {
        throw StatusCode.INVALID_ARGUMENT.newException(s"Query not found: ${request.queryId}")
      }

  private def runQuery(queryId: ULID, request: QueryRequest)(using
      queryProgressMonitor: QueryProgressMonitor = QueryProgressMonitor.noOp
  ): Unit =
    var lastInfo = queryMap(queryId)
    lastInfo = lastInfo.copy(
      pageToken = "1",
      status = QueryStatus.RUNNING,
      startedAt = Some(Instant.now())
    )
    queryMap += queryId -> lastInfo

    val queryResult = scriptRunner.runStatement(request)
    if queryResult.isSuccess then
      // TODO Support pagination
      // TODO Return the query result
      val preview = queryResult.toPrettyBox()
      queryMap += queryId ->
        lastInfo.copy(
          pageToken = "2",
          status = QueryStatus.FINISHED,
          completedAt = Some(Instant.now()),
          preview = Some(preview)
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

      queryMap += queryId ->
        lastInfo.copy(
          pageToken = "2",
          status = QueryStatus.FAILED,
          statusCode = errorReport.head.statusCode,
          completedAt = Some(Instant.now()),
          errors = errorReport
        )
    end if

  end runQuery

end QueryService
