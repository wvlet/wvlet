package wvlet.lang.ui.editor

import wvlet.airframe.rx.{Cancelable, Rx}
import wvlet.lang.api.v1.frontend.FrontendApi.{QueryInfoRequest, QueryRequest, QueryResponse}
import wvlet.lang.api.v1.frontend.FrontendRPC.RPCAsyncClient
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.log.LogSupport

import java.util.concurrent.TimeUnit
import scala.util.{Failure, Success}

class QueryResultReader(rpcClient: RPCAsyncClient) extends LogSupport:

  def submitQuery(query: String, isTestRun: Boolean): Unit = rpcClient
    .FrontendApi
    .submitQuery(QueryRequest(query = query, isDebugRun = isTestRun))
    .map { resp =>
      fetchQueryResult(resp)
    }
    .run()

  def fetchQueryResult(query: QueryResponse): Unit =

    val lst = List.newBuilder[QueryInfo]

    def getQueryInfo(page: Int): Rx[QueryInfo] = rpcClient
      .FrontendApi
      .getQueryInfo(QueryInfoRequest(queryId = query.queryId, pageToken = s"${page}"))
      .tap(queryInfo =>
        ConsoleLog.write(s"Query: ${queryInfo.queryId} ${queryInfo.status}")
        lst += queryInfo
      )

    def loop(page: Int): Rx[QueryInfo] = getQueryInfo(page).flatMap { queryInfo =>
      if queryInfo.status.isFinished then
        Rx.single(queryInfo)
      else
        Rx.delay(300, TimeUnit.MILLISECONDS).flatMap(_ => loop(page + 1))
    }

    val rx = loop(1).tapOnFailure { e =>
      ConsoleLog.writeError(s"Failed to fetch query result: ${e.getMessage}")
    }
    rx.run { _ =>
      val queryInfo = lst.result()
      trace(queryInfo)

      // Show error message if exists
      queryInfo
        .lastOption
        .flatMap(_.error)
        .map { err =>
          ConsoleLog.writeError(s"Query failed: ${err.message}")
        }

      // Show query result preview if exists
      queryInfo
        .lastOption
        .flatMap(x => x.preview)
        .filter(_.trim.nonEmpty)
        .foreach { preview =>
          PreviewWindow.previewResult := preview
        }
    }

  end fetchQueryResult

end QueryResultReader
