package wvlet.lang.ui.editor

import wvlet.airframe.rx.{Cancelable, Rx, RxVar}
import wvlet.lang.api.v1.frontend.FrontendApi.{QueryInfoRequest, QueryRequest, QueryResponse}
import wvlet.lang.api.v1.frontend.FrontendRPC.RPCAsyncClient
import wvlet.lang.api.v1.query.{QueryError, QueryInfo}
import wvlet.log.LogSupport

import java.util.concurrent.TimeUnit
import scala.util.{Failure, Success}

class QueryResultReader(rpcClient: RPCAsyncClient, errorReports: RxVar[Seq[QueryError]])
    extends LogSupport:

  def submitQuery(request: QueryRequest): Unit = rpcClient
    .FrontendApi
    .submitQuery(request)
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
        .map { x =>
          ConsoleLog.writeError(s"Query failed:")
          errorReports := x.errors
          x.errors
            .foreach { e =>
              ConsoleLog.writeError(e.message)
            }
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
