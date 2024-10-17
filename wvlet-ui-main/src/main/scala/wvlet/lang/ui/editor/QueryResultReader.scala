package wvlet.lang.ui.editor

import wvlet.airframe.rx.{Cancelable, Rx}
import wvlet.lang.api.v1.frontend.FrontendApi.{QueryInfoRequest, QueryRequest, QueryResponse}
import wvlet.lang.api.v1.frontend.FrontendRPC.RPCAsyncClient
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.log.LogSupport

import java.util.concurrent.TimeUnit
import scala.util.{Failure, Success}

class QueryResultReader(rpcClient: RPCAsyncClient) extends LogSupport:

  def submitQuery(query: String): Unit = rpcClient
    .FrontendApi
    .submitQuery(QueryRequest(query = query))
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

    val rx = getQueryInfo(1).transformRx {
      case Success(queryInfo) =>
        if queryInfo.status.isFinished then
          Rx.single(queryInfo)
        else
          Rx.delay(300, TimeUnit.MILLISECONDS)
            .flatMap(_ => getQueryInfo(queryInfo.pageToken.toInt + 1))
      case Failure(e) =>
        Rx.exception(e)
    }
    rx.run { _ =>
      val queryInfo = lst.result()
      info(queryInfo)
      queryInfo
        .lastOption
        .flatMap(_.preview)
        .foreach { preview =>
          info(preview)
          PreviewWindow.previewResult := preview
        }
    }

  end fetchQueryResult

end QueryResultReader
