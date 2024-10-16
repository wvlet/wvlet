package wvlet.lang.ui.editor

import wvlet.airframe.rx.Rx
import wvlet.lang.api.v1.frontend.FrontendApi.{QueryInfoRequest, QueryRequest, QueryResponse}
import wvlet.lang.api.v1.frontend.FrontendRPC.RPCAsyncClient
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.log.LogSupport

class QueryResultReader(rpcClient: RPCAsyncClient) extends LogSupport:

  def submitQuery(query: String): Unit = rpcClient
    .FrontendApi
    .submitQuery(QueryRequest(query = query))
    .map { resp =>
      fetchQueryResult(resp)
    }
    .run()

  def fetchQueryResult(query: QueryResponse): Unit = rpcClient
    .FrontendApi
    .getQueryInfo(QueryInfoRequest(queryId = query.queryId, pageToken = "0"))
    .tap { queryInfo =>
      info(s"Query info: ${queryInfo}")
    }
    .run()
