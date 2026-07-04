/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.api.v1.frontend.client

import wvlet.lang.api.v1.frontend.FrontendApi
import wvlet.lang.api.v1.frontend.FrontendApi.{QueryInfoRequest, QueryResponse, ServerStatus}
import wvlet.lang.api.v1.query.{QueryInfo, QueryRequest}
import wvlet.uni.http.*
import wvlet.uni.http.rpc.RPCClient
import wvlet.uni.rx.Rx
import wvlet.uni.surface.Surface

/**
  * Hand-written RPC client for [[FrontendApi]]. Mirrors the shape of
  * `wvlet.uni.http.codegen.client.RPCClientGenerator` output so it can be regenerated later by
  * uni's codegen without affecting consumers. Replaces the previous airframe-codegen'd FrontendRPC.
  */
object FrontendApiClient:
  private val rpc = RPCClient.build(Surface.of[FrontendApi], Surface.methodsOf[FrontendApi])

  class SyncClient(client: HttpSyncClient):
    def status: ServerStatus = rpc.callSync(client, "status", Seq.empty)
    def submitQuery(request: QueryRequest): QueryResponse = rpc.callSync(
      client,
      "submitQuery",
      Seq(request)
    )

    def getQueryInfo(request: QueryInfoRequest): QueryInfo = rpc.callSync(
      client,
      "getQueryInfo",
      Seq(request)
    )

  end SyncClient

  class AsyncClient(client: HttpAsyncClient):
    def status: Rx[ServerStatus] = rpc.callAsync(client, "status", Seq.empty)
    def submitQuery(request: QueryRequest): Rx[QueryResponse] = rpc.callAsync(
      client,
      "submitQuery",
      Seq(request)
    )

    def getQueryInfo(request: QueryInfoRequest): Rx[QueryInfo] = rpc.callAsync(
      client,
      "getQueryInfo",
      Seq(request)
    )

  end AsyncClient

end FrontendApiClient
