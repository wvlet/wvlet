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
package wvlet.lang.api.v1.frontend

import wvlet.lang.api.v1.frontend.client.{FileApiClient, FlowApiClient, FrontendApiClient}
import wvlet.uni.http.{HttpAsyncClient, HttpSyncClient}

/**
  * Aggregated RPC entry point that wraps the per-service uni-RPC clients into a single client
  * object with a stable consumer surface (`FrontendRPC.RPCSyncClient`, `.RPCAsyncClient`,
  * `.FrontendApi`, `.FileApi`). Replaces the airframe-codegen'd FrontendRPC dropped in #1662 phase 2.
  *
  * The shape preserves the previous `client.FrontendApi.method(...)` and
  * `client.FileApi.method(...)` access pattern so consumers don't need to learn two clients.
  */
object FrontendRPC:

  def newRPCSyncClient(http: HttpSyncClient): RPCSyncClient    = new RPCSyncClient(http)
  def newRPCAsyncClient(http: HttpAsyncClient): RPCAsyncClient = new RPCAsyncClient(http)

  class RPCSyncClient(val http: HttpSyncClient) extends AutoCloseable:
    val FrontendApi: FrontendApiClient.SyncClient = new FrontendApiClient.SyncClient(http)
    val FileApi: FileApiClient.SyncClient         = new FileApiClient.SyncClient(http)
    val FlowApi: FlowApiClient.SyncClient         = new FlowApiClient.SyncClient(http)
    override def close(): Unit                    = http.close()
  end RPCSyncClient

  class RPCAsyncClient(val http: HttpAsyncClient) extends AutoCloseable:
    val FrontendApi: FrontendApiClient.AsyncClient = new FrontendApiClient.AsyncClient(http)
    val FileApi: FileApiClient.AsyncClient         = new FileApiClient.AsyncClient(http)
    val FlowApi: FlowApiClient.AsyncClient         = new FlowApiClient.AsyncClient(http)
    override def close(): Unit                     = http.close()
  end RPCAsyncClient

end FrontendRPC
