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

import wvlet.lang.api.v1.flow.FlowApi
import wvlet.lang.api.v1.flow.FlowApi.{
  FlowRunDetail,
  FlowRunListRequest,
  FlowRunRequest,
  FlowRunSummary
}
import wvlet.uni.http.*
import wvlet.uni.http.rpc.RPCClient
import wvlet.uni.rx.Rx
import wvlet.uni.surface.Surface

/**
  * Hand-written RPC client for [[FlowApi]]. Same shape as uni's codegen output — see
  * [[FrontendApiClient]] for rationale.
  */
object FlowApiClient:
  private val rpc = RPCClient.build(Surface.of[FlowApi], Surface.methodsOf[FlowApi])

  class SyncClient(client: HttpSyncClient):
    def listRuns(request: FlowRunListRequest): List[FlowRunSummary] = rpc.callSync(
      client,
      "listRuns",
      Seq(request)
    )

    def getRun(request: FlowRunRequest): FlowRunDetail = rpc.callSync(
      client,
      "getRun",
      Seq(request)
    )

  class AsyncClient(client: HttpAsyncClient):
    def listRuns(request: FlowRunListRequest): Rx[List[FlowRunSummary]] = rpc.callAsync(
      client,
      "listRuns",
      Seq(request)
    )

    def getRun(request: FlowRunRequest): Rx[FlowRunDetail] = rpc.callAsync(
      client,
      "getRun",
      Seq(request)
    )

end FlowApiClient
