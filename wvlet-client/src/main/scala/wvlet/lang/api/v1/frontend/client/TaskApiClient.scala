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

import wvlet.lang.api.v1.task.TaskApi
import wvlet.lang.api.v1.task.TaskApi.*
import wvlet.uni.http.*
import wvlet.uni.http.rpc.RPCClient
import wvlet.uni.rx.Rx
import wvlet.uni.surface.Surface

/**
  * Hand-written RPC client for [[TaskApi]], following the same pattern as [[FlowApiClient]]
  */
object TaskApiClient:
  private val rpc = RPCClient.build(Surface.of[TaskApi], Surface.methodsOf[TaskApi])

  class SyncClient(client: HttpSyncClient):
    def poll(request: PollRequest): PollResponse = rpc.callSync(client, "poll", Seq(request))

    def reportResult(request: TaskResultRequest): TaskResultResponse = rpc.callSync(
      client,
      "reportResult",
      Seq(request)
    )

    def heartbeat(request: HeartbeatRequest): HeartbeatResponse = rpc.callSync(
      client,
      "heartbeat",
      Seq(request)
    )

  class AsyncClient(client: HttpAsyncClient):
    def poll(request: PollRequest): Rx[PollResponse] = rpc.callAsync(client, "poll", Seq(request))

    def reportResult(request: TaskResultRequest): Rx[TaskResultResponse] = rpc.callAsync(
      client,
      "reportResult",
      Seq(request)
    )

    def heartbeat(request: HeartbeatRequest): Rx[HeartbeatResponse] = rpc.callAsync(
      client,
      "heartbeat",
      Seq(request)
    )

end TaskApiClient
