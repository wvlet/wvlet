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
package wvlet.lang.server

import wvlet.lang.api.v1.flow.FlowApi
import wvlet.lang.api.v1.flow.FlowApi.*
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.FlowRunRecord
import wvlet.lang.runner.FlowRunStore
import wvlet.uni.control.Control
import wvlet.uni.http.rpc.RPCStatus
import wvlet.uni.log.LogSupport

/**
  * Read-only [[FlowApi]] over the local flow run store of the server's working folder. The store is
  * opened per request so the server always observes runs written by other processes (CLI runs,
  * scheduler daemons) without holding a connection
  */
class FlowApiImpl(workEnv: WorkEnv) extends FlowApi with LogSupport:

  private def withStore[A](body: FlowRunStore => A): A =
    Control.withResource(FlowRunStore.forWorkEnv(workEnv))(body)

  override def listRuns(request: FlowRunListRequest): List[FlowRunSummary] = withStore { store =>
    val now = System.currentTimeMillis()
    store
      .list()
      .iterator
      .filter(r => request.flowName.forall(_ == r.flowName))
      .take(request.limit.max(0))
      .map(toSummary(_, now))
      .toList
  }

  override def getRun(request: FlowRunRequest): FlowRunDetail = withStore { store =>
    store.get(request.runId) match
      case Some(r) =>
        FlowRunDetail(
          run = toSummary(r, System.currentTimeMillis()),
          stages = r.stages.map(s => StageRunInfo(s.name, s.state, s.attempts, s.error))
        )
      case None =>
        throw RPCStatus.NOT_FOUND_U5.newException(s"Flow run '${request.runId}' is not found")
  }

  private def toSummary(r: FlowRunRecord, nowMillis: Long): FlowRunSummary = FlowRunSummary(
    runId = r.runId,
    flowName = r.flowName,
    flowCall = r.flowCallForm,
    state = r.effectiveStateAt(nowMillis),
    startedAtMillis = r.startedAtMillis,
    finishedAtMillis = r.finishedAtMillis,
    runTimeMillis = r.runTimeMillis
  )

end FlowApiImpl
