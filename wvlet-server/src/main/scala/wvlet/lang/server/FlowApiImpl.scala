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

import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.flow.FlowApi
import wvlet.lang.api.v1.flow.FlowApi.*
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.FlowRunLauncher
import wvlet.lang.runner.FlowRunRecord
import wvlet.lang.runner.FlowRunStore
import wvlet.lang.runner.ThreadManager
import wvlet.uni.control.Control
import wvlet.uni.http.rpc.RPCStatus
import wvlet.uni.log.LogSupport

/**
  * [[FlowApi]] over the local flow run store of the server's working folder. The store is opened
  * once per server session (both store backends read fresh state on every list/get, so runs written
  * by other processes — CLI runs, scheduler daemons — stay visible) and closed with the DI session.
  * Cancel and resume mutations follow the same store/launcher paths as `wvlet flow session`, so the
  * web UI and the CLI observe identical semantics
  */
class FlowApiImpl(workEnv: WorkEnv, profile: Profile, threadManager: ThreadManager)
    extends FlowApi
    with AutoCloseable
    with LogSupport:

  private lazy val store: FlowRunStore = FlowRunStore.forWorkEnv(workEnv)

  override def close(): Unit = store.close()

  override def listRuns(request: FlowRunListRequest): List[FlowRunSummary] =
    val now = System.currentTimeMillis()
    store
      .list()
      .iterator
      .filter(r => request.flowName.forall(_ == r.flowName))
      .take(request.limit.max(0))
      .map(toSummary(_, now))
      .toList

  override def getRun(request: FlowRunRequest): FlowRunDetail =
    store.get(request.runId) match
      case Some(r) =>
        FlowRunDetail(
          run = toSummary(r, System.currentTimeMillis()),
          stages = r
            .stages
            .map(s =>
              StageRunInfo(
                s.name,
                s.state,
                s.attempts,
                s.error,
                waitingSinceMillis = s.waitingSinceMillis,
                lastPollAtMillis = s.lastPollAtMillis
              )
            )
        )
      case None =>
        throw RPCStatus.NOT_FOUND_U5.newException(s"Flow run '${request.runId}' is not found")

  override def cancelRun(request: FlowRunRequest): FlowRunActionResult =
    val r = recordOf(request.runId)
    if r.isTerminal then
      FlowRunActionResult(
        runId = r.runId,
        accepted = false,
        message = s"Run ${r.runId} is already ${r.state}"
      )
    else
      store.requestCancel(r.runId)
      FlowRunActionResult(
        runId = r.runId,
        accepted = true,
        message = s"Requested cancellation of run ${r.runId}"
      )

  override def resumeRun(request: FlowRunRequest): FlowRunActionResult =
    val r   = recordOf(request.runId)
    val now = System.currentTimeMillis()
    r.state match
      // A stale running record belongs to a crashed process and can be resumed
      case FlowRunRecord.STATE_RUNNING if !r.isStaleAt(now) =>
        throw RPCStatus
          .INVALID_REQUEST_U1
          .newException(s"Run ${r.runId} is still running and cannot be resumed")
      case FlowRunRecord.STATE_SUCCESS =>
        FlowRunActionResult(
          runId = r.runId,
          accepted = false,
          message = s"Run ${r.runId} already succeeded; nothing to resume"
        )
      case FlowRunRecord.STATE_SKIPPED =>
        throw RPCStatus
          .INVALID_REQUEST_U1
          .newException(
            s"Run ${r
                .runId} was skipped (its dependency was not satisfied); start a new run instead"
          )
      case _ =>
        // Compile synchronously so unknown flows and compile errors surface to the caller;
        // the run itself executes in the background (an RPC must not block for a whole flow)
        val loaded =
          try
            val l = FlowRunLauncher.loadFlows(workEnv.path, workEnv)
            l.find(r.flowName)
            l
          catch
            case e: WvletLangException =>
              throw RPCStatus.INVALID_REQUEST_U1.newException(e.getMessage, e)
        threadManager.runBackgroundTask { () =>
          try
            // A dedicated store instance: the background run outlives this RPC call and must
            // not race the session-scoped store's lifecycle
            Control.withResource(FlowRunStore.forWorkEnv(workEnv)) { runStore =>
              FlowRunLauncher.execute(
                loaded,
                r.flowName,
                profile,
                workEnv,
                runStore,
                resumeFrom = Some(r)
              )
            }
          catch
            case e: Throwable =>
              warn(s"Resumed run ${r.runId} of flow '${r.flowName}' failed: ${e.getMessage}")
        }
        FlowRunActionResult(
          runId = r.runId,
          accepted = true,
          message = s"Resuming run ${r.runId} of flow '${r.flowName}' in the background"
        )
    end match

  end resumeRun

  private def recordOf(runId: String): FlowRunRecord = store
    .get(runId)
    .getOrElse(throw RPCStatus.NOT_FOUND_U5.newException(s"Flow run '${runId}' is not found"))

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
