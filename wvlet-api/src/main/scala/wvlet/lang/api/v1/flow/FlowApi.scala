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
package wvlet.lang.api.v1.flow

import wvlet.uni.http.router.RxRouter
import wvlet.uni.http.router.RxRouterProvider

/**
  * Read-only inspection of recorded flow runs for the web UI. Mutations (cancel, resume) stay in
  * the `wvlet flow session` CLI
  */
trait FlowApi:
  import FlowApi.*

  /** List recorded flow runs, most recent first, optionally filtered by flow name */
  def listRuns(request: FlowRunListRequest): List[FlowRunSummary]

  /** The per-stage details of a recorded run */
  def getRun(request: FlowRunRequest): FlowRunDetail

object FlowApi extends RxRouterProvider:
  override def router = RxRouter.of[FlowApi]

  case class FlowRunListRequest(flowName: Option[String] = None, limit: Int = 100)

  case class FlowRunRequest(runId: String)

  /**
    * Run-level summary of a recorded flow run
    *
    * @param runId
    *   Unique run identifier (ULID)
    * @param flowName
    *   Name of the executed flow
    * @param flowCall
    *   The flow call form of the run including its recorded arguments, e.g. `F(segment = 'a')`
    * @param state
    *   The observable run state (running, success, failed, cancelled, or skipped); a running record
    *   whose liveness lease expired is reported as failed
    * @param startedAtMillis
    *   Epoch millis when the run started
    * @param finishedAtMillis
    *   Epoch millis when the run reached a terminal state
    * @param runTimeMillis
    *   The logical run time of the run (schedule fire time for scheduled/backfill runs)
    */
  case class FlowRunSummary(
      runId: String,
      flowName: String,
      flowCall: String,
      state: String,
      startedAtMillis: Long,
      finishedAtMillis: Option[Long] = None,
      runTimeMillis: Option[Long] = None
  )

  /** Per-stage state of a recorded run */
  case class StageRunInfo(name: String, state: String, attempts: Int, error: Option[String] = None)

  /** A recorded run with its per-stage states, attempts, and errors */
  case class FlowRunDetail(run: FlowRunSummary, stages: List[StageRunInfo])

end FlowApi
