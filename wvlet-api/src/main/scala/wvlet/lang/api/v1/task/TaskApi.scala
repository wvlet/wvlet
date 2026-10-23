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
package wvlet.lang.api.v1.task

import wvlet.uni.http.router.RxRouter
import wvlet.uni.http.router.RxRouterProvider

/**
  * Task-queue worker protocol for the activity orchestrator. Workers (any language, any platform)
  * poll for tasks, execute them, and report results over this HTTP API. The server owns all
  * scheduling and state; workers are stateless.
  *
  * Endpoints:
  *   - `poll` — long-poll for a task matching the worker's declared activity types
  *   - `reportResult` — report completion, failure, or cancellation of an attempt
  *   - `heartbeat` — renew the liveness lease of a running attempt
  */
trait TaskApi:
  import TaskApi.*

  /**
    * Long-poll for a task matching the worker's declared activity types. Returns immediately with
    * `hasTask = false` if no task is available within the poll timeout
    */
  def poll(request: PollRequest): PollResponse

  /**
    * Report the outcome of a task attempt. The server validates the attemptId against the current
    * attempt for fencing — a stale completion from a dead worker is rejected
    */
  def reportResult(request: TaskResultRequest): TaskResultResponse

  /**
    * Renew the liveness lease of a running attempt. The response signals whether the worker should
    * continue or cancel (the orchestrator requested cancellation)
    */
  def heartbeat(request: HeartbeatRequest): HeartbeatResponse

object TaskApi extends RxRouterProvider:
  override def router: RxRouter = RxRouter.of[TaskApi]

  /**
    * @param activityTypes
    *   The activity types this worker can handle (e.g. ["agent.investigate", "agent.summarize"])
    * @param workerId
    *   A self-assigned worker identity for observability; the server does not require uniqueness
    * @param timeoutMillis
    *   How long to wait for a task before returning empty. None or 0 for immediate return
    */
  case class PollRequest(
      activityTypes: List[String],
      workerId: String,
      timeoutMillis: Option[Long] = None
  )

  /**
    * The response to a poll request. When `hasTask` is true, the remaining fields describe the
    * assigned task. When false, the worker should wait `retryAfterMillis` before polling again
    *
    * @param hasTask
    *   Whether a task was assigned
    * @param activityType
    *   The type of the assigned activity
    * @param invocationId
    *   The stable invocation identity (same across retries of the same item)
    * @param attemptId
    *   The unique attempt identity (use in reportResult and heartbeat)
    * @param input
    *   The activity's input parameters as string key-value pairs
    * @param timeoutMillis
    *   Per-attempt timeout; the worker should complete or report failure within this window
    * @param heartbeatMillis
    *   How often the worker must send heartbeats to maintain its lease
    * @param retryAfterMillis
    *   Suggested wait before the next poll (when hasTask is false)
    */
  case class PollResponse(
      hasTask: Boolean,
      activityType: String = "",
      invocationId: String = "",
      attemptId: String = "",
      input: Map[String, String] = Map.empty,
      timeoutMillis: Option[Long] = None,
      heartbeatMillis: Option[Long] = None,
      retryAfterMillis: Option[Long] = None
  )

  object PollResponse:
    def empty(retryAfterMillis: Long = 1000L): PollResponse = PollResponse(
      hasTask = false,
      retryAfterMillis = Some(retryAfterMillis)
    )

  /**
    * @param attemptId
    *   The attempt being reported (from PollResponse.attemptId)
    * @param status
    *   "completed", "failed", or "cancelled"
    * @param output
    *   Structured output (for completed status)
    * @param error
    *   Error description (for failed status)
    * @param retryable
    *   Whether the failure is transient (for failed status)
    */
  case class TaskResultRequest(
      attemptId: String,
      status: String,
      output: Map[String, String] = Map.empty,
      error: Option[String] = None,
      retryable: Boolean = false
  )

  /**
    * @param accepted
    *   True if the result was recorded. False if the attemptId was stale (fenced)
    * @param message
    *   Human-readable explanation
    */
  case class TaskResultResponse(accepted: Boolean, message: String)

  /**
    * @param attemptId
    *   The attempt renewing its lease
    */
  case class HeartbeatRequest(attemptId: String)

  /**
    * @param action
    *   "continue" — keep working; "cancel" — the orchestrator wants this attempt to stop
    */
  case class HeartbeatResponse(action: String)

  object HeartbeatResponse:
    val Continue: HeartbeatResponse = HeartbeatResponse("continue")
    val Cancel: HeartbeatResponse   = HeartbeatResponse("cancel")

end TaskApi
