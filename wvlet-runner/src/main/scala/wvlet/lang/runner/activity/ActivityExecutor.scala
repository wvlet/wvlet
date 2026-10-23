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
package wvlet.lang.runner.activity

/**
  * A request to execute a single activity invocation. The executor receives this and produces an
  * [[ActivityResult]].
  *
  * @param activityType
  *   The registered activity type name (e.g. "agent.investigate", "sql.materialize")
  * @param invocationId
  *   Stable identity of this invocation across attempts: hash of (runId, stageName, itemKey). Two
  *   attempts of the same invocation share this id
  * @param attemptId
  *   Unique identity of this specific attempt (ULID). The store rejects results whose attemptId
  *   does not match the current attempt (stale-completion fencing)
  * @param input
  *   Typed input for the activity, keyed by parameter name
  * @param config
  *   Execution configuration (timeout, heartbeat interval)
  */
case class ActivityRequest(
    activityType: String,
    invocationId: String,
    attemptId: String,
    input: Map[String, Any],
    config: ActivityConfig
):
  def withInput(key: String, value: Any): ActivityRequest = copy(input = input + (key -> value))

/**
  * Configuration for a single activity execution, extracted from the stage's `with { ... }` block
  */
case class ActivityConfig(timeoutMillis: Option[Long] = None, heartbeatMillis: Option[Long] = None):
  def withTimeoutMillis(millis: Long): ActivityConfig   = copy(timeoutMillis = Some(millis))
  def withHeartbeatMillis(millis: Long): ActivityConfig = copy(heartbeatMillis = Some(millis))
  def noTimeout(): ActivityConfig                       = copy(timeoutMillis = None)
  def noHeartbeat(): ActivityConfig                     = copy(heartbeatMillis = None)

/**
  * The result of a single activity execution attempt
  */
sealed trait ActivityResult

object ActivityResult:
  /**
    * The activity completed successfully
    *
    * @param output
    *   Structured output of the activity. Agent results may be references (session id, result URL)
    *   rather than materialized data
    */
  case class Completed(output: Map[String, Any] = Map.empty) extends ActivityResult

  /**
    * The activity failed
    *
    * @param error
    *   Human-readable error description
    * @param retryable
    *   Whether the failure is transient and the attempt can be retried. Non-idempotent activities
    *   should return false unless they can guarantee safe replay
    */
  case class Failed(error: String, retryable: Boolean = false) extends ActivityResult

  /** The activity acknowledged a cancellation request and stopped */
  case object Cancelled extends ActivityResult

  /**
    * The outcome is unknown: the executor lost contact with the work (e.g. a remote worker stopped
    * heartbeating). The external action may or may not have happened, so the orchestrator must not
    * retry it automatically
    *
    * @param reason
    *   Why the outcome is unknown
    */
  case class Uncertain(reason: String) extends ActivityResult

end ActivityResult

/**
  * Acknowledgement of a cancellation request sent to a running activity
  */
sealed trait CancelAck

object CancelAck:
  /** The executor acknowledged the cancellation and will stop the activity */
  case object Acknowledged extends CancelAck

  /** The executor could not confirm whether the activity will stop (e.g. unreachable) */
  case object Unknown extends CancelAck

/**
  * The production contract for executing non-SQL work within a flow stage. Implementations own the
  * execution of a single activity type (agent sessions, external service calls, etc.) while the
  * flow executor owns scheduling, admission, and lifecycle.
  *
  * The contract is intentionally narrow: an executor starts work, reports progress, returns a
  * result, and acknowledges cancellation. It does not manage retries, admission queues, or
  * persistence — those belong to the orchestrator.
  *
  * Implementations must be thread-safe: the batch executor may call [[execute]] concurrently for
  * different items within a fan-out stage.
  */
trait ActivityExecutor:
  /** The activity type name this executor handles */
  def name: String

  /**
    * Execute a single activity invocation. The implementation should call `heartbeat()`
    * periodically when the activity has a heartbeat configuration; the orchestrator treats a missed
    * heartbeat as a liveness failure.
    *
    * @param request
    *   The activity request with input, identity, and configuration
    * @param heartbeat
    *   Callback to report liveness. Call periodically during long-running work
    * @return
    *   The activity result (completed, failed, or cancelled)
    */
  def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult

  /**
    * Request cancellation of a running activity attempt. The executor should make a best-effort
    * attempt to stop the work and return an acknowledgement.
    *
    * @param attemptId
    *   The attempt to cancel (matches [[ActivityRequest.attemptId]])
    * @return
    *   Whether the executor acknowledged the cancellation
    */
  def cancel(attemptId: String): CancelAck

  /**
    * Whether an attempt started before an orchestrator restart is still tracked by this executor,
    * so that calling [[execute]] with the same attemptId awaits it instead of starting new work.
    * Executors whose work does not outlive the orchestrator process return false (the default); the
    * orchestrator then marks such an in-flight attempt uncertain rather than re-executing it
    */
  def canResume(attemptId: String): Boolean = false

end ActivityExecutor
