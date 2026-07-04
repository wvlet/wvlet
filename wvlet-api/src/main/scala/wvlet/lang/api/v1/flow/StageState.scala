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

/**
  * The state of a flow stage in the stage execution model.
  *
  * Each stage progresses through a state machine:
  * {{{
  * pending -> running -> success
  * pending -> running -> attempt_failed -> retrying -> running -> ...
  * pending -> running -> attempt_failed -> failed  (max retries exceeded)
  * pending -> skipped   (trigger rule evaluates upstream non-success)
  * pending -> cancelled (user/parent cancellation)
  * }}}
  */
enum StageState(val stateName: String):
  /** Waiting for upstream dependencies */
  case Pending extends StageState("pending")

  /** Currently executing */
  case Running extends StageState("running")

  /** Completed successfully (terminal) */
  case Success extends StageState("success")

  /** Current attempt failed, will retry if attempts remain */
  case AttemptFailed extends StageState("attempt_failed")

  /** Waiting for the retry delay before the next attempt */
  case Retrying extends StageState("retrying")

  /** All retry attempts exhausted (terminal) */
  case Failed extends StageState("failed")

  /** Bypassed due to a trigger rule or upstream non-success (terminal) */
  case Skipped extends StageState("skipped")

  /** Stopped by user or parent flow cancellation (terminal) */
  case Cancelled extends StageState("cancelled")

  /** True if the stage has reached a terminal state */
  def isTerminal: Boolean =
    this match
      case Success | Failed | Skipped | Cancelled =>
        true
      case _ =>
        false

  /**
    * True if this terminal state satisfies an implicit success dependency (from/depends on)
    */
  def isSuccess: Boolean = this == Success

end StageState

object StageState:
  def fromString(name: String): Option[StageState] = StageState.values.find(_.stateName == name)
