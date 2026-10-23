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
  * The state of a single item within a fan-out batch. Items progress through:
  * {{{
  * queued -> running -> succeeded
  * queued -> running -> failed
  * queued -> running -> cancelled
  * queued -> running -> uncertain   (worker died, cancellation unconfirmed)
  * queued -> cancelled              (batch cancelled before admission)
  * running -> cancelling -> cancelled
  * running -> cancelling -> uncertain  (cancellation not confirmed within timeout)
  * }}}
  */
enum ItemState(val stateName: String):
  case Queued    extends ItemState("queued")
  case Running   extends ItemState("running")
  case Succeeded extends ItemState("succeeded")
  case Failed    extends ItemState("failed")
  case Cancelled extends ItemState("cancelled")

  /** Cancellation requested, awaiting executor acknowledgement */
  case Cancelling extends ItemState("cancelling")

  /**
    * The item's outcome is unknown: its worker died or became unreachable, and the external action
    * may or may not have completed. Automatic retry requires explicit policy
    */
  case Uncertain extends ItemState("uncertain")

  def isTerminal: Boolean =
    this match
      case Succeeded | Failed | Cancelled | Uncertain =>
        true
      case _ =>
        false

  /**
    * A final state is never overwritten by a result report. An uncertain item is terminal for
    * scheduling but not final: a late result of the same attempt may still resolve it
    */
  def isFinal: Boolean =
    this match
      case Succeeded | Failed | Cancelled =>
        true
      case _ =>
        false

end ItemState

object ItemState:
  def fromString(name: String): Option[ItemState] = ItemState.values.find(_.stateName == name)
