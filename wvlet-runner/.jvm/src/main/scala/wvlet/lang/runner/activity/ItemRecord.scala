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

end ItemState

object ItemState:
  def fromString(name: String): Option[ItemState] = ItemState.values.find(_.stateName == name)

/**
  * The persisted record of a single item within a fan-out batch stage
  *
  * @param itemId
  *   Stable identity of this item: deterministic hash of (runId, stageName, itemKey). Survives
  *   restarts and attempt reassignment
  * @param runId
  *   The flow run this item belongs to
  * @param stageName
  *   The stage this item belongs to
  * @param itemKey
  *   The fan-out value (the `for each` expression result for this item)
  * @param state
  *   Current item state
  * @param attemptId
  *   The ULID of the current (or most recent) attempt. Changes on each retry. The store rejects
  *   results whose attemptId does not match (stale-completion fencing)
  * @param attempts
  *   Total number of attempts made for this item
  * @param output
  *   JSON-encoded activity result (for succeeded items)
  * @param error
  *   Error message (for failed/uncertain items)
  * @param startedAtMillis
  *   When the current attempt started
  * @param finishedAtMillis
  *   When the item reached a terminal state
  * @param leaseExpiresAtMillis
  *   Liveness lease of the current attempt. A running item whose lease expired transitions to
  *   uncertain on recovery
  */
case class ItemRecord(
    itemId: String,
    runId: String,
    stageName: String,
    itemKey: String,
    state: String,
    attemptId: String,
    attempts: Int,
    output: Option[String] = None,
    error: Option[String] = None,
    startedAtMillis: Option[Long] = None,
    finishedAtMillis: Option[Long] = None,
    leaseExpiresAtMillis: Option[Long] = None
):
  def isTerminal: Boolean = ItemState.fromString(state).exists(_.isTerminal)

  def isStaleAt(nowMillis: Long): Boolean =
    state == ItemState.Running.stateName && leaseExpiresAtMillis.exists(_ < nowMillis)

end ItemRecord

object ItemRecord:
  import java.security.MessageDigest
  import java.nio.charset.StandardCharsets

  /**
    * Compute the stable item id from its coordinates. The id is deterministic so that a restarted
    * flow finds the same item records
    */
  def itemId(runId: String, stageName: String, itemKey: String): String =
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(s"${runId}:${stageName}:${itemKey}".getBytes(StandardCharsets.UTF_8))
    digest.digest().take(16).map("%02x".format(_)).mkString
