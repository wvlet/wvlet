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

import scala.util.hashing.MurmurHash3

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
  * @param activityType
  *   The activity type of a task dispatched through the [[TaskQueue]]. Set when the item is
  *   enqueued, so that a restarted queue can dispatch it again
  * @param input
  *   JSON object of the task input (string values), for re-dispatch after a restart
  * @param timeoutMillis
  *   Per-attempt timeout of a dispatched task
  * @param heartbeatMillis
  *   Heartbeat interval of a dispatched task
  * @param workerId
  *   The worker that claimed the current attempt
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
    leaseExpiresAtMillis: Option[Long] = None,
    activityType: Option[String] = None,
    input: Option[String] = None,
    timeoutMillis: Option[Long] = None,
    heartbeatMillis: Option[Long] = None,
    workerId: Option[String] = None
):
  def isTerminal: Boolean = ItemState.fromString(state).exists(_.isTerminal)

  /**
    * Whether the item reached an outcome that a result report must not overwrite. Unlike
    * [[isTerminal]], `uncertain` is not final: a late result of the same attempt resolves it
    */
  def isFinal: Boolean = ItemState.fromString(state).exists(_.isFinal)

  def isStaleAt(nowMillis: Long): Boolean =
    state == ItemState.Running.stateName && leaseExpiresAtMillis.exists(_ < nowMillis)

end ItemRecord

object ItemRecord:

  /**
    * Compute the stable item id from its coordinates. The id is deterministic so that a restarted
    * flow finds the same item records. Uses MurmurHash3 for cross-platform compatibility (JVM,
    * Scala.js, Scala Native)
    */
  def itemId(runId: String, stageName: String, itemKey: String): String =
    val input = s"${runId}:${stageName}:${itemKey}"
    val h1    = MurmurHash3.stringHash(input, 0x12345678)
    val h2    = MurmurHash3.stringHash(input, 0x9abcdef0.toInt)
    val h3    = MurmurHash3.stringHash(input, h1)
    val h4    = MurmurHash3.stringHash(input, h2)
    f"${h1}%08x${h2}%08x${h3}%08x${h4}%08x"
