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
  * Persistent store for item-level records within fan-out batch stages. Extends the flow run
  * store's run-level persistence with item/attempt granularity.
  *
  * Implementations must be thread-safe: the batch executor reads and writes item records
  * concurrently from worker threads
  */
trait ItemStore extends AutoCloseable:
  /** Persist (create or update) an item record */
  def saveItem(record: ItemRecord): Unit

  /** Read a specific item by its stable id */
  def getItem(itemId: String): Option[ItemRecord]

  /** List all items of a stage within a run, in insertion order */
  def listItems(runId: String, stageName: String): List[ItemRecord]

  /**
    * Atomically report a result for an item, rejecting stale completions. Returns true if the
    * result was accepted (the attemptId matches the current attempt for this item). Returns false
    * if the attemptId is stale (the item was reassigned to a new attempt)
    */
  def reportResult(
      itemId: String,
      attemptId: String,
      state: String,
      output: Option[String],
      error: Option[String],
      finishedAtMillis: Long
  ): Boolean

  /**
    * Refresh the liveness lease of a running item's current attempt
    */
  def refreshItemLease(itemId: String, attemptId: String, leaseExpiresAtMillis: Long): Unit

  /** Delete all item records for a run (cleanup) */
  def deleteItems(runId: String): Unit

  override def close(): Unit = ()

end ItemStore
