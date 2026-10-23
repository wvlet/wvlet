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

import scala.collection.mutable

/**
  * An in-memory item store for testing and lightweight server deployments. All state is lost on
  * restart. Thread-safe: every operation is synchronized, which also makes the conditional updates
  * atomic
  */
class InMemoryItemStore extends ItemStore:

  // Keeps insertion order for listing
  private val items = mutable.LinkedHashMap.empty[String, ItemRecord]

  override def saveItem(record: ItemRecord): Unit = synchronized {
    items.update(record.itemId, record)
  }

  override def getItem(itemId: String): Option[ItemRecord] = synchronized {
    items.get(itemId)
  }

  override def listItems(runId: String, stageName: String): List[ItemRecord] = synchronized {
    items.values.filter(r => r.runId == runId && r.stageName == stageName).toList
  }

  /** Apply `f` to the item when `cond` holds; returns whether it was applied */
  private def updateIf(itemId: String)(cond: ItemRecord => Boolean)(
      f: ItemRecord => ItemRecord
  ): Boolean = synchronized {
    items.get(itemId) match
      case Some(current) if cond(current) =>
        items.update(itemId, f(current))
        true
      case _ =>
        false
  }

  override def reportResult(
      itemId: String,
      attemptId: String,
      state: String,
      output: Option[String],
      error: Option[String],
      finishedAtMillis: Long
  ): Boolean =
    updateIf(itemId)(r => r.attemptId == attemptId && !r.isFinal) {
      _.copy(
        state = state,
        output = output,
        error = error,
        finishedAtMillis = Some(finishedAtMillis),
        leaseExpiresAtMillis = None
      )
    }

  override def refreshItemLease(
      itemId: String,
      attemptId: String,
      leaseExpiresAtMillis: Long
  ): Unit =
    updateIf(itemId)(_.attemptId == attemptId) {
      _.copy(leaseExpiresAtMillis = Some(leaseExpiresAtMillis))
    }

  override def claimItem(
      itemId: String,
      attemptId: String,
      workerId: String,
      startedAtMillis: Long,
      leaseExpiresAtMillis: Long
  ): Boolean =
    updateIf(itemId)(r => r.attemptId == attemptId && r.state == ItemState.Queued.stateName) {
      _.copy(
        state = ItemState.Running.stateName,
        workerId = Some(workerId),
        startedAtMillis = Some(startedAtMillis),
        leaseExpiresAtMillis = Some(leaseExpiresAtMillis)
      )
    }

  override def markCancelling(itemId: String, attemptId: String): Boolean =
    updateIf(itemId)(r =>
      r.attemptId == attemptId &&
        (r.state == ItemState.Queued.stateName || r.state == ItemState.Running.stateName)
    ) {
      _.copy(state = ItemState.Cancelling.stateName)
    }

  override def listActiveItems(): List[ItemRecord] = synchronized {
    items.values.filterNot(_.isTerminal).toList
  }

  override def listItemsPage(
      runId: String,
      stageName: String,
      state: Option[String],
      offset: Int,
      limit: Int
  ): List[ItemRecord] = listItems(runId, stageName)
    .filter(r => state.forall(_ == r.state))
    .slice(offset, offset + limit)

  override def countItemsByState(runId: String, stageName: String): Map[String, Int] =
    listItems(runId, stageName).groupMapReduce(_.state)(_ => 1)(_ + _)

  override def deleteItems(runId: String): Unit = synchronized {
    items.filterInPlace((_, r) => r.runId != runId)
  }

end InMemoryItemStore
