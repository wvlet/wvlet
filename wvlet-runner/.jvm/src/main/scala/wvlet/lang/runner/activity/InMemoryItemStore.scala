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

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

/**
  * An in-memory item store for testing and lightweight server deployments. All state is lost on
  * restart. Thread-safe via ConcurrentHashMap.
  */
class InMemoryItemStore extends ItemStore:

  private val items = ConcurrentHashMap[String, ItemRecord]()

  override def saveItem(record: ItemRecord): Unit = items.put(record.itemId, record)

  override def getItem(itemId: String): Option[ItemRecord] = Option(items.get(itemId))

  override def listItems(runId: String, stageName: String): List[ItemRecord] = items
    .values()
    .asScala
    .filter(r => r.runId == runId && r.stageName == stageName)
    .toList
    .sortBy(_.itemKey)

  override def reportResult(
      itemId: String,
      attemptId: String,
      state: String,
      output: Option[String],
      error: Option[String],
      finishedAtMillis: Long
  ): Boolean =
    items.computeIfPresent(
      itemId,
      (_, current) =>
        if current.attemptId == attemptId then
          current.copy(
            state = state,
            output = output,
            error = error,
            finishedAtMillis = Some(finishedAtMillis),
            leaseExpiresAtMillis = None
          )
        else
          current
    ) match
      case null =>
        false
      case updated =>
        updated.attemptId == attemptId && updated.state == state

  override def refreshItemLease(
      itemId: String,
      attemptId: String,
      leaseExpiresAtMillis: Long
  ): Unit = items.computeIfPresent(
    itemId,
    (_, current) =>
      if current.attemptId == attemptId then
        current.copy(leaseExpiresAtMillis = Some(leaseExpiresAtMillis))
      else
        current
  )

  override def deleteItems(runId: String): Unit = items
    .entrySet()
    .removeIf(e => e.getValue.runId == runId)

end InMemoryItemStore
