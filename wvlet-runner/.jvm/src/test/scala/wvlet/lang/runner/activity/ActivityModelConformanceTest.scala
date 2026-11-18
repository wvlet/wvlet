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

import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONArray
import wvlet.uni.json.JSON.JSONBoolean
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.json.JSON.JSONValue
import wvlet.uni.test.UniTest

import java.nio.file.Files
import scala.io.Source
import scala.util.Using

/**
  * Replays the traces of the Lean model of the activity state machine (formal/, exported by `lake
  * exe activity-traces`) against the shipped item stores, task queue, and batch executor. The model
  * proves the fencing and restart properties; this test checks that the Scala code behaves as the
  * model does.
  */
class ActivityModelConformanceTest extends UniTest:

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/runner/activity/activity-traces.json")
        )
      )(_.mkString)
    obj(JSON.parse(text))

  private def obj(v: JSONValue): Map[String, JSONValue] =
    v match
      case JSONObject(entries) =>
        entries.toMap
      case other =>
        fail(s"Expected a JSON object: ${other.toJSON}")

  private def cases(name: String): List[Map[String, JSONValue]] =
    fixture(name) match
      case JSONArray(elems) =>
        elems.map(obj).toList
      case other =>
        fail(s"Expected a JSON array: ${other.toJSON}")

  extension (m: Map[String, JSONValue])
    private def str(key: String): String =
      m(key) match
        case JSONString(s) =>
          s
        case other =>
          fail(s"Expected a string at ${key}: ${other.toJSON}")

    private def bool(key: String): Boolean =
      m(key) match
        case JSONBoolean(b) =>
          b
        case other =>
          fail(s"Expected a boolean at ${key}: ${other.toJSON}")

  private def newSQLiteStore(): SQLiteItemStore = SQLiteItemStore(
    Files.createTempDirectory("wv-activity-model").resolve("items.db")
  )

  private def withStores(body: (String, ItemStore) => Unit): Unit =
    body("in-memory", InMemoryItemStore())
    Using.resource(newSQLiteStore())(store => body("sqlite", store))

  private def item(itemId: String, state: String): ItemRecord = ItemRecord(
    itemId = itemId,
    runId = "run",
    stageName = "stage",
    itemKey = itemId,
    state = state,
    attemptId = "a0",
    attempts = 1
  )

  test("fenced store operations follow the model") {
    val traces = cases("store")
    traces.size shouldBe 1092
    withStores { (storeName, store) =>
      traces
        .zipWithIndex
        .foreach { (trace, idx) =>
          val itemId = s"item-${idx}"
          store.saveItem(item(itemId, trace.str("init")))
          val ops =
            trace("ops") match
              case JSONArray(elems) =>
                elems.map(obj)
              case other =>
                fail(s"Expected ops: ${other.toJSON}")
          ops.foreach { op =>
            val attemptId = op.str("attempt")
            val accepted  =
              op.str("op") match
                case "claim" =>
                  store.claimItem(itemId, attemptId, "worker", 0L, 1000L)
                case "markCancelling" =>
                  store.markCancelling(itemId, attemptId)
                case "reportResult" =>
                  store.reportResult(itemId, attemptId, op.str("outcome"), None, None, 0L)
                case other =>
                  fail(s"Unknown op ${other}")
            val clue =
              s"${storeName} trace #${idx}: ${trace.str("init")} then ${ops.map(_.str("op"))}"
            (clue, accepted) shouldBe (clue, op.bool("accepted"))
            (clue, store.getItem(itemId).map(_.state)) shouldBe (clue, Some(op.str("state")))
            (clue, store.getItem(itemId).map(_.attemptId)) shouldBe (clue, Some("a0"))
          }
        }
    }
  }

  test("batch restart reconciliation follows the model") {
    val all = cases("batchRestart")
    all.size shouldBe 112
    withStores { (storeName, store) =>
      all
        .zipWithIndex
        .foreach { (c, idx) =>
          val runId  = s"run-${idx}"
          val itemId = ItemRecord.itemId(runId, "stage", "k")
          store.saveItem(item(itemId, c.str("init")).copy(runId = runId, itemKey = "k"))
          val resumable = c.bool("canResume")
          val executor  =
            new ActivityExecutor:
              override def name: String = "test"
              override def execute(
                  request: ActivityRequest,
                  heartbeat: () => Unit
              ): ActivityResult = fail("recoverItems must not execute activities")
              override def cancel(attemptId: String): CancelAck  = CancelAck.Unknown
              override def canResume(attemptId: String): Boolean = resumable
          val config = BatchConfig()
            .withRetryUncertain(c.bool("retryUncertain"))
            .withRetryFailed(c.bool("retryFailed"))
            .withRerunCancelled(c.bool("rerunCancelled"))
          val (items, resumed) = BatchExecutor(executor, store, config).recoverItems(
            runId,
            "stage",
            List("k"),
            System.currentTimeMillis()
          )
          val clue   = s"${storeName} case ${c}"
          val stored = store.getItem(itemId).get
          (clue, items.map(_.state)) shouldBe (clue, List(c.str("state")))
          (clue, stored.state) shouldBe (clue, c.str("state"))
          (clue, stored.attemptId != "a0") shouldBe (clue, c.bool("newAttempt"))
          (clue, resumed.contains(itemId)) shouldBe (clue, c.bool("resumed"))
        }
    }
  }

  test("task queue restart and lease expiry follow the model") {
    val all = cases("queueRestart")
    all.size shouldBe 28
    all.foreach { c =>
      // Each case restarts a queue on its own store: recovery processes every active item
      withStores { (storeName, store) =>
        val itemId = "item"
        store.saveItem(
          item(itemId, c.str("init")).copy(
            activityType = Some("test"),
            workerId = Option.when(c.bool("hasWorker"))("worker"),
            leaseExpiresAtMillis = Some(1000L)
          )
        )
        val now =
          if c.bool("leaseExpired") then
            2000L
          else
            500L
        val queue = TaskQueue(store, clock = () => now)
        queue.expireLeases()
        val clue = s"${storeName} case ${c}"
        (clue, store.getItem(itemId).map(_.state)) shouldBe (clue, Some(c.str("state")))
      }
    }
  }

end ActivityModelConformanceTest
