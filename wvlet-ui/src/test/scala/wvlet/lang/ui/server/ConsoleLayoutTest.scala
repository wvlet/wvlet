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
package wvlet.lang.ui.server

import wvlet.lang.api.v1.query.QueryMemory
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.lang.api.v1.query.QuerySummary
import wvlet.uni.test.UniTest

import java.time.Instant

class ConsoleLayoutTest extends UniTest:
  private val t0 = Instant.ofEpochMilli(1_000_000L)

  private def q(
      id: String,
      createdAfterMillis: Long,
      queued: Long,
      run: Option[Long],
      status: QueryStatus = QueryStatus.FINISHED,
      session: Option[String] = None,
      memory: QueryMemory = QueryMemory()
  ): QuerySummary =
    val created = t0.plusMillis(createdAfterMillis)
    QuerySummary(
      queryId = id,
      status = status,
      sessionId = session,
      createdAt = created,
      startedAt = run.map(_ => created.plusMillis(queued)),
      completedAt =
        if status.isFinished then
          Some(created.plusMillis(queued + run.getOrElse(0L)))
        else
          None
      ,
      queuedMillis = queued,
      runMillis = run,
      memory = memory
    )

  test("should scale time bars to the slowest query in view") {
    val bars = ConsoleLayout.timeBars(Seq(q("a", 0, 100, Some(300)), q("b", 0, 0, Some(200))))
    bars("a") shouldBe TimeBar(25.0, 75.0)
    bars("b") shouldBe TimeBar(0.0, 50.0)
  }

  test("should give no memory bar to queries without memory accounting") {
    val bars = ConsoleLayout.memoryBars(
      Seq(
        q("a", 0, 0, Some(1), memory = QueryMemory(tracked = true, peakBytes = 400)),
        q("b", 0, 0, Some(1), memory = QueryMemory(tracked = true, peakBytes = 100)),
        q("c", 0, 0, Some(1))
      )
    )
    bars shouldBe Map("a" -> 100.0, "b" -> 25.0)
  }

  test("should use reserved bytes of a running query when above its peak so far") {
    val running = q(
      "a",
      0,
      0,
      Some(1),
      status = QueryStatus.RUNNING,
      memory = QueryMemory(tracked = true, reservedBytes = 50, peakBytes = 10)
    )
    ConsoleLayout.peakOrReserved(running) shouldBe 50L
  }

  test("should lay out one timeline lane per session in order of first query") {
    val qs = Seq(
      q("c", 600, 0, Some(400), session = Some("s1")),
      q("b", 200, 100, Some(100)),
      q("a", 0, 0, Some(200), session = Some("s1"))
    )
    val layout = ConsoleLayout.timeline(qs, t0.plusMillis(5000))
    // Every query finished: the window ends at the last completion
    layout.start shouldBe t0
    layout.spanMillis shouldBe 1000L
    layout.lanes.map(_.session) shouldBe Seq("s1", ConsoleLayout.DefaultSession)
    layout.lanes.head.bars.map(_.query.queryId) shouldBe Seq("a", "c")
    val b = layout.lanes(1).bars.head
    b shouldBe TimelineBar(b.query, 20.0, 10.0, 10.0)
  }

  test("should put queries overlapping in time on separate rows of their lane") {
    val qs = Seq(
      q("a", 0, 0, Some(500), session = Some("s")),
      q("b", 100, 400, Some(100), session = Some("s")),
      q("c", 600, 0, Some(100), session = Some("s"))
    )
    val lane = ConsoleLayout.timeline(qs, t0.plusMillis(1000)).lanes.head
    lane.bars.map(b => b.query.queryId -> b.row) shouldBe Seq("a" -> 0, "b" -> 1, "c" -> 0)
    lane.rows shouldBe 2
  }

  test("should keep the timeline to the last few minutes") {
    val w  = ConsoleLayout.TimelineWindowMillis
    val qs = Seq(
      q("old", 0, 0, Some(100)),
      q("cut", w, 0, Some(w)),
      q("new", 2 * w, 0, Some(100), status = QueryStatus.RUNNING)
    )
    val layout = ConsoleLayout.timeline(qs, t0.plusMillis(2 * w + 100))
    layout.hidden shouldBe 1
    layout.spanMillis shouldBe w
    val bars = layout.lanes.flatMap(_.bars).map(b => b.query.queryId -> b).toMap
    bars.keySet shouldBe Set("cut", "new")
    // "cut" began before the window: it starts at the left edge with the rest of its run
    bars("cut").startPct shouldBe 0.0
    bars("cut").runPct shouldBe ((w - 100) * 100.0 / w)
  }

  test("should end the timeline at now while a query runs") {
    val qs     = Seq(q("a", 0, 0, Some(100), status = QueryStatus.RUNNING))
    val layout = ConsoleLayout.timeline(qs, t0.plusMillis(400))
    layout.spanMillis shouldBe 400L
  }

  test("should parse console and flow hashes") {
    Page.parse("#/") shouldBe Page.Console(None)
    Page.parse("") shouldBe Page.Console(None)
    Page.parse("#/q/20261127_1") shouldBe Page.Console(Some("20261127_1"))
    // Links from the first UI
    Page.parse("#/queries/20261127_1") shouldBe Page.Console(Some("20261127_1"))
    Page.parse("#/flows") shouldBe Page.Flows
    Page.parse("#/unknown") shouldBe Page.Console(None)
    Page.Console(Some("a b")).hash shouldBe "#/q/a%20b"
  }

end ConsoleLayoutTest
