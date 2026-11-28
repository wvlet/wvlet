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
package wvlet.lang.engine.exec

import wvlet.uni.test.UniTest

class FairTaskQueueTest extends UniTest:
  private val second = 1000000000L

  private def group(runNanos: Long = 0L): TaskGroup =
    val g = TaskGroup.newGroup()
    g.addRunNanos(runNanos)
    g

  // Take n tasks, accounting `nanos` of run time to each, and return their groups
  private def drain(q: FairTaskQueue[Int], n: Int, nanos: Long): Seq[TaskGroup] = (1 to n).map {
    _ =>
      val p = q.poll(0L)
      q.finished(p, nanos)
      p.group
  }

  test("should put groups at levels by their run time") {
    val q = FairTaskQueue[Int]()
    q.levelOf(0) shouldBe 0
    q.levelOf(second - 1) shouldBe 0
    q.levelOf(second) shouldBe 1
    q.levelOf(59 * second) shouldBe 2
    q.levelOf(1000 * second) shouldBe 4
  }

  test("should run one group's tasks in FIFO order") {
    val q = FairTaskQueue[Int]()
    val g = group()
    (1 to 5).foreach(i => q.add(g, i, 0L))
    q.size shouldBe 5
    (1 to 5).map(_ => q.poll(0L).task) shouldBe (1 to 5)
    q.poll(0L) shouldBe null
    q.isEmpty shouldBe true
  }

  test("should alternate between groups of one level") {
    val q = FairTaskQueue[Int]()
    val a = group()
    val b = group()
    (1 to 3).foreach(i => q.add(a, i, 0L))
    (1 to 3).foreach(i => q.add(b, i, 0L))
    drain(q, 6, 1000L) shouldBe Seq(a, b, a, b, a, b)
  }

  test("should run the group that ran less first within a level") {
    val q    = FairTaskQueue[Int]()
    val ran  = group(50000000L) // 50 ms, level 0
    val idle = group()
    (1 to 10).foreach(i => q.add(ran, i, 0L))
    (1 to 10).foreach(i => q.add(idle, i, 0L))
    // idle catches up at 10 ms per task, then the earlier queued group goes first on the tie
    drain(q, 7, 10000000L) shouldBe Seq(idle, idle, idle, idle, idle, ran, idle)
  }

  test("should count running tasks as run time") {
    val q    = FairTaskQueue[Int]()
    val busy = group()
    (1 to 10).foreach(i => q.add(busy, i, 0L))
    // busy's first two tasks are running, without having finished any
    val running = Seq(q.poll(0L), q.poll(0L))
    running.map(_.group) shouldBe Seq(busy, busy)
    val idle = group()
    q.add(idle, 1, 10000000L)
    q.poll(10000000L).group shouldBe idle
    running.foreach(q.finished(_, 10000000L))
    busy.runNanos shouldBe 20000000L
    q.poll(10000000L).group shouldBe busy
  }

  test("should give a new group most of the pool next to a long one, without starving it") {
    val q    = FairTaskQueue[Int]()
    val long = group(70 * second) // level 3
    (1 to 1000).foreach(i => q.add(long, i, 0L))
    // The long group runs alone for a while
    drain(q, 100, 10000000L)
    val short = group()
    (1 to 100).foreach(i => q.add(short, i, 0L))
    val picks = drain(q, 90, 10000000L)
    // Level 0 gets 2^3 times level 3's share
    picks.count(_ eq short) shouldBe 80
    picks.count(_ eq long) shouldBe 10
  }

  test("should move a group down a level once its tasks ran past the threshold") {
    val q     = FairTaskQueue[Int]()
    val long  = group()
    val other = group(2 * second) // level 1
    (1 to 10).foreach(i => q.add(long, i, 0L))
    (1 to 10).foreach(i => q.add(other, i, 0L))
    // Level 1 weighs its run time twice: long takes two of the first three turns, then moves to
    // level 1 at 1 s, where it goes first until it has run as long as other
    drain(q, 6, second / 2) shouldBe Seq(long, other, long, long, long, long)
    long.runNanos shouldBe 5 * second / 2
    q.levelOf(long.runNanos) shouldBe 1
  }

  test("should count coordinator time apart from pool-task time") {
    val q = FairTaskQueue[Int](coordinators = true)
    val g = group()
    q.add(g, 1, 0L)
    q.finished(q.poll(0L), 5000L)
    g.coordinatorNanos shouldBe 5000L
    g.finishedCoordinators shouldBe 1L
    g.runNanos shouldBe 0L
  }

end FairTaskQueueTest
