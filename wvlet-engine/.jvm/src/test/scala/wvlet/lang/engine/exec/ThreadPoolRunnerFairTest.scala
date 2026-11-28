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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/** [[ThreadPoolRunner]]'s tasks of [[TaskGroup]]s, on the fair pool */
class ThreadPoolRunnerFairTest extends UniTest:

  test("should run tasks in the submitter's group, and their own tasks too") {
    val runner = ThreadPoolRunner(2)
    try
      val g                           = TaskGroup.newGroup()
      val (outer, inner, coordinated) =
        TaskGroup.withGroup(g) {
          runner
            .submit { () =>
              val inner = runner.submit(() => TaskGroup.current).get()
              (TaskGroup.current, inner, runner.coordinate(() => TaskGroup.current).get())
            }
            .get()
        }
      outer shouldBeTheSameInstanceAs g
      inner shouldBeTheSameInstanceAs g
      coordinated shouldBeTheSameInstanceAs g
      // Outside a group, tasks run on the FIFO pool, without a group
      runner.submit(() => TaskGroup.current).get() shouldBe null
      g.runNanos > 0 shouldBe true
    finally
      runner.shutdown()
  }

  test("should rethrow a task's exception") {
    val runner = ThreadPoolRunner(2)
    try
      val h = TaskGroup.asQuery()(runner.submit(() => throw IllegalStateException("boom")))
      intercept[IllegalStateException](h.get()).getMessage shouldBe "boom"
    finally
      runner.shutdown()
  }

  test("should run a short query's tasks before a long query's queued tasks") {
    val runner = ThreadPoolRunner(2)
    try
      val done                            = ConcurrentLinkedQueue[String]()
      def busy(label: String): () => Unit =
        () =>
          Thread.sleep(2)
          done.add(label)
      val longs  = TaskGroup.asQuery()((1 to 200).map(_ => runner.submit(busy("long"))))
      val shorts = TaskGroup.asQuery()((1 to 10).map(_ => runner.submit(busy("short"))))
      shorts.foreach(_.get())
      val longsBefore = AtomicInteger(0)
      val it          = done.iterator()
      var shortsSeen  = 0
      while shortsSeen < 10 && it.hasNext do
        if it.next() == "short" then
          shortsSeen += 1
        else
          longsBefore.incrementAndGet()
      // FIFO would run all 200 long tasks first; the groups take turns instead
      longsBefore.get() < 50 shouldBe true
      longs.foreach(_.get())
    finally
      runner.shutdown()
  }

  test("should run fair coordinator tasks in their group, counting coordinator time") {
    val runner = ThreadPoolRunner(2)
    try
      val g                       = TaskGroup.newGroup(fairCoordinators = true)
      val (inCoordinator, inTask) =
        TaskGroup.withGroup(g) {
          runner
            .coordinate { () =>
              Thread.sleep(2)
              (TaskGroup.current, runner.submit(() => TaskGroup.current).get())
            }
            .get()
        }
      inCoordinator shouldBeTheSameInstanceAs g
      inTask shouldBeTheSameInstanceAs g
      g.finishedCoordinators shouldBe 1L
      g.coordinatorNanos >= 2000000L shouldBe true
      // Only the pool task counts as run time
      g.finishedTasks shouldBe 1L
    finally
      runner.shutdown()
  }

end ThreadPoolRunnerFairTest
