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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** [[TaskRunner.runTogether]] with real coordinator threads */
class RunTogetherTest extends UniTest:

  test("should run every task once, at the same time") {
    val runner = ThreadPoolRunner(8)
    try
      // Each task waits until all have started: they can only finish when they overlap
      val all  = CountDownLatch(3)
      val runs = AtomicInteger(0)
      val task =
        () =>
          runs.incrementAndGet()
          all.countDown()
          all.await(10, TimeUnit.SECONDS) shouldBe true
      TaskRunner.runTogether(runner, Seq(task, task, task))
      runs.get() shouldBe 3
    finally
      runner.shutdown()
  }

  test("should run queued tasks on the calling thread when coordinators are busy") {
    // One coordinator (parallelism 2 / 2), held by an outer task that runs nested tasks
    val runner = ThreadPoolRunner(2)
    try
      val runs = AtomicInteger(0)
      val leaf =
        () =>
          runs.incrementAndGet()
          ()
      val nested = () => TaskRunner.runTogether(runner, Seq(leaf, leaf, leaf))
      TaskRunner.runTogether(runner, Seq(nested, nested, nested))
      runs.get() shouldBe 9
    finally
      runner.shutdown()
  }

  test("should raise the first failure after the started tasks finish") {
    val runner = ThreadPoolRunner(4)
    try
      val started = CountDownLatch(1)
      val done    = AtomicInteger(0)
      val slow    =
        () =>
          started.countDown()
          Thread.sleep(100)
          done.incrementAndGet()
          ()
      val failing =
        () =>
          started.await()
          throw IllegalStateException("build failed")
      val e = intercept[IllegalStateException] {
        TaskRunner.runTogether(runner, Seq(failing, slow))
      }
      e.getMessage shouldBe "build failed"
      done.get() shouldBe 1
    finally
      runner.shutdown()
  }

  test("should run tasks in order on a sequential runner") {
    val order = scala.collection.mutable.ArrayBuffer.empty[Int]
    TaskRunner.runTogether(TaskRunner.Sequential, (1 to 3).map(i => () => order += i: Unit))
    order.toSeq shouldBe Seq(1, 2, 3)
  }

end RunTogetherTest
