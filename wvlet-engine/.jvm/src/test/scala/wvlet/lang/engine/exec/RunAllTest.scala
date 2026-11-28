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

/** [[TaskRunner.runAll]] with real pool threads */
class RunAllTest extends UniTest:

  test("should return every task's result in order") {
    val runner = ThreadPoolRunner(4)
    try TaskRunner.runAll(runner, 10)(i => i * i) shouldBe (0 until 10).map(i => i * i)
    finally runner.shutdown()
  }

  test("should not start queued tasks after a failure, and wait for the running ones") {
    // Two threads: the failing task and a slow one run; the rest queue behind them
    val runner = ThreadPoolRunner(2)
    try
      val started  = CountDownLatch(2)
      val slowDone = AtomicInteger(0)
      val queued   = AtomicInteger(0)
      val e        = intercept[IllegalStateException] {
        TaskRunner.runAll(runner, 20) { i =>
          if i == 0 then
            started.countDown()
            started.await(10, TimeUnit.SECONDS)
            throw IllegalStateException("part failed")
          else if i == 1 then
            started.countDown()
            Thread.sleep(200)
            slowDone.incrementAndGet()
          else
            queued.incrementAndGet()
        }
      }
      e.getMessage shouldBe "part failed"
      // The running task finished before runAll returned
      slowDone.get() shouldBe 1
      // The failure came after both started, so the tasks queued behind them never ran
      queued.get() shouldBe 0
    finally
      runner.shutdown()
  }

  test("should wait for a running task after cancel, and drop a queued one") {
    val runner = ThreadPoolRunner(2)
    try
      val started = CountDownLatch(1)
      val done    = AtomicInteger(0)
      val running = runner.submit { () =>
        started.countDown()
        Thread.sleep(200)
        done.incrementAndGet()
      }
      // Holds the second thread, so the next task stays queued
      val blocker = runner.submit(() => Thread.sleep(200))
      started.await(10, TimeUnit.SECONDS) shouldBe true
      val queued = runner.submit(() => done.addAndGet(100))
      running.cancel()
      queued.cancel()
      // A closing operator cancels then waits: get() returns only once the running task ends
      running.get() shouldBe 1
      done.get() shouldBe 1
      intercept[java.util.concurrent.CancellationException](queued.get())
      blocker.get()
      done.get() shouldBe 1
    finally
      runner.shutdown()
  }

end RunAllTest
