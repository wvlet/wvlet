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

import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

/** [[StartSharedOp]] with real coordinator threads */
class StartSharedOpTest extends UniTest:
  private def child = IteratorOp(Schema(IndexedSeq.empty), () => Iterator.empty, "root")

  /** A shared result that blocks once it starts, until released */
  private class Blocking extends SharedBatches:
    val started        = CountDownLatch(1)
    val release        = CountDownLatch(1)
    val runs           = AtomicInteger(0)
    private val result = ComputeOnce { () =>
      runs.incrementAndGet()
      started.countDown()
      release.await()
      Seq.empty[Batch]
    }

    def op: Operator                       = child
    def isComputed: Boolean                = result.isStarted
    def compute(stop: () => Boolean): Unit = result.runUnless(stop)
    def awaitIfStarted(): Unit             = result.awaitIfStarted()

  test("should wait on close for the shared results that coordinators started") {
    val runner = ThreadPoolRunner(4)
    try
      val slow = Blocking()
      val op   = StartSharedOp(child, Seq(slow), runner)
      op.next() shouldBe null
      slow.started.await()
      val closer = Thread(() => op.close())
      closer.start()
      // close() is still waiting for the running computation
      closer.join(200)
      closer.isAlive shouldBe true
      slow.release.countDown()
      closer.join()
      slow.runs.get() shouldBe 1
    finally
      runner.shutdown()
  }

  test("should not start queued shared results after close") {
    // One coordinator (parallelism 2 / 2): the second result queues behind the first
    val runner = ThreadPoolRunner(2)
    try
      val first  = Blocking()
      val second = Blocking()
      val op     = StartSharedOp(child, Seq(first, second), runner)
      op.next() shouldBe null
      first.started.await()
      val closer = Thread(() => op.close())
      closer.start()
      closer.join(100)
      first.release.countDown()
      closer.join()
      second.isComputed shouldBe false
      second.runs.get() shouldBe 0
    finally
      runner.shutdown()
  }

end StartSharedOpTest
