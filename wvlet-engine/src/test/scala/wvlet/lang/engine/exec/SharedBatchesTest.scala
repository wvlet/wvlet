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

import java.util.concurrent.atomic.AtomicInteger

class SharedBatchesTest extends UniTest:

  /** A shared result counting how many times it is computed */
  private class Counted(fail: Boolean = false) extends SharedBatches:
    val runs           = AtomicInteger(0)
    private val result = ComputeOnce { () =>
      runs.incrementAndGet()
      if fail then
        throw IllegalStateException("failed body")
      Seq.empty[Batch]
    }

    def op: Operator        = IteratorOp(Schema(IndexedSeq.empty), () => Iterator.empty, "x")
    def isComputed: Boolean = result.isStarted
    def compute(stop: () => Boolean): Unit = result.runUnless(stop)
    def awaitIfStarted(): Unit             = result.awaitIfStarted()
    def batches: Seq[Batch]                = result.get()

  private def child = IteratorOp(Schema(IndexedSeq.empty), () => Iterator.empty, "root")

  test("should compute a value once when many threads ask for it") {
    val n    = AtomicInteger(0)
    val once = ComputeOnce { () =>
      n.incrementAndGet()
    }
    val runner = TaskRunner.default
    val hs     = (0 until 16).map(_ => runner.coordinate(() => once.get()))
    hs.map(_.get()).distinct shouldBe Seq(1)
    once.get() shouldBe 1
    n.get() shouldBe 1
  }

  test("should keep a failure and raise it only when the value is read") {
    val once = ComputeOnce[Int](() => throw IllegalStateException("boom"))
    once.run()
    once.isStarted shouldBe true
    intercept[IllegalStateException](once.get()).getMessage shouldBe "boom"
    intercept[IllegalStateException](once.get()).getMessage shouldBe "boom"
  }

  test("should start shared results when the plan starts, not when it is built") {
    val a  = Counted()
    val b  = Counted()
    val op = StartSharedOp(child, Seq(a, b), TaskRunner.default)
    a.isComputed shouldBe false
    op.next() shouldBe null
    // Readers read the results the coordinators compute, or compute them if none has started
    a.batches shouldBe Seq.empty
    b.batches shouldBe Seq.empty
    op.close()
    a.runs.get() shouldBe 1
    b.runs.get() shouldBe 1
  }

  test("should not fail the plan for a shared result that fails but is never read") {
    val bad = Counted(fail = true)
    val op  = StartSharedOp(child, Seq(bad), TaskRunner.default)
    op.next() shouldBe null
    // Closing waits for a running coordinator and drops a queued one, without raising its failure
    op.close()
    intercept[IllegalStateException](bad.batches)
    bad.runs.get() shouldBe 1
  }

  test("should compute nothing when closed before it starts") {
    val a  = Counted()
    val op = StartSharedOp(child, Seq(a), TaskRunner.default)
    op.close()
    a.isComputed shouldBe false
  }

end SharedBatchesTest
