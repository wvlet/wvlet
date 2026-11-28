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

import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

import java.util.concurrent.atomic.AtomicInteger

/** Morsel parts submitted within the scan decode budget (`bound_scan_decode`) */
class ScanDecodeBudgetTest extends UniTest:
  private val runner = ThreadPoolRunner(8)

  override def afterAll: Unit = runner.shutdown()

  private val schema = Schema(IndexedSeq(Field("x", PType.Int64)))

  /** A source of `parts` parts of 100 values each, `bytes` each, recording how many run at once */
  private class Counting(parts: Int, bytes: Long) extends TableSource:
    val name              = "t"
    val schema            = ScanDecodeBudgetTest.this.schema
    val running           = AtomicInteger(0)
    val maxRunning        = AtomicInteger(0)
    override def canSplit = true

    private def part(k: Int): Batch =
      val now = running.incrementAndGet()
      maxRunning.accumulateAndGet(now, math.max)
      try
        Thread.sleep(5)
        Batch(
          Array(LongVec(PType.Int64, Array.tabulate(100)(i => (k * 100 + i).toLong), null)),
          100
        )
      finally
        running.decrementAndGet()

    def scan(spec: ScanSpec): Iterator[Batch]                   = Iterator.range(0, parts).map(part)
    override def scanSplits(spec: ScanSpec): Option[ScanSplits] = Some(
      ScanSplits(
        (0 until parts).map(k => () => Iterator.single(part(k))),
        () => (),
        IndexedSeq.fill(parts)(100L),
        IndexedSeq.fill(parts)(bytes)
      )
    )

  end Counting

  private def run(source: Counting, budget: ScanDecodeBudget): (Seq[Long], MorselOp) =
    val op = MorselOp(ScanOp(source, schema, None, None), x => x, schema, runner).withDecodeBudget(
      budget
    )
    try (
      op.drain().flatMap(b => (0 until b.size).map(i => b.column(0).get(i).asInstanceOf[Long])),
      op
    )
    finally op.close()

  test("should run at most as many parts as fit the budget, with the same rows") {
    val source     = Counting(40, 100L)
    val budget     = ScanDecodeBudget(250L)
    val (rows, op) = run(source, budget)
    rows shouldBe (0 until 4000).map(_.toLong)
    (source.maxRunning.get() <= 2) shouldBe true
    budget.peakBytes shouldBe 200L
    (op.budgetWaits > 0) shouldBe true
    budget.inFlightBytes shouldBe 0L
  }

  test("should run a part larger than the budget alone") {
    val source = Counting(10, 1000L)
    val budget = ScanDecodeBudget(250L)
    run(source, budget)._1.size shouldBe 1000
    source.maxRunning.get() shouldBe 1
    budget.inFlightBytes shouldBe 0L
  }

  test("should run up to the window without a budget") {
    val source = Counting(40, 100L)
    run(source, null)._1.size shouldBe 4000
    (source.maxRunning.get() > 2) shouldBe true
  }

  test("should count a query's parts in flight apart from its reservations") {
    val memory = MemoryPool().newQuery()
    val source = Counting(20, 100L)
    MemoryContext.withCurrent(memory) {
      run(source, ScanDecodeBudget(250L))
    }
    memory.peakScanBytes shouldBe 200L
    memory.scanBytes shouldBe 0L
    memory.peakBytes shouldBe 0L
    memory.close()
  }

  test("should return the bytes of parts dropped when the operator closes early") {
    val budget = ScanDecodeBudget(1000L)
    val op     = MorselOp(ScanOp(Counting(40, 100L), schema, None, None), x => x, schema, runner)
      .withDecodeBudget(budget)
    op.next() shouldNotBe null
    op.close()
    budget.inFlightBytes shouldBe 0L
  }

  test("should drop the budget with bound_scan_decode off") {
    val op = MorselOp(ScanOp(Counting(4, 100L), schema, None, None), x => x, schema, runner)
    op.decodeBudgetOrNull shouldBeTheSameInstanceAs ScanDecodeBudget.process
    wvlet.lang.engine.plan.Planner.disableScanDecodeBudget(op)
    op.decodeBudgetOrNull shouldBe null
    op.close()
  }

  test("should acquire past the budget only when forced or empty") {
    val b = ScanDecodeBudget(100L)
    b.tryAcquire(80L, force = false) shouldBe true
    b.tryAcquire(30L, force = false) shouldBe false
    b.tryAcquire(30L, force = true) shouldBe true
    b.release(110L)
    b.tryAcquire(500L, force = false) shouldBe true
    b.peakBytes shouldBe 500L
  }

end ScanDecodeBudgetTest
