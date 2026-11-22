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
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class MorselOpTest extends UniTest:
  private val runner = ThreadPoolRunner(4)

  override def afterAll: Unit = runner.shutdown()

  private val schema = Schema(IndexedSeq(Field("x", PType.Int64)))

  /** A source of `parts` parts holding 100 consecutive values each */
  private class Splittable(parts: Int, failAt: Int = -1) extends TableSource:
    val name                        = "t"
    val schema                      = MorselOpTest.this.schema
    val read                        = AtomicInteger(0)
    val closed                      = AtomicBoolean(false)
    override def canSplit           = true
    private def part(k: Int): Batch =
      if k == failAt then
        throw IllegalStateException(s"part ${k} failed")
      // Later parts finish first, so ordering can't come from completion order
      Thread.sleep((parts - k).toLong)
      read.incrementAndGet()
      Batch(Array(LongVec(PType.Int64, Array.tabulate(100)(i => (k * 100 + i).toLong), null)), 100)

    def scan(spec: ScanSpec): Iterator[Batch]                   = Iterator.range(0, parts).map(part)
    override def scanSplits(spec: ScanSpec): Option[ScanSplits] =
      if parts <= 1 then
        None
      else
        Some(
          ScanSplits(
            (0 until parts).map(k => () => Iterator.single(part(k))),
            () => closed.set(true)
          )
        )

  private def evenFilter(input: Operator): Operator = FilterOp(
    input,
    Compare(
      CmpOp.Eq,
      Arith(ArithOp.Mod, ColRef(0, PType.Int64, "x"), Const(2L, PType.Int64), PType.Int64),
      Const(0L, PType.Int64)
    )
  )

  private def values(op: Operator): Seq[Long] =
    try op.drain().flatMap(b => (0 until b.size).map(i => b.column(0).get(i).asInstanceOf[Long]))
    finally op.close()

  test("should emit parts in order through the pipeline") {
    val src = Splittable(20)
    val op  = MorselOp(ScanOp(src, schema, None, None), evenFilter, schema, runner)
    values(op) shouldBe (0L until 2000L by 2L)
    src.closed.get() shouldBe true
  }

  test("should stop reading parts when closed early") {
    val src = Splittable(100)
    val op  = LimitOp(MorselOp(ScanOp(src, schema, None, None), identity, schema, runner), 5, 0)
    values(op) shouldBe (0L until 5L)
    src.closed.get() shouldBe true
    // At most the in-flight window was read
    (src.read.get() <= 2 * runner.parallelism + 1) shouldBe true
  }

  test("should rethrow a failing part") {
    val src = Splittable(10, failAt = 3)
    val op  = MorselOp(ScanOp(src, schema, None, None), identity, schema, runner)
    intercept[IllegalStateException] {
      values(op)
    }
    src.closed.get() shouldBe true
  }

  test("should scan sequentially when the source has a single part") {
    val src = Splittable(1)
    values(MorselOp(ScanOp(src, schema, None, None), evenFilter, schema, runner)) shouldBe
      (0L until 100L by 2L)
  }

end MorselOpTest
