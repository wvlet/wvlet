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

import java.math.BigDecimal as JBigDecimal

/** The spill block format keeps every spillable vector exactly, on every platform */
class SpillBlocksTest extends UniTest:
  private def roundTrip(schema: Schema, b: Batch): Batch = SpillBlocks.decode(
    schema,
    SpillBlocks.encode(b)
  )

  private def rowsOf(b: Batch): Seq[Seq[Any]] = (0 until b.size).map(i => b.row(i).toSeq)

  private val nulls = Array(false, true, false, false)

  test("should round-trip primitive, decimal, and null columns with their nulls") {
    val schema = Schema(
      IndexedSeq(
        Field("b", PType.Bool),
        Field("i", PType.Int32),
        Field("d", PType.Date),
        Field("l", PType.Int64),
        Field("ts", PType.Timestamp),
        Field("dec", PType.Decimal(12, 2)),
        Field("wide", PType.Decimal(38, 4)),
        Field("f", PType.Float64),
        Field("n", PType.Null)
      )
    )
    val b = Batch(
      Array(
        BoolVec(Array(true, false, false, true), nulls),
        IntVec(PType.Int32, Array(1, 0, Int.MinValue, Int.MaxValue), nulls),
        IntVec(PType.Date, Array(19000, 0, -1, 3), null),
        LongVec(PType.Int64, Array(Long.MinValue, 0L, -1L, Long.MaxValue), nulls),
        LongVec(PType.Timestamp, Array(1L, 2L, 3L, 4L), null),
        LongVec(PType.Decimal(12, 2), Array(12345L, 0L, -99L, 1L), nulls),
        ObjVec(
          PType.Decimal(38, 4),
          Array(
            JBigDecimal("123456789012345678901234.5678"),
            null,
            JBigDecimal("-0.0001"),
            JBigDecimal("0")
          ),
          nulls
        ),
        DoubleVec(Array(-0.0, 0.0, Double.NaN, Double.NegativeInfinity), nulls),
        Vec.nullVec(PType.Null, 4)
      ),
      4
    )
    val r = roundTrip(schema, b)
    r.size shouldBe 4
    rowsOf(r).map(_.toString) shouldBe rowsOf(b).map(_.toString)
    val f = r.column(7).asInstanceOf[DoubleVec].values
    java.lang.Double.doubleToRawLongBits(f(0)) shouldBe java.lang.Double.doubleToRawLongBits(-0.0)
    r.column(5).ptype shouldBe PType.Decimal(12, 2)
  }

  test("should round-trip plain and entry-coded strings") {
    val schema = Schema(IndexedSeq(Field("s", PType.Str), Field("e", PType.Str)))
    val plain  = StrVec(Array("abc", null, "", "日本語 😀 \uD800"), nulls)
    val coded  = StrVec.ofEntries(Array("x", "yy"), Array(1, -1, 0, 1), nulls, 4)
    val r      = roundTrip(schema, Batch(Array(plain, coded), 4))
    rowsOf(r) shouldBe rowsOf(Batch(Array(plain, coded), 4))
    r.column(1).asInstanceOf[StrVec].holdsEntries shouldBe true
  }

  test("should round-trip an empty batch and gather deferred columns") {
    val schema = Schema(IndexedSeq(Field("i", PType.Int64)))
    roundTrip(schema, Batch.empty(schema)).size shouldBe 0
    val src      = Batch(Array(LongVec(PType.Int64, Array(10L, 20L, 30L), null)), 3)
    val deferred = src.take(Array(2, 0), 2)
    rowsOf(roundTrip(schema, deferred)) shouldBe Seq(Seq(30L), Seq(10L))
  }

  test("should not spill nested values") {
    SpillBlocks.spillable(Schema(IndexedSeq(Field("a", PType.AnyT)))) shouldBe false
    SpillBlocks.spillable(Schema(IndexedSeq(Field("s", PType.Str)))) shouldBe true
  }

end SpillBlocksTest
