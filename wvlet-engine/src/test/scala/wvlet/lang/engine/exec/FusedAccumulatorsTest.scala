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

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

import java.math.BigDecimal as JBigDecimal

class FusedAccumulatorsTest extends UniTest:
  private val dec    = PType.Decimal(15, 2)
  private val wide   = PType.Decimal(18, 0)
  private val sumDec = Aggregates.resultType("sum", Seq(dec)).asInstanceOf[PType.Decimal]
  private val sumBig = Aggregates.resultType("sum", Seq(wide)).asInstanceOf[PType.Decimal]
  private val in     = Schema(
    IndexedSeq(
      Field("k", PType.Int64),
      Field("x", dec),
      Field("y", dec),
      Field("w", wide),
      Field("f", PType.Bool)
    )
  )

  private val k = ColRef(0, PType.Int64, "k")
  private val x = ColRef(1, dec, "x")
  private val y = ColRef(2, dec, "y")
  private val w = ColRef(3, wide, "w")
  private val f = ColRef(4, PType.Bool, "f")

  private val aggs = IndexedSeq(
    AggCall("sum", Seq(x), false, None, sumDec),
    AggCall("count", Seq(x), false, None, PType.Int64),
    AggCall("sum", Seq(y), false, None, sumDec),
    AggCall("count", Seq(y), false, None, PType.Int64),
    AggCall("sum", Seq(w), false, None, sumBig),
    AggCall("count_star", Nil, false, None, PType.Int64),
    AggCall("sum", Seq(x), false, Some(f), sumDec),
    AggCall("count", Seq(y), false, Some(f), PType.Int64),
    AggCall("sum", Seq(x), true, None, sumDec)
  )

  /**
    * Batches over `groups` keys: `y` has NULLs in every other batch, and `w` holds values near the
    * Long range, so that its sums overflow the unscaled Longs. Group 0 only appears in the last
    * batch, so that it is unseen by the sums until then
    */
  private def input(n: Int, batch: Int, groups: Int): Seq[Batch] =
    val rnd = scala.util.Random(7)
    (0 until n)
      .grouped(batch)
      .zipWithIndex
      .map { (rows, b) =>
        val m    = rows.size
        val last = rows.last == n - 1
        Batch(
          Array(
            LongVec(
              PType.Int64,
              Array.fill(m)(
                if last then
                  rnd.nextInt(groups).toLong
                else
                  1L + rnd.nextInt(groups - 1)
              ),
              null
            ),
            LongVec(dec, Array.fill(m)(rnd.nextLong() % 100000000L), null),
            LongVec(
              dec,
              Array.fill(m)(rnd.nextLong() % 100000000L),
              if b % 2 == 1 then
                Array.fill(m)(rnd.nextInt(4) == 0)
              else
                null
            ),
            LongVec(wide, Array.fill(m)(Long.MaxValue - rnd.nextLong(1000L)), null),
            BoolVec(Array.fill(m)(rnd.nextBoolean()), null)
          ),
          m
        )
      }
      .toSeq

  end input

  private def aggregate(
      batches: Seq[Batch],
      fuse: Boolean,
      calls: IndexedSeq[AggCall] = aggs,
      columnMajor: Boolean = false
  ): Map[Any, Seq[Any]] =
    val outSchema = Schema(
      Field("k", PType.Int64) +: calls.indices.map(i => Field(s"a${i}", calls(i).ptype))
    )
    val op = HashAggOp(BatchesOp(in, batches), IndexedSeq(k), calls, outSchema)
      .withFusedAccumulators(fuse)
      .withColumnMajorSums(columnMajor)
    val out =
      try op.drain()
      finally op.close()
    out
      .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toSeq))
      .map(r => r.head -> r.tail)
      .toMap

  test("should fold counts and decimal sums over few groups as separate accumulators do") {
    val batches = input(20000, 1024, groups = 4)
    val fused   = aggregate(batches, fuse = true)
    fused shouldBe aggregate(batches, fuse = false)
    fused.size shouldBe 4
  }

  test("should fold decimal sums without a count over few groups as separate accumulators do") {
    val batches = input(20000, 1024, groups = 4)
    val sums    = IndexedSeq(aggs(0), aggs(2), aggs(4))
    val fused   = aggregate(batches, fuse = true, sums)
    fused shouldBe aggregate(batches, fuse = false, sums)
    fused.size shouldBe 4
  }

  test("should fold counts and decimal sums over many groups as separate accumulators do") {
    val batches = input(20000, 1024, groups = 3000)
    aggregate(batches, fuse = true) shouldBe aggregate(batches, fuse = false)
  }

  test("should sum decimals beyond the Long range exactly") {
    val batches  = input(5000, 512, groups = 4)
    val expected =
      batches
        .flatMap { b =>
          val keys = b.columns(0)
          val ws   = b.columns(3)
          (0 until b.size).map(i => keys.get(i) -> ws.get(i).asInstanceOf[JBigDecimal])
        }
        .groupMapReduce(_._1)(_._2)(_.add(_))
    val fused = aggregate(batches, fuse = true)
    fused.map((key, v) => key -> v(4).asInstanceOf[JBigDecimal].stripTrailingZeros) shouldBe
      expected.map((key, v) => key -> v.stripTrailingZeros)
  }

  test("should fold column-major as separate accumulators do, in batches of odd sizes") {
    for groups <- Seq(4, 60) do
      val batches = input(20000, 1023, groups)
      aggregate(batches, fuse = true, columnMajor = true) shouldBe aggregate(batches, fuse = false)
  }

  test("should fold column-major sums without a count as separate accumulators do") {
    val batches = input(20000, 1021, groups = 4)
    val sums    = IndexedSeq(aggs(0), aggs(2), aggs(4))
    aggregate(batches, fuse = true, sums, columnMajor = true) shouldBe
      aggregate(batches, fuse = false, sums)
  }

  test("should sum large decimals column-major when their magnitudes don't bound the batch") {
    // Magnitudes near 2^55: the bound fails at 1024 rows, though the sums fit in a Long
    val rnd     = scala.util.Random(11)
    val batches = (0 until 8).map { _ =>
      Batch(
        Array(
          LongVec(PType.Int64, Array.fill(1024)(rnd.nextInt(4).toLong), null),
          LongVec(dec, Array.fill(1024)(rnd.nextLong() >> 8), null),
          LongVec(dec, Array.fill(1024)(rnd.nextLong() % 1000L), null),
          LongVec(wide, Array.fill(1024)(Long.MinValue + rnd.nextLong(1000L)), null),
          BoolVec(Array.fill(1024)(false), null)
        ),
        1024
      )
    }
    val sums = IndexedSeq(aggs(0), aggs(2), aggs(4), aggs(5))
    aggregate(batches, fuse = true, sums, columnMajor = true) shouldBe
      aggregate(batches, fuse = false, sums)
  }

  test("should bound batch sums by the values' magnitudes") {
    ColumnMajorSums.bounded(0L, 2048) shouldBe true
    ColumnMajorSums.bounded((1L << 49) - 1, 2048) shouldBe true
    ColumnMajorSums.bounded(1L << 49, 2048) shouldBe true
    ColumnMajorSums.bounded(1L << 50, 2048) shouldBe false
    ColumnMajorSums.bounded(Long.MaxValue, 1) shouldBe false
  }

end FusedAccumulatorsTest
