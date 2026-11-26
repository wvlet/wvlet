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

class AggScratchBuffersTest extends UniTest:
  private val dec = PType.Decimal(15, 2)
  private val in  = Schema(
    IndexedSeq(
      Field("flag", PType.Str),
      Field("status", PType.Str),
      Field("price", dec),
      Field("disc", dec),
      Field("tax", dec)
    )
  )

  private def col(i: Int): ColRef = ColRef(i, in(i).ptype, in(i).name)

  private val (flag, status, price, disc, tax) = (col(0), col(1), col(2), col(3), col(4))

  private val one = Const(java.math.BigDecimal("1.00"), PType.Decimal(16, 2))

  // q1's `price * (1 - disc)` and `price * (1 - disc) * (1 + tax)`, fused
  private val discPrice = FusedDecimalMul(
    price,
    Arith(ArithOp.Sub, one, disc, PType.Decimal(16, 2)),
    innerLeft = false,
    PType.Decimal(31, 4)
  )

  private val charge = FusedDecimalMul(
    discPrice,
    Arith(ArithOp.Add, one, tax, PType.Decimal(16, 2)),
    innerLeft = false,
    PType.Decimal(38, 6)
  )

  private def agg(name: String, arg: VExpr): AggCall = AggCall(
    name,
    Seq(arg),
    false,
    None,
    Aggregates.resultType(name, Seq(arg.ptype))
  )

  private val aggs = IndexedSeq(
    agg("sum", price),
    agg("sum", discPrice),
    agg("sum", charge),
    agg("avg", disc),
    AggCall("count_star", Seq.empty, false, None, PType.Int64)
  )

  private val flags    = Array("A", "N", "R")
  private val statuses = Array("F", "O")

  /**
    * Batches of the given sizes whose rows a filter keeps at increasing positions (9 out of 10).
    * With `uncoded`, a few key rows are NULL without a dictionary code (-1)
    */
  private def input(sizes: Seq[Int], uncoded: Boolean = false): Seq[Batch] =
    val rnd = scala.util.Random(11)
    sizes.map { m =>
      def coded(dict: Array[String]): Vec =
        val codes = Array.fill(m)(rnd.nextInt(dict.length))
        if uncoded then
          (0 until m by 97).foreach(codes(_) = -1)
        val values = codes.map(c =>
          if c < 0 then
            null
          else
            dict(c)
        )
        val nulls =
          if uncoded then
            codes.map(_ < 0)
          else
            null
        StrVec(values, nulls, codes, dict.length)
      def decs(bound: Int): Vec = LongVec(dec, Array.fill(m)(rnd.nextInt(bound).toLong), null)
      val src                   = Batch(
        Array[Vec](coded(flags), coded(statuses), decs(10000000), decs(11), decs(9)),
        m
      )
      val keep = (0 until m).filter(_ => rnd.nextInt(10) < 9).toArray
      src.take(keep, keep.length)
    }

  private def aggregate(
      batches: Seq[Batch],
      keys: IndexedSeq[VExpr],
      scratch: Boolean,
      dense: Boolean = true
  ): Map[Seq[Any], Seq[Any]] =
    val outSchema = Schema(
      keys.zipWithIndex.map((c, i) => Field(s"k${i}", c.ptype)) ++
        aggs.indices.map(i => Field(s"a${i}", aggs(i).ptype))
    )
    val op = HashAggOp(BatchesOp(in, batches), keys, aggs, outSchema)
      .withDenseSelected(dense)
      .withScratchBuffers(scratch)
    val out =
      try op.drain()
      finally op.close()
    out
      .flatMap(r => (0 until r.size).map(i => r.columns.map(_.get(i)).toSeq))
      .map(r => r.take(keys.size) -> r.drop(keys.size))
      .toMap

  private def same(batches: => Seq[Batch], keys: IndexedSeq[VExpr]): Map[Seq[Any], Seq[Any]] =
    val reused = aggregate(batches, keys, scratch = true)
    reused shouldBe aggregate(batches, keys, scratch = false)
    reused shouldBe aggregate(batches, keys, scratch = false, dense = false)
    reused

  test("should aggregate fused products into reused arrays as into fresh ones") {
    // Batch sizes change, so some batches take arrays of a new size
    same(input(Seq(2048, 2048, 1500, 2048, 700)), IndexedSeq(flag, status)).size shouldBe 6
  }

  test("should group one or two coded string keys in one pass as column by column") {
    same(input(Seq(2048, 2048, 2048)), IndexedSeq(flag)).size shouldBe 3
    same(input(Seq(2048, 2048, 2048)), IndexedSeq(status, flag)).size shouldBe 6
  }

  test("should group rows whose keys have no dictionary code") {
    val groups = same(input(Seq(2048, 2048, 1000), uncoded = true), IndexedSeq(flag, status))
    groups.keys.exists(_.contains(null)) shouldBe true
  }

  test("should hand out distinct arrays until reset, then the same ones") {
    val s = LongScratch()
    val a = s.take(8)
    val b = s.take(8)
    (a eq b) shouldBe false
    s.reset()
    (s.take(8) eq a) shouldBe true
    // A new size takes a new array
    val c = s.take(4)
    (c eq b) shouldBe false
    c.length shouldBe 4
  }

end AggScratchBuffersTest
