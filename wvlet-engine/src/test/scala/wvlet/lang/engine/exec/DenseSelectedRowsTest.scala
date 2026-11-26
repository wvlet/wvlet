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

class DenseSelectedRowsTest extends UniTest:
  private val dec = PType.Decimal(15, 2)
  private val in  = Schema(
    IndexedSeq(
      Field("flag", PType.Str),
      Field("status", PType.Str),
      Field("qty", dec),
      Field("price", dec),
      Field("disc", dec),
      Field("tax", dec),
      Field("d", PType.Float64),
      Field("n", PType.Int64)
    )
  )

  private def col(i: Int): ColRef = ColRef(i, in(i).ptype, in(i).name)

  private val (flag, status, qty, price, disc, tax, d, n) =
    (col(0), col(1), col(2), col(3), col(4), col(5), col(6), col(7))

  private val one = Const(java.math.BigDecimal("1.00"), PType.Decimal(16, 2))

  // TPC-H q1's projection: the columns, `price * (1 - disc)`, and forms computed as usual
  private val exprs: IndexedSeq[VExpr] = IndexedSeq(
    flag,
    status,
    qty,
    price,
    disc,
    tax,
    d,
    Arith(
      ArithOp.Mul,
      price,
      Arith(ArithOp.Sub, one, disc, PType.Decimal(16, 2)),
      PType.Decimal(31, 4)
    ),
    Arith(ArithOp.Mul, d, d, PType.Float64),
    // Int64 arithmetic may throw: computed over the kept rows only
    Arith(ArithOp.Add, n, n, PType.Int64),
    // Division is not computed over the source rows
    Arith(ArithOp.Div, price, disc, PType.Decimal(38, 6))
  )

  private val projected = Schema(exprs.zipWithIndex.map((e, i) => Field(s"c${i}", e.ptype)))

  private def pcol(i: Int): ColRef = ColRef(i, projected(i).ptype, projected(i).name)

  private def agg(name: String, arg: VExpr): AggCall = AggCall(
    name,
    Seq(arg),
    false,
    None,
    Aggregates.resultType(name, Seq(arg.ptype))
  )

  private val aggs = IndexedSeq(
    agg("sum", pcol(2)),
    agg("sum", pcol(3)),
    agg("sum", pcol(7)),
    // q1's `sum(disc_price * (1 + tax))`
    agg(
      "sum",
      Arith(
        ArithOp.Mul,
        pcol(7),
        Arith(ArithOp.Add, one, pcol(5), PType.Decimal(16, 2)),
        PType.Decimal(38, 6)
      )
    ),
    agg("count", pcol(2)),
    agg("sum", pcol(4)),
    agg("count", pcol(4)),
    agg("sum", pcol(6)),
    agg("sum", pcol(8)),
    agg("sum", pcol(9)),
    agg("sum", pcol(10)),
    AggCall("count_star", Seq.empty, false, None, PType.Int64)
  )

  private val flags    = Array("A", "N", "R")
  private val statuses = Array("F", "O")

  /**
    * Source batches whose rows a filter keeps at increasing positions (`kept` of them out of each
    * 10), and, with `nullRows`, a few at -1. `disc` has NULLs and the string keys carry dictionary
    * codes, as Parquet dictionary pages decode
    */
  private def input(rows: Int, batch: Int, kept: Int, nullRows: Boolean = false): Seq[Batch] =
    val rnd = scala.util.Random(7)
    (0 until rows)
      .grouped(batch)
      .map { r =>
        val m                               = r.size
        def decs(f: => Long): Array[Long]   = Array.fill(m)(f)
        def coded(dict: Array[String]): Vec =
          val codes = Array.fill(m)(rnd.nextInt(dict.length))
          StrVec(codes.map(dict(_)), null, codes, dict.length)
        val src = Batch(
          Array[Vec](
            coded(flags),
            coded(statuses),
            LongVec(dec, decs(rnd.nextInt(5000).toLong), null),
            LongVec(dec, decs(rnd.nextInt(10000000).toLong), null),
            LongVec(dec, decs(rnd.nextInt(11).toLong), Array.fill(m)(rnd.nextInt(9) == 0)),
            LongVec(dec, decs(rnd.nextInt(9).toLong), null),
            DoubleVec(Array.fill(m)(rnd.nextInt(1000) / 8.0), null),
            // Doubling these overflows a Long on rows the filter drops
            LongVec(PType.Int64, Array.fill(m)(Long.MaxValue), null)
          ),
          m
        )
        val keep = (0 until m).filter(_ => rnd.nextInt(10) < kept).toArray
        val idx  =
          if nullRows then
            keep.map(i =>
              if i % 50 == 0 then
                -1
              else
                i
            )
          else
            keep
        src.take(idx, idx.length)
      }
      .toSeq

  end input

  /**
    * The rows of `exprs` over the batches, as the kept rows' values: n doubled overflows, so it is
    * replaced by a small column first
    */
  private def withSmallN(batches: Seq[Batch]): Seq[Batch] = batches.map { b =>
    val m   = b.size
    val src = b.deferredSource(7)
    if src == null then
      b
    else
      // Replace the Int64 source by small values at the same positions
      val small = LongVec(PType.Int64, Array.tabulate(src.size)(_.toLong), null)
      val cols  = new Array[Vec](b.numColumns)
      val srcs  =
        Array.tabulate(b.numColumns)(i =>
          if i == 7 then
            small
          else
            b.deferredSource(i)
        )
      val sels = Array.tabulate(b.numColumns)(b.deferredPositions)
      Batch.withDeferred(cols, srcs, sels, m)
  }

  // q1's aggregates alone: every argument is deferred, so the fused loop reads them at the kept
  // positions
  private val q1Aggs = aggs.take(8) :+ aggs.last

  private def aggregate(
      batches: Seq[Batch],
      dense: Boolean,
      aggs: IndexedSeq[AggCall] = aggs
  ): Map[Seq[Any], Seq[Any]] =
    val keys      = IndexedSeq(pcol(0), pcol(1))
    val outSchema = Schema(
      keys.zipWithIndex.map((c, i) => Field(s"k${i}", c.ptype)) ++
        aggs.indices.map(i => Field(s"a${i}", aggs(i).ptype))
    )
    val project = ProjectOp(BatchesOp(in, batches), exprs, projected).withDenseSelected(dense)
    val op      = HashAggOp(project, keys, aggs, outSchema).withDenseSelected(dense)
    val out     =
      try op.drain()
      finally op.close()
    out
      .flatMap(r => (0 until r.size).map(i => r.columns.map(_.get(i)).toSeq))
      .map(r => r.take(2) -> r.drop(2))
      .toMap

  private def same(
      batches: => Seq[Batch],
      aggs: IndexedSeq[AggCall] = aggs
  ): Map[Seq[Any], Seq[Any]] =
    val dense = aggregate(withSmallN(batches), dense = true, aggs)
    dense shouldBe aggregate(withSmallN(batches), dense = false, aggs)
    dense

  test("should aggregate a filter's kept rows over few groups as over gathered ones") {
    val groups = same(input(20000, 2048, kept = 9))
    groups.size shouldBe 6
  }

  test("should fold q1's aggregates at a filter's kept positions as over gathered ones") {
    same(input(20000, 2048, kept = 9), q1Aggs).size shouldBe 6
    same(input(20000, 2048, kept = 9, nullRows = true), q1Aggs).size shouldBe 7
  }

  test("should aggregate a filter's kept rows over many groups as over gathered ones") {
    // Small batches: fewer than 16 rows per group, so the accumulators fold one by one
    same(input(3000, 64, kept = 9)).size shouldBe 6
  }

  test("should evaluate as usual when the filter keeps few rows") {
    same(input(20000, 2048, kept = 3)).size shouldBe 6
  }

  test("should group rows at position -1 under NULL keys") {
    val groups = same(input(20000, 2048, kept = 9, nullRows = true))
    groups.keys.exists(_.head == null) shouldBe true
  }

  test("should pass deferred columns through and compute total expressions over source rows") {
    val b   = withSmallN(input(2048, 2048, kept = 9)).head
    val out = DenseSelectedRows.project(exprs, b)
    out.size shouldBe b.size
    // Columns and `price * (1 - disc)` / `d * d` stay deferred; the others are computed
    Seq(0, 1, 2, 3, 4, 5, 6, 7, 8).foreach(i => (out.deferredSource(i) != null) shouldBe true)
    (out.deferredSource(7).size == out.deferredSource(3).size) shouldBe true
    Seq(9, 10).foreach(i => (out.deferredSource(i) == null) shouldBe true)
    // The values equal those computed over the gathered rows
    val gathered = Batch(
      exprs.map(_.eval(withSmallN(input(2048, 2048, kept = 9)).head)).toArray,
      b.size
    )
    (0 until exprs.size).foreach { c =>
      (0 until b.size).foreach(i => out.column(c).get(i) shouldBe gathered.column(c).get(i))
    }
  }

  test("should not compute Int64 arithmetic over the rows a filter drops") {
    // n doubled overflows on every row: the kept rows of a filter on n < 0 are none
    val src = Batch(
      Array[Vec](LongVec(PType.Int64, Array(Long.MaxValue, Long.MaxValue, 1L), null)),
      3
    ).take(Array(2), 1)
    val m = ColRef(0, PType.Int64, "n")
    val p = DenseSelectedRows.project(IndexedSeq(Arith(ArithOp.Add, m, m, PType.Int64)), src)
    p.column(0).get(0) shouldBe 2L
  }

  test("should not convert doubles of dropped rows to decimals") {
    // A NaN has no BigDecimal value: only the kept row converts
    val src = Batch(
      Array[Vec](DoubleVec(Array(Double.NaN, 1.5), null), LongVec(dec, Array(100L, 200L), null)),
      2
    ).take(Array(1), 1)
    val e = Arith(
      ArithOp.Add,
      ColRef(0, PType.Float64, "d"),
      ColRef(1, dec, "x"),
      PType.Decimal(18, 2)
    )
    DenseSelectedRows.eval(e, src) shouldBe null
    DenseSelectedRows.project(IndexedSeq(e), src).column(0).get(0) shouldBe
      java.math.BigDecimal("3.50")
  }

end DenseSelectedRowsTest
