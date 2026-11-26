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

class SelectedInputsTest extends UniTest:
  private val dec  = PType.Decimal(7, 2)
  private val wide = PType.Decimal(18, 0)
  private val in   = Schema(
    IndexedSeq(
      Field("k", PType.Int64),
      Field("j", PType.Int64),
      Field("a", PType.Int64),
      Field("b", PType.Int64),
      Field("x", dec),
      Field("y", dec),
      Field("z", dec),
      Field("d", PType.Float64),
      Field("e", PType.Float64),
      Field("w", wide)
    )
  )

  private def col(i: Int): ColRef = ColRef(i, in(i).ptype, in(i).name)

  private val (k, j, a, b, x, y, z, d, e, w) =
    (col(0), col(1), col(2), col(3), col(4), col(5), col(6), col(7), col(8), col(9))

  private def sum(arg: VExpr): AggCall = AggCall(
    "sum",
    Seq(arg),
    false,
    None,
    Aggregates.resultType("sum", Seq(arg.ptype))
  )

  private def call(name: String, arg: VExpr): AggCall = AggCall(
    name,
    Seq(arg),
    false,
    None,
    Aggregates.resultType(name, Seq(arg.ptype))
  )

  private def decimalOf(l: VExpr, r: VExpr): PType =
    (l.ptype, r.ptype) match
      case (PType.Decimal(p1, s1), PType.Decimal(p2, s2)) =>
        PType.Decimal(math.min(38, math.max(p1 - s1, p2 - s2) + math.max(s1, s2) + 1), s1)
      case (t, _) =>
        t

  private def add(l: VExpr, r: VExpr): Arith = Arith(ArithOp.Add, l, r, decimalOf(l, r))
  private def sub(l: VExpr, r: VExpr): Arith = Arith(ArithOp.Sub, l, r, decimalOf(l, r))

  private val aggs = IndexedSeq(
    sum(a),
    sum(d),
    sum(x),
    call("min", a),
    call("max", a),
    call("min", d),
    call("max", d),
    call("count", x),
    call("count", d),
    // q04's shape: ((x - y) - z) + x
    sum(add(sub(sub(x, y), z), x)),
    sum(sub(a, b)),
    sum(sub(d, add(e, d))),
    // Overflows a Long: computed as usual
    sum(add(w, w)),
    // DISTINCT reads gathered values
    AggCall("sum", Seq(w), true, None, Aggregates.resultType("sum", Seq(wide)))
  )

  /**
    * Source batches taken at random positions, as a join probe's output: about a tenth of the rows
    * are at position -1 (an outer join's NULL rows). `y` and `d` have NULLs; key `j` grows from
    * batch to batch so that its packing widens
    */
  private def input(n: Int, batch: Int, groups: Int): Seq[Batch] =
    val rnd = scala.util.Random(11)
    (0 until n)
      .grouped(batch)
      .zipWithIndex
      .map { (rows, bi) =>
        val m                               = rows.size
        def longs(f: => Long): Array[Long]  = Array.fill(m)(f)
        def nullsAt(p: Int): Array[Boolean] = Array.fill(m)(rnd.nextInt(p) == 0)
        val src                             = Batch(
          Array(
            LongVec(PType.Int64, longs(rnd.nextInt(groups).toLong), null),
            LongVec(PType.Int64, longs(rnd.nextInt(3).toLong + bi * 1000L), null),
            LongVec(PType.Int64, longs(rnd.nextLong() % 1000000L), null),
            LongVec(PType.Int64, longs(rnd.nextLong() % 1000000L), null),
            LongVec(dec, longs(rnd.nextLong() % 10000000L), null),
            LongVec(dec, longs(rnd.nextLong() % 10000000L), nullsAt(5)),
            LongVec(dec, longs(rnd.nextLong() % 10000000L), null),
            DoubleVec(Array.fill(m)(rnd.nextInt(1000) / 8.0), nullsAt(7)),
            DoubleVec(Array.fill(m)(rnd.nextInt(1000) / 4.0), null),
            LongVec(wide, longs(Long.MaxValue - rnd.nextLong(1000L)), null)
          ),
          m
        )
        val taken = (m * 2) / 3
        val idx   =
          Array.fill(taken)(
            if rnd.nextInt(10) == 0 then
              -1
            else
              rnd.nextInt(m)
          )
        src.take(idx, taken)
      }
      .toSeq

  end input

  private def aggregate(
      batches: Seq[Batch],
      selected: Boolean,
      keys: IndexedSeq[VExpr] = IndexedSeq(k, j)
  ): Map[Seq[Any], Seq[Any]] =
    val outSchema = Schema(
      keys.zipWithIndex.map((c, i) => Field(s"k${i}", c.ptype)) ++
        aggs.indices.map(i => Field(s"a${i}", aggs(i).ptype))
    )
    val op  = HashAggOp(BatchesOp(in, batches), keys, aggs, outSchema).withSelectedInputs(selected)
    val out =
      try op.drain()
      finally op.close()
    out
      .flatMap(r => (0 until r.size).map(i => r.columns.map(_.get(i)).toSeq))
      .map(r => r.take(keys.size) -> r.drop(keys.size))
      .toMap

  test("should aggregate deferred columns in place as over gathered ones") {
    val inPlace  = aggregate(input(20000, 1024, groups = 3000), selected = true)
    val gathered = aggregate(input(20000, 1024, groups = 3000), selected = false)
    inPlace shouldBe gathered
    // Rows at position -1 group under NULL keys
    inPlace.keys.exists(_.head == null) shouldBe true
  }

  test("should aggregate deferred columns in place over few groups, with fused accumulators") {
    aggregate(input(20000, 1024, groups = 4), selected = true) shouldBe
      aggregate(input(20000, 1024, groups = 4), selected = false)
  }

  test("should aggregate deferred columns in place without group keys") {
    aggregate(input(5000, 512, groups = 10), selected = true, keys = IndexedSeq.empty) shouldBe
      aggregate(input(5000, 512, groups = 10), selected = false, keys = IndexedSeq.empty)
  }

  test("should read the group keys and the aggregated columns without gathering them") {
    val batches = input(4096, 1024, groups = 3000)
    aggregate(batches, selected = true)
    val untouched = Seq(0, 1, 2, 3, 4, 5, 6, 7, 8)
    batches.foreach { bt =>
      untouched.foreach(i => (bt.deferredSource(i) != null) shouldBe true)
      // Gathered for the decimal overflow fallback and the DISTINCT sum
      (bt.deferredSource(9) == null) shouldBe true
    }
    val off = input(4096, 1024, groups = 3000)
    aggregate(off, selected = false)
    off.foreach(bt => (bt.deferredSource(0) == null) shouldBe true)
  }

  test("should throw on an Int64 overflow in place as the gathered evaluation does") {
    val src = Batch(
      Array[Vec](
        LongVec(PType.Int64, Array(1L, 2L), null),
        LongVec(PType.Int64, Array(Long.MaxValue, 5L), null)
      ),
      2
    ).take(Array(0, 1), 2)
    val s2    = Schema(IndexedSeq(Field("k", PType.Int64), Field("v", PType.Int64)))
    val v     = ColRef(1, PType.Int64, "v")
    val calls = IndexedSeq(sum(Arith(ArithOp.Add, v, v, PType.Int64)))
    val op    = HashAggOp(
      BatchesOp(s2, Seq(src)),
      IndexedSeq(ColRef(0, PType.Int64, "k")),
      calls,
      Schema(IndexedSeq(Field("k", PType.Int64), Field("s", PType.Int64)))
    )
    intercept[ArithmeticException] {
      try op.drain()
      finally op.close()
    }
  }

end SelectedInputsTest
