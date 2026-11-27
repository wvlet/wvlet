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
package wvlet.lang.engine.runtime

import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

class CommonSubexpressionsTest extends UniTest:
  private val dec   = PType.Decimal(15, 2)
  private val price = ColRef(0, dec, "price")
  private val disc  = ColRef(1, dec, "disc")
  private val tax   = ColRef(2, dec, "tax")
  private val n     = ColRef(3, PType.Int64, "n")

  // A new operator for each read: an operator is drained once
  private def input: Operator =
    val schema = Schema(
      IndexedSeq(
        Field("price", dec),
        Field("disc", dec),
        Field("tax", dec),
        Field("n", PType.Int64)
      )
    )
    val batch = Batch(
      Array(
        LongVec(dec, Array(10000L, 2050L, 99L), null),
        LongVec(dec, Array(5L, 10L, 0L), null),
        LongVec(dec, Array(8L, 2L, 1L), null),
        LongVec(PType.Int64, Array(0L, 2L, 5L), null)
      ),
      3
    )
    BatchesOp(schema, Seq(batch))

  private def one: VExpr = Const(1, PType.Int32)

  private def sub(l: VExpr, r: VExpr): VExpr = Arith(ArithOp.Sub, l, r, PType.Decimal(16, 2))

  private def mul(l: VExpr, r: VExpr, t: PType): VExpr = Arith(ArithOp.Mul, l, r, t)

  private def rows(op: Operator, exprs: IndexedSeq[VExpr]): Seq[Seq[Any]] =
    val p = ProjectOp(op, exprs, Schema(exprs.indices.map(i => Field(s"c${i}", exprs(i).ptype))))
    p.drain().flatMap(b => (0 until b.size).map(i => b.columns.toSeq.map(_.get(i))))

  /** The projections `share` put below the expressions */
  private def projections(op: Operator): Int =
    op match
      case p: ProjectOp =>
        1 + projections(p.child)
      case _ =>
        0

  test("should compute a repeated subexpression once, as TPC-H q1 does") {
    val discPrice = mul(price, sub(one, disc), PType.Decimal(31, 4))
    val charge    = mul(
      discPrice,
      Arith(ArithOp.Add, one, tax, PType.Decimal(16, 2)),
      PType.Decimal(46, 6)
    )
    val exprs    = IndexedSeq(discPrice, charge)
    val (op, es) = CommonSubexpressions.share(input, exprs)
    op.schema.size shouldBe 5
    es(0) shouldBe ColRef(4, discPrice.ptype, "__cse_0")
    es(1).sig shouldContain "#4"
    rows(op, es) shouldBe rows(input, exprs)
  }

  test("should share the repeated parts of a shared subexpression in an earlier projection") {
    val d        = sub(one, disc)
    val a        = mul(price, d, PType.Decimal(31, 4))
    val exprs    = IndexedSeq(a, a, d)
    val (op, es) = CommonSubexpressions.share(input, exprs)
    // `1 - disc`, then `price * (1 - disc)` over it
    projections(op) shouldBe 2
    op.schema.size shouldBe 6
    rows(op, es) shouldBe rows(input, exprs)
  }

  test("should not share a subexpression evaluated only in later CASE branches") {
    // price / n is evaluated only for the rows where n != 0
    val div   = Arith(ArithOp.Div, price, n, PType.Float64)
    val guard = Compare(CmpOp.Ne, n, Const(0L, PType.Int64))
    val c1    = CaseWhen(Seq(guard -> div), None, PType.Float64)
    val c2    = CaseWhen(
      Seq(guard -> mul(div, Const(2.0, PType.Float64), PType.Float64)),
      None,
      PType.Float64
    )
    val (op, es) = CommonSubexpressions.share(input, IndexedSeq(c1, c2))
    // The guard is evaluated for every row by both, so it is shared; the division isn't
    op.schema.size shouldBe 5
    op.schema(4).ptype shouldBe PType.Bool
    rows(op, es) shouldBe rows(input, IndexedSeq(c1, c2))
  }

  test("should share a subexpression in a CASE branch when it is also evaluated for every row") {
    val d        = mul(price, disc, PType.Decimal(30, 4))
    val guard    = Compare(CmpOp.Gt, n, Const(1L, PType.Int64))
    val c        = CaseWhen(Seq(guard -> d), Some(Const(0, PType.Int32)), PType.Decimal(30, 4))
    val (op, es) = CommonSubexpressions.share(input, IndexedSeq(c, d))
    op.schema.size shouldBe 5
    es(1) shouldBe ColRef(4, d.ptype, "__cse_0")
    rows(op, es) shouldBe rows(input, IndexedSeq(c, d))
  }

  test("should not share volatile functions") {
    def rnd: VExpr = RowFn(
      "random",
      Nil,
      PType.Float64,
      _ => scala.util.Random.nextDouble(),
      nullSafe = true
    )
    val r        = rnd
    val e        = Arith(ArithOp.Add, r, Const(1.0, PType.Float64), PType.Float64)
    val in       = input
    val (op, es) = CommonSubexpressions.share(in, IndexedSeq(e, e))
    (op eq in) shouldBe true
    es shouldBe IndexedSeq(e, e)
  }

  test("should share the same subexpressions of an OR of ANDs with hashed keys") {
    // An OR of ANDs with repeated conjuncts and a repeated sum, as TPC-DS q41's filter
    def cmp(op: CmpOp, c: ColRef, v: Long): VExpr = Compare(op, c, Const(v, PType.Int64))
    val total    = Arith(ArithOp.Add, n, Const(1L, PType.Int64), PType.Int64)
    val branches = (0 until 8).map { i =>
      AndExpr(
        AndExpr(cmp(CmpOp.Gt, n, i % 3), Compare(CmpOp.Lt, total, Const(i.toLong, PType.Int64))),
        cmp(CmpOp.Ne, n, i % 2)
      ): VExpr
    }
    val cond         = branches.reduce((a, b) => OrExpr(a, b))
    val exprs        = IndexedSeq(cond, total)
    val (plain, pes) = CommonSubexpressions.share(input, exprs)
    val (op, es)     = CommonSubexpressions.share(input, exprs, hashedKeys = true)
    // The repeated sum is shared
    (projections(plain) > 0) shouldBe true
    es shouldBe pes
    op.schema shouldBe plain.schema
    projections(op) shouldBe projections(plain)
    rows(op, es) shouldBe rows(input, exprs)
  }

  test("should leave expressions without repeats as they are") {
    val exprs = IndexedSeq(
      sub(one, disc),
      price,
      Arith(ArithOp.Add, one, tax, PType.Decimal(16, 2))
    )
    val in       = input
    val (op, es) = CommonSubexpressions.share(in, exprs)
    (op eq in) shouldBe true
    es shouldBe exprs
  }

  test("should fold constant subexpressions") {
    val e = Compare(
      CmpOp.Le,
      n,
      Arith(ArithOp.Add, Const(1, PType.Int32), Const(10, PType.Int32), PType.Int32)
    )
    FoldConstants(e) shouldBe Compare(CmpOp.Le, n, Const(11, PType.Int32))
  }

  test("should pre-scale an integral constant added to a decimal") {
    val e = sub(one, disc)
    val f = FoldConstants(e)
    f shouldBe
      Arith(
        ArithOp.Sub,
        Const(java.math.BigDecimal("1.00"), PType.Decimal(16, 2)),
        disc,
        PType.Decimal(16, 2)
      )
    rows(input, IndexedSeq(f)) shouldBe rows(input, IndexedSeq(e))
  }

  test("should keep a constant expression that fails to evaluate") {
    val e = RowFn(
      "boom",
      Seq(Const(1, PType.Int32)),
      PType.Int32,
      _ => throw RuntimeException("boom")
    )
    FoldConstants(e) shouldBe e
  }

end CommonSubexpressionsTest
