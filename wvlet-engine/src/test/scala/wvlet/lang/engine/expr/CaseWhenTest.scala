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
package wvlet.lang.engine.expr

import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

import java.math.BigDecimal as JBigDecimal

class CaseWhenTest extends UniTest:
  private val dec2 = PType.decimal(15, 2)

  private def rows(v: Vec): Seq[Any] = (0 until v.size).map(v.get)

  private def eval(e: VExpr, cols: Vec*): Vec = e.eval(Batch(cols.toArray, cols.head.size))

  // Column 0: the branch selector
  private val sel    = IntVec(PType.Int32, Array(1, 2, 3, 1), Array(false, false, false, true))
  private val selRef = ColRef(0, PType.Int32, "k")
  private def is(k: Int): VExpr = Compare(CmpOp.Eq, selRef, Const(k, PType.Int32))

  test("should fill typed values from the chosen branch, with a missing ELSE as NULL") {
    val e = CaseWhen(
      Seq(is(1) -> Const(10L, PType.Int64), is(2) -> Const(20L, PType.Int64)),
      None,
      PType.Int64
    )
    val v = eval(e, sel)
    v shouldMatch { case _: LongVec =>
    }
    // the NULL selector takes the (missing) ELSE branch
    rows(v) shouldBe Seq(10L, 20L, null, null)
  }

  test("should keep decimals of different scales on unscaled longs") {
    val price = LongVec(dec2, Array(150L, 250L, 350L, 450L), Array(false, true, false, false))
    val e     = CaseWhen(
      Seq(is(1) -> ColRef(1, dec2, "price"), is(2) -> ColRef(1, dec2, "price")),
      Some(Const(JBigDecimal("0.5"), PType.decimal(2, 1))),
      dec2
    )
    val v = eval(e, sel, price)
    v shouldMatch {
      case l: LongVec if l.ptype == dec2 =>
    }
    rows(v) shouldBe Seq(JBigDecimal("1.50"), null, JBigDecimal("0.50"), JBigDecimal("0.50"))
  }

  test("should take strings, doubles, and booleans from typed arrays") {
    val s  = StrVec(Array("a", null, "c", "d"), Array(false, true, false, false))
    val es = CaseWhen(
      Seq(is(1) -> Const("one", PType.Str)),
      Some(ColRef(1, PType.Str, "s")),
      PType.Str
    )
    rows(eval(es, sel, s)) shouldBe Seq("one", null, "c", "d")

    val d  = DoubleVec(Array(0.5, 1.5, 2.5, 3.5), null)
    val ed = CaseWhen(
      Seq(is(3) -> ColRef(1, PType.Float64, "d")),
      Some(Const(-1.0, PType.Float64)),
      PType.Float64
    )
    rows(eval(ed, sel, d)) shouldBe Seq(-1.0, -1.0, 2.5, -1.0)

    val eb = CaseWhen(
      Seq(is(2) -> Const(true, PType.Bool)),
      Some(Const(false, PType.Bool)),
      PType.Bool
    )
    rows(eval(eb, sel)) shouldBe Seq(false, true, false, false)
  }

  test("should fall back to the row path for wide decimals") {
    val wide = PType.decimal(38, 2)
    val big  = JBigDecimal("12345678901234567890123.45")
    val e = CaseWhen(Seq(is(1) -> Const(big, wide)), Some(Const(JBigDecimal("1.00"), wide)), wide)
    rows(eval(e, sel)) shouldBe
      Seq(big, JBigDecimal("1.00"), JBigDecimal("1.00"), JBigDecimal("1.00"))
  }

  test("should not evaluate a branch that no row takes") {
    var evaluated = 0
    val probe     = RowFn(
      "probe",
      Seq(selRef),
      PType.Int64,
      _ =>
        evaluated += 1
        0L
    )
    val e = CaseWhen(Seq(is(9) -> probe), Some(Const(1L, PType.Int64)), PType.Int64)
    rows(eval(e, sel)) shouldBe Seq(1L, 1L, 1L, 1L)
    evaluated shouldBe 0
  }

  test("should return the branch vector itself when every row takes it") {
    val d = DoubleVec(Array(0.5, 1.5, 2.5, 3.5), null)
    val e = CaseWhen(
      Seq(is(9) -> Const(1.0, PType.Float64)),
      Some(ColRef(1, PType.Float64, "d")),
      PType.Float64
    )
    eval(e, sel, d) shouldBeTheSameInstanceAs d
  }

  test("should return all NULLs when every taken branch is NULL") {
    val e = CaseWhen(Seq(is(5) -> Const(1, PType.Int32)), None, PType.Int32)
    rows(eval(e, sel)) shouldBe Seq(null, null, null, null)
  }

  test("should mask the THEN vector of a single WHEN without ELSE") {
    val price = LongVec(dec2, Array(150L, 250L, 350L, 450L), Array(false, true, false, false))
    val then1 = ColRef(1, dec2, "price")
    Seq(None, Some(Const(null, dec2))).foreach { elseExpr =>
      val plain  = CaseWhen(Seq(is(1) -> then1), elseExpr, dec2)
      val masked = plain.copy(maskNulls = true)
      val v      = eval(masked, sel, price)
      v shouldMatch { case l: LongVec =>
      }
      // A false condition, a NULL condition, and a NULL value each yield NULL
      rows(v) shouldBe Seq(JBigDecimal("1.50"), null, null, null)
      rows(v) shouldBe rows(eval(plain, sel, price))
    }
  }

  test("should mask strings, doubles, ints and booleans, and return all-taken or no-taken rows") {
    val all  = BoolVec(Array(true, true, true, true), null)
    val none = BoolVec(Array(false, false, false, false), null)
    val strs = StrVec(Array("a", "b", "c", null), Array(false, false, false, true))
    val dbls = DoubleVec(Array(1.0, 2.0, 3.0, 4.0), null)
    val ints = IntVec(PType.Int32, Array(1, 2, 3, 4), null)
    val bls  = BoolVec(Array(true, false, true, false), null)
    val cols = Seq(sel, strs, dbls, ints, bls, all, none)
    def masked(cond: VExpr, col: Int, t: PType): Seq[Any] =
      val c = CaseWhen(Seq(cond -> ColRef(col, t, "v")), None, t)
      val m = rows(eval(c.copy(maskNulls = true), cols*))
      m shouldBe rows(eval(c, cols*))
      m
    masked(is(2), 1, PType.Str) shouldBe Seq(null, "b", null, null)
    masked(is(3), 2, PType.Float64) shouldBe Seq(null, null, 3.0, null)
    masked(is(1), 3, PType.Int32) shouldBe Seq(1, null, null, null)
    masked(is(1), 4, PType.Bool) shouldBe Seq(true, null, null, null)
    masked(ColRef(5, PType.Bool, "all"), 3, PType.Int32) shouldBe Seq(1, 2, 3, 4)
    masked(ColRef(6, PType.Bool, "none"), 3, PType.Int32) shouldBe Seq(null, null, null, null)
  }

  test("should mask wide decimals through the row path, evaluating the THEN value once") {
    val wide      = PType.decimal(38, 2)
    val big       = JBigDecimal("12345678901234567890123.45")
    var evaluated = 0
    val probe     = RowFn(
      "probe",
      Seq(selRef),
      wide,
      _ =>
        evaluated += 1
        big
    )
    val c = CaseWhen(Seq(is(1) -> probe), None, wide, maskNulls = true)
    rows(eval(c, sel)) shouldBe Seq(big, null, null, null)
    val maskedCalls = evaluated
    evaluated = 0
    rows(eval(c.copy(maskNulls = false), sel)) shouldBe Seq(big, null, null, null)
    maskedCalls shouldBe evaluated
  }

end CaseWhenTest

class LogicalKernelsTest extends UniTest:
  // Every pair of TRUE, FALSE, NULL, with `true` left in the value slot of NULL rows
  private val left = BoolVec(
    Array(true, true, true, false, false, false, true, true, true),
    Array(false, false, false, false, false, false, true, true, true)
  )

  private val right = BoolVec(
    Array(true, false, true, true, false, true, true, false, true),
    Array(false, false, true, false, false, true, false, false, true)
  )

  private def rows(v: Vec): Seq[Any] = (0 until v.size).map(v.get)

  private def ref(v: Vec): Seq[Any] = rows(v)

  private def expectedAnd(a: Any, b: Any): Any =
    if a == false || b == false then
      false
    else if a == null || b == null then
      null
    else
      true

  private def expectedOr(a: Any, b: Any): Any =
    if a == true || b == true then
      true
    else if a == null || b == null then
      null
    else
      false

  test("should follow three-valued logic when both sides have nulls") {
    rows(Kernels.and(left, right)) shouldBe ref(left).zip(ref(right)).map(expectedAnd)
    rows(Kernels.or(left, right)) shouldBe ref(left).zip(ref(right)).map(expectedOr)
  }

  test("should follow three-valued logic when one side has nulls") {
    val noNulls = BoolVec(Array(true, false, true, false, true, false, true, false, true), null)
    for (a, b) <- Seq(left -> noNulls, noNulls -> left) do
      rows(Kernels.and(a, b)) shouldBe ref(a).zip(ref(b)).map(expectedAnd)
      rows(Kernels.or(a, b)) shouldBe ref(a).zip(ref(b)).map(expectedOr)
  }

  test("should reuse a null array both sides share") {
    val nulls = Array(false, true, false, true)
    val a     = BoolVec(Array(true, true, false, false), nulls)
    val b     = BoolVec(Array(true, false, true, true), nulls)
    val and   = Kernels.and(a, b)
    and.nulls shouldBeTheSameInstanceAs nulls
    rows(and) shouldBe Seq(true, null, false, null)
    val or = Kernels.or(a, b)
    or.nulls shouldBeTheSameInstanceAs nulls
    rows(or) shouldBe Seq(true, null, true, null)
  }

  test("should drop the null array when no result row is NULL") {
    val a = BoolVec(Array(false, false), Array(false, false))
    val b = BoolVec(Array(true, true), Array(true, false))
    Kernels.and(a, b).nulls shouldBe null
  }

  test("should share the column's null array for BETWEEN with literal bounds") {
    val x = LongVec(PType.Int64, Array(1L, 5L, 9L, 0L), Array(false, false, false, true))
    val c = ColRef(0, PType.Int64, "x")
    val e = AndExpr(
      Compare(CmpOp.Ge, c, Const(2L, PType.Int64)),
      Compare(CmpOp.Le, c, Const(8L, PType.Int64))
    )
    val v = e.eval(Batch(Array[Vec](x), 4))
    v.nulls shouldBeTheSameInstanceAs x.nulls
    rows(v) shouldBe Seq(false, true, false, null)
  }

end LogicalKernelsTest
