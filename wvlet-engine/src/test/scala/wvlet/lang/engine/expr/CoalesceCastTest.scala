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

class CoalesceCastTest extends UniTest:
  private val dec2 = PType.decimal(15, 2)
  private val dec4 = PType.decimal(18, 4)

  private def rows(v: Vec): Seq[Any] = (0 until v.size).map(v.get)

  private def eval(e: VExpr, cols: Vec*): Vec = e.eval(Batch(cols.toArray, cols.head.size))

  private def nulls(bs: Boolean*): Array[Boolean] = bs.toArray

  test("should coalesce decimals on unscaled longs, casting integer arguments") {
    val a = LongVec(dec2, Array(150L, 0L, 0L, 425L), nulls(false, true, true, false))
    val b = IntVec(PType.Int32, Array(7, 8, 0, 9), nulls(false, false, true, false))
    val e = Coalesce(
      Seq(ColRef(0, dec2, "a"), ColRef(1, PType.Int32, "b"), Const(0, PType.Int32)),
      dec2
    )
    val v = eval(e, a, b)
    v shouldMatch {
      case l: LongVec if l.ptype == dec2 && l.nulls == null =>
    }
    rows(v) shouldBe
      Seq(JBigDecimal("1.50"), JBigDecimal("8.00"), JBigDecimal("0.00"), JBigDecimal("4.25"))
  }

  test("should keep NULL when every argument is NULL") {
    val a = LongVec(PType.Int64, Array(1L, 0L, 3L), nulls(false, true, false))
    val b = LongVec(PType.Int64, Array(0L, 0L, 0L), nulls(true, true, true))
    val e = Coalesce(
      Seq(ColRef(0, PType.Int64, "a"), Const(null, PType.Null), ColRef(1, PType.Int64, "b")),
      PType.Int64
    )
    rows(eval(e, a, b)) shouldBe Seq(1L, null, 3L)

    val allNull = Coalesce(Seq(Const(null, PType.Null), Const(null, PType.Null)), PType.Int64)
    rows(eval(allNull, a)) shouldBe Seq(null, null, null)
  }

  test("should return the first argument when it has no NULLs") {
    val a = StrVec(Array("x", "y"), null)
    val e = Coalesce(Seq(ColRef(0, PType.Str, "a"), Const("z", PType.Str)), PType.Str)
    eval(e, a) shouldBeTheSameInstanceAs a
  }

  test("should coalesce strings, doubles, and wide decimals") {
    val s  = StrVec(Array(null, "b"), nulls(true, false))
    val es = Coalesce(Seq(ColRef(0, PType.Str, "s"), Const("z", PType.Str)), PType.Str)
    rows(eval(es, s)) shouldBe Seq("z", "b")

    val d  = DoubleVec(Array(0.0, 2.5), nulls(true, false))
    val ed = Coalesce(Seq(ColRef(0, PType.Float64, "d"), Const(1.5, PType.Float64)), PType.Float64)
    rows(eval(ed, d)) shouldBe Seq(1.5, 2.5)

    val wide = PType.decimal(38, 0)
    val big  = JBigDecimal("123456789012345678901234567890")
    val w    = ObjVec(wide, Array[AnyRef](null, big), nulls(true, false))
    val ew   = Coalesce(Seq(ColRef(0, wide, "w"), Const(1, PType.Int32)), wide)
    rows(eval(ew, w)) shouldBe Seq(JBigDecimal("1"), big)
  }

  test("should lower a decimal scale on unscaled longs, rounding half away from zero") {
    // 1.2345, 1.2350, -1.2350, -1.2349, 0.0050, NULL
    val v = LongVec(
      dec4,
      Array(12345L, 12350L, -12350L, -12349L, 50L, 0L),
      nulls(false, false, false, false, false, true)
    )
    val c = Kernels.cast(v, dec2, tryCast = false)
    c shouldMatch {
      case l: LongVec if l.ptype == dec2 =>
    }
    rows(c) shouldBe
      Seq(
        JBigDecimal("1.23"),
        JBigDecimal("1.24"),
        JBigDecimal("-1.24"),
        JBigDecimal("-1.23"),
        JBigDecimal("0.01"),
        null
      )
    // Same values as the BigDecimal path
    (0 until 5).foreach { i =>
      c.get(i) shouldBe ValueOps.cast(v.get(i), dec4, dec2)
    }
    // To scale 0
    rows(Kernels.cast(v, PType.decimal(18, 0), tryCast = false)).take(5) shouldBe
      Seq(
        JBigDecimal("1"),
        JBigDecimal("1"),
        JBigDecimal("-1"),
        JBigDecimal("-1"),
        JBigDecimal("0")
      )
  }

  test("should cast decimals to doubles like BigDecimal.doubleValue") {
    val v = LongVec(dec2, Array(150L, -1L, 123456789L, 0L), nulls(false, false, false, true))
    val c = Kernels.cast(v, PType.Float64, tryCast = false)
    c shouldMatch { case _: DoubleVec =>
    }
    rows(c) shouldBe Seq(1.5, -0.01, 1234567.89, null)

    // Beyond 2^53 the BigDecimal path keeps the correctly rounded value
    val large = LongVec(dec2, Array(Long.MaxValue), null)
    Kernels.cast(large, PType.Float64, tryCast = false).get(0) shouldBe
      JBigDecimal.valueOf(Long.MaxValue, 2).doubleValue()
  }

  test("should cast an untyped NULL vector to a NULL vector of the target type") {
    val n = Vec.nullVec(PType.Null, 3)
    val c = Kernels.cast(n, dec2, tryCast = false)
    c shouldMatch {
      case l: LongVec if l.ptype == dec2 =>
    }
    rows(c) shouldBe Seq(null, null, null)
  }

  test("should fill decimal constants as unscaled longs") {
    val c = Vec.constant(dec2, JBigDecimal("1.5"), 3)
    c shouldMatch {
      case l: LongVec if l.ptype == dec2 =>
    }
    rows(c) shouldBe Seq.fill(3)(JBigDecimal("1.50"))

    val wide = PType.decimal(38, 0)
    val big  = JBigDecimal("123456789012345678901234567890")
    rows(Vec.constant(wide, big, 2)) shouldBe Seq(big, big)
  }

  test("should not evaluate a NULL literal CASE WHEN branch") {
    val sel   = IntVec(PType.Int32, Array(1, 2, 1), null)
    val price = LongVec(dec2, Array(100L, 200L, 300L), null)
    val e     = CaseWhen(
      Seq(
        Compare(CmpOp.Eq, ColRef(0, PType.Int32, "k"), Const(1, PType.Int32)) ->
          ColRef(1, dec2, "p")
      ),
      Some(Const(null, PType.Null)),
      dec2
    )
    val v = eval(e, sel, price)
    v shouldMatch {
      case l: LongVec if l.ptype == dec2 =>
    }
    rows(v) shouldBe Seq(JBigDecimal("1.00"), null, JBigDecimal("3.00"))
  }

  test("should divide decimals on doubles like BigDecimal.doubleValue") {
    val x = LongVec(dec2, Array(100L, 250L, 0L, 300L), nulls(false, false, true, false))
    val y = LongVec(dec2, Array(300L, 50L, 100L, 0L), null)
    val q = Kernels.arith(ArithOp.Div, x, y, PType.Float64)
    rows(q) shouldBe Seq(1.0 / 3.0, 5.0, null, null)
    // Above 2^53 the unscaled values go through BigDecimal
    val large = LongVec(dec2, Array(Long.MaxValue), null)
    val one   = LongVec(dec2, Array(100L), null)
    Kernels.arith(ArithOp.Div, large, one, PType.Float64).get(0) shouldBe
      JBigDecimal.valueOf(Long.MaxValue, 2).doubleValue()
  }

end CoalesceCastTest
