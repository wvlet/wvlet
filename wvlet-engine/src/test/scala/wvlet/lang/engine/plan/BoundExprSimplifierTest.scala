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
package wvlet.lang.engine.plan

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

import java.math.BigDecimal as JBigDecimal

class BoundExprSimplifierTest extends UniTest:
  import BoundExprSimplifier.*

  private val i32   = ColRef(0, PType.Int32, "i")
  private val dec72 = ColRef(1, PType.Decimal(7, 2), "price")
  private val flag  = ColRef(2, PType.Bool, "flag")

  private def int(v: Int): Const                      = Const(v, PType.Int32)
  private def dec(v: String, t: PType.Decimal): Const = Const(JBigDecimal(v), t)
  private val yes                                     = Const(true, PType.Bool)
  private val no                                      = Const(false, PType.Bool)

  test("should fold arithmetic over literals") {
    fold(Arith(ArithOp.Add, int(1200), int(11), PType.Int32)) shouldBe int(1211)
  }

  test("should fold a cast of a folded literal") {
    val t      = PType.Decimal(21, 2)
    val folded = fold(CastExpr(int(11), t))
    folded shouldMatch { case Const(v: JBigDecimal, `t`) =>
      v.compareTo(JBigDecimal("11")) shouldBe 0
    }
  }

  test("should fold a comparison of literals") {
    fold(Compare(CmpOp.Lt, int(1), int(2))) shouldBe yes
  }

  test("should keep expressions over columns") {
    val e = Arith(ArithOp.Add, i32, int(1), PType.Int32)
    fold(e) shouldBeTheSameInstanceAs e
  }

  test("should keep an expression whose evaluation fails") {
    val e = RowFn("fails", Seq(int(1)), PType.Int32, _ => throw IllegalArgumentException("bad"))
    fold(e) shouldBeTheSameInstanceAs e
  }

  test("should keep volatile functions") {
    val e = RowFn("random", Seq(int(1)), PType.Float64, _ => 0.5)
    fold(e) shouldBeTheSameInstanceAs e
  }

  test("should simplify and/or with a literal operand under three-valued logic") {
    fold(AndExpr(no, flag)) shouldBe no
    fold(AndExpr(flag, no)) shouldBe no
    fold(AndExpr(yes, flag)) shouldBe flag
    fold(AndExpr(flag, yes)) shouldBe flag
    fold(OrExpr(yes, flag)) shouldBe yes
    fold(OrExpr(flag, yes)) shouldBe yes
    fold(OrExpr(no, flag)) shouldBe flag
    fold(OrExpr(flag, no)) shouldBe flag
    // A NULL operand isn't true or false
    val n = AndExpr(Const(null, PType.Bool), flag)
    fold(n) shouldBeTheSameInstanceAs n
  }

  test("should compare a widened integer column directly") {
    unwrapCompare(CmpOp.Le, CastExpr(i32, PType.Int64), Const(1211L, PType.Int64)) shouldBe
      Compare(CmpOp.Le, i32, int(1211))
    // The literal on the left keeps its side
    unwrapCompare(CmpOp.Gt, Const(5L, PType.Int64), CastExpr(i32, PType.Int64)) shouldBe
      Compare(CmpOp.Gt, int(5), i32)
  }

  test("should keep the cast when the literal doesn't fit the column type") {
    val c = CastExpr(i32, PType.Int64)
    val k = Const(3000000000L, PType.Int64)
    unwrapCompare(CmpOp.Lt, c, k) shouldBe Compare(CmpOp.Lt, c, k)
  }

  test("should compare a widened decimal column directly") {
    val t = PType.Decimal(12, 2)
    unwrapCompare(CmpOp.Gt, CastExpr(dec72, t), dec("50.00", t)) shouldMatch {
      case Compare(CmpOp.Gt, `dec72`, Const(v: JBigDecimal, PType.Decimal(7, 2))) =>
        v shouldBe JBigDecimal("50.00")
    }
  }

  test("should keep the cast when a decimal literal has more fraction digits") {
    val t = PType.Decimal(12, 3)
    val c = CastExpr(dec72, t)
    val k = dec("50.005", t)
    unwrapCompare(CmpOp.Gt, c, k) shouldBe Compare(CmpOp.Gt, c, k)
  }

  test("should keep casts that lose values or order") {
    // Fewer integer digits
    isLossless(PType.Decimal(7, 2), PType.Decimal(6, 2)) shouldBe false
    // Fewer fraction digits
    isLossless(PType.Decimal(7, 2), PType.Decimal(8, 1)) shouldBe false
    isLossless(PType.Int64, PType.Int32) shouldBe false
    isLossless(PType.Int64, PType.Decimal(18, 0)) shouldBe false
    isLossless(PType.Int32, PType.Decimal(10, 0)) shouldBe true
    isLossless(PType.Int32, PType.Float64) shouldBe false
    // A try_cast keeps its NULLs on failure, so it stays
    val c = CastExpr(i32, PType.Int64, tryCast = true)
    val k = Const(1L, PType.Int64)
    unwrapCompare(CmpOp.Eq, c, k) shouldBe Compare(CmpOp.Eq, c, k)
  }

  test("should unwrap an integer IN list when every value converts") {
    unwrapInList(CastExpr(i32, PType.Int64), Set(1L, 2L), hasNull = true, negate = false) shouldBe
      Some(InList(i32, Set(1, 2), hasNullValue = true, negate = false))
    unwrapInList(CastExpr(i32, PType.Int64), Set(1L, 3000000000L), false, false) shouldBe None
  }

  test("should evaluate the same before and after unwrapping") {
    val t   = PType.Decimal(12, 2)
    val col = LongVec(
      PType.Decimal(7, 2),
      Array(4999L, 5000L, 5001L, 0L),
      Array(false, false, false, true)
    )
    val batch = Batch(Array(IntVec(PType.Int32, Array(1, 2, 3, 4), null), col), 4)
    for op <- CmpOp.values do
      val cast      = Compare(op, CastExpr(dec72, t), dec("50.00", t))
      val unwrapped = unwrapCompare(op, CastExpr(dec72, t), dec("50.00", t))
      unwrapped shouldNotBe cast
      val a = cast.eval(batch)
      val b = unwrapped.eval(batch)
      (0 until 4).map(b.get) shouldBe (0 until 4).map(a.get)
  }

end BoundExprSimplifierTest
