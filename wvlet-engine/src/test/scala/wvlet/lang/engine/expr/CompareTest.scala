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

class CompareTest extends UniTest:
  private val dec = PType.decimal(15, 2)

  private def eval(e: VExpr, cols: Vec*): Seq[Any] =
    val b = Batch(cols.toArray, cols.head.size)
    val v = e.eval(b)
    (0 until v.size).map(v.get)

  test("should compare a column with a constant on either side") {
    val x = LongVec(PType.Int64, Array(1L, 5L, 9L), Array(false, true, false))
    val c = ColRef(0, PType.Int64, "x")
    val k = Const(5L, PType.Int64)
    eval(Compare(CmpOp.Lt, c, k), x) shouldBe Seq(true, null, false)
    // 5 < x is x > 5
    eval(Compare(CmpOp.Lt, k, c), x) shouldBe Seq(false, null, true)
    for op <- CmpOp.values do
      eval(Compare(op, c, k), x) shouldBe
        eval(Compare(op, c, ColRef(1, PType.Int64, "k")), x, Vec.constant(PType.Int64, 5L, 3))
  }

  test("should compare decimals with a constant at the column's scale") {
    val x = LongVec(dec, Array(100L, 150L, 199L), null) // 1.00, 1.50, 1.99
    val c = ColRef(0, dec, "x")
    eval(Compare(CmpOp.Ge, c, Const(JBigDecimal("1.5"), dec)), x) shouldBe Seq(false, true, true)
    // a constant with more digits than the scale falls back to exact comparison
    eval(Compare(CmpOp.Gt, c, Const(JBigDecimal("1.505"), dec)), x) shouldBe Seq(false, false, true)
  }

  test("should return NULL for every row when the constant is NULL") {
    val x = IntVec(PType.Int32, Array(1, 2), null)
    eval(Compare(CmpOp.Eq, ColRef(0, PType.Int32, "x"), Const(null, PType.Int32)), x) shouldBe
      Seq(null, null)
  }

  test("should compare strings with NULL rows") {
    val x = StrVec(Array("a", null, "c"), Array(false, true, false))
    eval(Compare(CmpOp.Ne, ColRef(0, PType.Str, "s"), Const("a", PType.Str)), x) shouldBe
      Seq(false, null, true)
  }

  test("should test string equality with a constant, with and without NULL rows") {
    val c = ColRef(0, PType.Str, "s")
    val x = StrVec(Array("Sunday", "Monday", "Sun", "Sundays"), null)
    eval(Compare(CmpOp.Eq, c, Const("Sunday", PType.Str)), x) shouldBe
      Seq(true, false, false, false)
    eval(Compare(CmpOp.Ne, Const("Sunday", PType.Str), c), x) shouldBe Seq(false, true, true, true)
    val y = StrVec(Array(null, "b"), Array(true, false))
    eval(Compare(CmpOp.Eq, c, Const("b", PType.Str)), y) shouldBe Seq(null, true)
  }

  test("should reuse the constant vector for batches of the same size") {
    val k = Const(1L, PType.Int64)
    val b = Batch.ofRows(4)
    k.eval(b) shouldBeTheSameInstanceAs k.eval(b)
    k.eval(Batch.ofRows(2)).size shouldBe 2
  }

end CompareTest
