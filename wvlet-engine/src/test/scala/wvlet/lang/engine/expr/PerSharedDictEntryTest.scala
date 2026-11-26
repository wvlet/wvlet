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

import scala.util.Random

/** [[PerSharedDictEntry]] gives the same values as evaluating its condition per row */
class PerSharedDictEntryTest extends UniTest:
  private val dict = Array("Sunday", "Monday", "Tuesday", "Wednesday")
  private val col  = ColRef(1, PType.Str, "day")
  private val rnd  = Random(7)

  /** n rows over `dict` in column 1, NULL (code -1) when `withNulls` and drawn so */
  private def batch(n: Int, withNulls: Boolean): Batch =
    val codes =
      Array.fill(n)(
        if withNulls && rnd.nextInt(6) == 0 then
          -1
        else
          rnd.nextInt(dict.length)
      )
    val values = codes.map(c =>
      if c >= 0 then
        dict(c)
      else
        null
    )
    val nulls =
      if withNulls then
        codes.map(_ < 0)
      else
        null
    val ints = IntVec(PType.Int32, Array.fill(n)(rnd.nextInt(10)), null)
    Batch(Array(ints, StrVec(values, nulls, codes, dict.length, dict)), n)

  private def str(s: String) = Const(s, PType.Str)

  private val conditions = Seq(
    Compare(CmpOp.Eq, col, str("Sunday")),
    Compare(CmpOp.Ne, col, str("Monday")),
    Compare(CmpOp.Lt, col, str("T")),
    OrExpr(Compare(CmpOp.Eq, col, str("Sunday")), LikeExpr(col, "W%", negate = false)),
    IsNullExpr(col, negate = false),
    NotExpr(Compare(CmpOp.Eq, col, str("Tuesday")))
  )

  private def values(v: Vec): Seq[Any] = (0 until v.size).map(v.get)

  test("give the same values as per-row evaluation over a shared dictionary") {
    for
      cond      <- conditions
      withNulls <- Seq(false, true)
    do
      val wrapped = DictionaryPredicates.wrapShared(cond)
      (wrapped != cond) shouldBe true
      // The second batch reads the results kept for the dictionary
      for b <- Seq(batch(100, withNulls), batch(37, withNulls)) do
        values(wrapped.eval(b)) shouldBe values(cond.eval(b))
  }

  test("give vectors without nulls when no row or entry is NULL") {
    val e = DictionaryPredicates.wrapShared(Compare(CmpOp.Eq, col, str("Sunday")))
    Kernels.asBool(e.eval(batch(50, withNulls = false))).nulls shouldBe null
    val b = batch(200, withNulls = true)
    (Kernels.asBool(e.eval(b)).nulls != null) shouldBe true
  }

  test("evaluate per row without codes") {
    val n      = 20
    val strs   = Array.fill(n)(dict(rnd.nextInt(dict.length)))
    val b      = Batch(Array(IntVec(PType.Int32, new Array[Int](n), null), StrVec(strs, null)), n)
    val cond   = Compare(CmpOp.Eq, col, str("Monday"))
    val shared = DictionaryPredicates.wrapShared(cond)
    values(shared.eval(b)) shouldBe values(cond.eval(b))
  }

  test("leave conditions over other columns as they are") {
    val cond = Compare(CmpOp.Eq, ColRef(0, PType.Int32, "x"), Const(1, PType.Int32))
    DictionaryPredicates.wrapShared(cond) shouldBe cond
  }

  test("select the positions where the condition holds, over all rows or a selection") {
    for
      cond      <- conditions
      withNulls <- Seq(false, true)
    do
      val e = DictionaryPredicates.wrap(cond, whole = true, shared = true)
      e match
        case p: PerSharedDictEntry =>
          for b <- Seq(batch(100, withNulls), batch(37, withNulls)) do
            val mask = Kernels.trueMask(cond.eval(b))
            p.selectPositions(b, null).toSeq shouldBe (0 until b.size).filter(mask(_))
            val sel = (0 until b.size by 3).toArray
            p.selectPositions(b, sel).toSeq shouldBe sel.filter(mask(_)).toSeq
        case other =>
          fail(s"Not wrapped: ${other.sig}")
  }

end PerSharedDictEntryTest
