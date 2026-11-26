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

class DeferredNullChecksTest extends UniTest:
  private val col = ColRef(0, PType.Int64, "x")

  private def bools(v: Vec): Seq[Boolean] = (0 until v.size).map(i => v.get(i) == true)

  // Rows 0..3 of a source, then padded NULL rows (-1)
  private val positions = Array(3, -1, 0, 2, -1, 1)

  private def deferred(src: Vec): Batch = Batch(Array(src), src.size).take(
    positions,
    positions.length
  )

  test("should answer IS NULL of a deferred column from its positions and source nulls") {
    val src = LongVec(PType.Int64, Array(10L, 11L, 12L, 13L), Array(false, true, false, true))
    Seq(false, true).foreach { negate =>
      val plain   = IsNullExpr(col, negate).eval(deferred(src))
      val b       = deferred(src)
      val inPlace = IsNullExpr(col, negate, deferred = true).eval(b)
      bools(inPlace) shouldBe bools(plain)
      // The column was not gathered
      (b.deferredSource(0) ne null) shouldBe true
    }
  }

  test("should answer IS NULL of a deferred column whose source has no nulls") {
    val src  = LongVec(PType.Int64, Array(10L, 11L, 12L, 13L), null)
    val expr = IsNullExpr(col, negate = false, deferred = true)
    bools(expr.eval(deferred(src))) shouldBe Seq(false, true, false, false, true, false)
  }

  test("should evaluate IS NULL of a gathered column as before") {
    val src  = LongVec(PType.Int64, Array(10L, 11L), Array(false, true))
    val expr = IsNullExpr(col, negate = true, deferred = true)
    bools(expr.eval(Batch(Array(src), 2))) shouldBe Seq(true, false)
  }

  test("should mark only IS NULL of column references") {
    val e = AndExpr(IsNullExpr(col, negate = false), IsNullExpr(Const(1L, PType.Int64), true))
    DeferredNullChecks(e) shouldBe
      AndExpr(
        IsNullExpr(col, negate = false, deferred = true),
        IsNullExpr(Const(1L, PType.Int64), true)
      )
  }

end DeferredNullChecksTest
