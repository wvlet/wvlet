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

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.model.expr.*
import wvlet.uni.test.UniTest

/** The literal counts that let `x = c` and `x in (...)` read the distinct values of x */
class LiteralCountTest extends UniTest:
  private val x                      = UnquotedIdentifier("x", NoSpan)
  private def num(v: Long)           = LongLiteral(v, v.toString, NoSpan)
  private def plus(a: Long, b: Long) = ArithmeticBinaryExpr(
    BinaryExprType.Add,
    num(a),
    num(b),
    NoSpan
  )

  private val yearList = In(x, List(num(1999), plus(1999, 1), plus(1999, 2)), NoSpan)

  test("should count only literals by default") {
    CardinalityEstimator.literalCount(In(x, List(num(1), num(2)), NoSpan)) shouldBe Some(2)
    CardinalityEstimator.literalCount(yearList) shouldBe None
    CardinalityEstimator.literalCount(Eq(x, plus(1, 2), NoSpan)) shouldBe None
  }

  test("should count constant expressions of literals with constants") {
    CardinalityEstimator.literalCount(yearList, constants = true) shouldBe Some(3)
    CardinalityEstimator.literalCount(Eq(x, plus(1, 2), NoSpan), constants = true) shouldBe Some(1)
    CardinalityEstimator.literalCount(
      Eq(
        ParenthesizedExpression(ArithmeticUnaryExpr(Sign.Negative, num(1), NoSpan), NoSpan),
        x,
        NoSpan
      ),
      constants = true
    ) shouldBe Some(1)
  }

  test("should not count expressions reading a column or NULL") {
    val y = UnquotedIdentifier("y", NoSpan)
    CardinalityEstimator.literalCount(
      In(x, List(num(1), ArithmeticBinaryExpr(BinaryExprType.Add, y, num(1), NoSpan)), NoSpan),
      constants = true
    ) shouldBe None
    CardinalityEstimator.literalCount(
      In(
        x,
        List(num(1), ArithmeticBinaryExpr(BinaryExprType.Add, NullLiteral(NoSpan), num(1), NoSpan)),
        NoSpan
      ),
      constants = true
    ) shouldBe None
  }

end LiteralCountTest
