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

/**
  * FactorOr and PushDownFilters match conditions by `VExpr.sig`, so expressions with different
  * values need different signatures, also when a literal's text looks like signature syntax
  */
class VExprSigTest extends UniTest:
  private val s                    = ColRef(0, PType.Str, "s")
  private def str(v: String)       = Const(v, PType.Str)
  private def coalesce(xs: VExpr*) = Coalesce(s +: xs, PType.Str)

  test("tell a string literal from a typed NULL") {
    str("null").sig shouldNotBe Const(null, PType.Str).sig
    Const(null, PType.Str).sig shouldBe "lit(null:string)"
  }

  test("tell a literal that prints like an argument list from the arguments") {
    coalesce(str("x:string),lit(y")).sig shouldNotBe coalesce(str("x"), str("y")).sig
    coalesce(str("x,y")).sig shouldNotBe coalesce(str("x"), str("y")).sig
  }

  test("keep plain literals as they print") {
    str("x").sig shouldBe "lit(x:string)"
    str("it's").sig shouldBe "lit('it''s':string)"
    Const(1, PType.Int32).sig shouldBe "lit(1:int)"
  }

  test("tell lists of values apart") {
    Const(List("a, b"), PType.AnyT).sig shouldNotBe Const(List("a", "b"), PType.AnyT).sig
    Const(List("[a]"), PType.AnyT).sig shouldNotBe Const(List(List("a")), PType.AnyT).sig
  }

  test("tell like patterns with different escape characters apart") {
    val esc   = LikeExpr(s, "a!%", negate = false, escape = Some('!'))
    val plain = LikeExpr(s, "a!%", negate = false)
    esc.sig shouldNotBe plain.sig
    plain.sig shouldBe "like(#0,a!%,false,false)"
    LikeExpr(s, "x,true", negate = false).sig shouldNotBe LikeExpr(s, "x", negate = true).sig
  }

end VExprSigTest
