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

import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.InList
import wvlet.lang.engine.expr.RowFn
import wvlet.lang.engine.vector.PType
import wvlet.uni.test.UniTest

/** [[PushDownFilters.shared]]: what a shared CTE body applies for all of its readers */
class SharedReaderFiltersTest extends UniTest:
  private val x = ColRef(0, PType.Str, "x")
  private val y = ColRef(1, PType.Str, "y")

  private def cond(e: wvlet.lang.engine.expr.VExpr): JoinCondition = JoinCondition(BExpr(e))

  test("keep both readers' conjuncts when only their signatures match") {
    // Two tuple IN conjuncts over different subqueries: the same name and arguments, different
    // closures
    val a = RowFn("tuple_in", Seq(x, y), PType.Bool, _ => true)
    val b = RowFn("tuple_in", Seq(x, y), PType.Bool, _ => false)
    a.sig shouldBe b.sig
    val shared = PushDownFilters.shared(Seq(cond(a), cond(b)))
    shared.map(_.cond.expr) shouldNotContain a
    shared.map(_.cond.expr) shouldNotContain b
  }

  test("tell a NOT IN list with a NULL from one without") {
    val withNull    = InList(x, Set("F"), hasNullValue = true, negate = true)
    val withoutNull = InList(x, Set("F"), hasNullValue = false, negate = true)
    withNull.sig shouldNotBe withoutNull.sig
    // Neither is applied alone: the body keeps the rows either reader keeps
    PushDownFilters.shared(Seq(cond(withNull), cond(withoutNull))).map(_.cond.toString) shouldBe
      Seq(s"(${withNull.sig} or ${withoutNull.sig})")
  }

  test("apply a conjunct every reader has once") {
    val c = InList(x, Set("F"), hasNullValue = false, negate = true)
    PushDownFilters.shared(Seq(cond(c), cond(c))).map(_.cond.expr) shouldBe Seq(c)
  }

end SharedReaderFiltersTest
