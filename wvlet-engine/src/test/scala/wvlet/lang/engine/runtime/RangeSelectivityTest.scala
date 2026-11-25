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

import wvlet.lang.engine.catalog.MemTable
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.plan.CardinalityEstimator
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/** Range comparisons estimated from a column's [min, max] */
class RangeSelectivityTest extends UniTest:
  private val d                         = ColRef(3, PType.Date, "d")
  private def date(days: Int)           = Const(days, PType.Date)
  private def cmp(op: CmpOp, days: Int) = Compare(op, d, date(days))

  test("read a bound from a comparison of a column with a constant, either way around") {
    RangeSelectivity.bound(cmp(CmpOp.Ge, 10)) shouldBe Some(RangeSelectivity.Bound(3, CmpOp.Ge, 10))
    RangeSelectivity.bound(Compare(CmpOp.Lt, date(10), d)) shouldBe
      Some(RangeSelectivity.Bound(3, CmpOp.Gt, 10))
    // Not a range: equality, another type, or two columns
    RangeSelectivity.bound(cmp(CmpOp.Eq, 10)) shouldBe None
    RangeSelectivity.bound(Compare(CmpOp.Lt, d, Const(10L, PType.Int64))) shouldBe None
    RangeSelectivity.bound(Compare(CmpOp.Lt, d, ColRef(4, PType.Date, "e"))) shouldBe None
  }

  test("read both bounds of a between") {
    RangeSelectivity.bounds(AndExpr(cmp(CmpOp.Ge, 10), cmp(CmpOp.Le, 19))).map(_.size) shouldBe
      Some(2)
    RangeSelectivity.bounds(AndExpr(cmp(CmpOp.Ge, 10), cmp(CmpOp.Eq, 19))) shouldBe None
  }

  test("estimate the share of [min, max] an interval of integers covers") {
    def f(bounds: (CmpOp, Int)*) = RangeSelectivity.fraction(
      bounds.map((op, v) => RangeSelectivity.Bound(3, op, v)),
      0,
      99
    )
    f(CmpOp.Ge -> 10, CmpOp.Lt -> 20) shouldBe 0.1
    f(CmpOp.Gt -> 9, CmpOp.Le -> 19) shouldBe 0.1
    f(CmpOp.Lt -> 50) shouldBe 0.5
    f(CmpOp.Ge -> -5) shouldBe 1.0
    // An empty interval still estimates one value
    f(CmpOp.Ge -> 60, CmpOp.Lt -> 40) shouldBe 0.01
  }

  // 100 rows whose column d spans 1000 days
  private val table =
    new MemTable(
      "t",
      Schema(IndexedSeq(Field("d", PType.Date))),
      Seq(Batch(Array(Vec.nullVec(PType.Date, 100)), 100))
    ):
      override def columnRange(column: String): Option[(Long, Long)] =
        Option.when(column == "d")((1000L, 1999L))

  private val scan = RuntimePlan.Scan(table, IndexedSeq(Column(ColumnId(3), "d", PType.Date)))
  private def conds(es: Compare*) = es.map(e => JoinCondition(BExpr(e)))
  private val month               = conds(cmp(CmpOp.Ge, 1100), cmp(CmpOp.Lt, 1130))

  test("estimate the bounds on one column as one interval with range_selectivity") {
    val on = OptimizerOptions()
    Stats.conjunctionSelectivity(scan, month, on) shouldBe 0.03
    Stats.filteredRows(scan, month, on) shouldBe 3.0
    // Without it, each comparison has the default selectivity
    val off = on.withDisabled(Optimization.RangeSelectivity)
    Stats.conjunctionSelectivity(scan, month, off) shouldBe
      CardinalityEstimator.DefaultSelectivity * CardinalityEstimator.DefaultSelectivity
  }

  test("keep the default selectivity of a column without a known range") {
    val e = ColRef(5, PType.Date, "e")
    val c = conds(Compare(CmpOp.Lt, e, date(1100)))
    Stats.conjunctionSelectivity(scan, c, OptimizerOptions()) shouldBe
      CardinalityEstimator.DefaultSelectivity
  }

end RangeSelectivityTest
