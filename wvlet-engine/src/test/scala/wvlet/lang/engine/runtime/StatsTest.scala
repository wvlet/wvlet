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

import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.expr.LikeExpr
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/** Estimates of shared CTE reads taken from their computed results */
class StatsTest extends UniTest:
  private def longs(xs: Option[Long]*): Vec = LongVec(
    PType.Int64,
    xs.map(_.getOrElse(0L)).toArray,
    xs.map(_.isEmpty).toArray
  )

  // Two batches: x = 1, NULL, 2 and 2, NULL, 3
  private val batches = Seq(
    Batch(Array(longs(Some(1), None, Some(2))), 3),
    Batch(Array(longs(Some(2), None, Some(3))), 3),
    Batch(Array(longs()), 0)
  )

  private val x = ColRef(0, PType.Int64, "x")

  test("count the distinct non-NULL values of a result column across batches") {
    Stats.resultDistinct(batches, 0, PType.Int64) shouldBe 3
    Stats.resultDistinct(Seq(Batch(Array(longs(None, None)), 2)), 0, PType.Int64) shouldBe 0
  }

  test("count the rows a condition keeps, dropping those where it is NULL") {
    Stats.resultRows(batches, Compare(CmpOp.Gt, x, Const(1L, PType.Int64))) shouldBe Some(3)
    Stats.resultRows(batches, Compare(CmpOp.Ne, x, Const(2L, PType.Int64))) shouldBe Some(2)
    Stats.resultRows(batches, Compare(CmpOp.Eq, x, Const(null, PType.Int64))) shouldBe Some(0)
  }

  /** A computed CTE of `rowCount` rows whose reads have the given statistics */
  private class Computed(
      distinctCount: Option[Double],
      passing: Option[Long],
      rowCount: Long = 1000
  ) extends SharedCTE:
    val name = "t"
    val body = RuntimePlan.Values(
      IndexedSeq(Column(ColumnId(1), "k", PType.Int64)),
      Batch(Array(longs()), 0)
    )

    override def rows: Option[Long]                      = Some(rowCount)
    override def distinct(position: Int): Option[Double] = distinctCount
    override def filteredRows(cond: BExpr): Option[Long] = passing
    def reader(schema: Schema, filter: Option[JoinCondition], setContext: Boolean): Operator =
      throw UnsupportedOperationException("not read")

  private val k   = Column(ColumnId(7), "k", PType.Int64)
  private val eq3 =
    val e = Compare(CmpOp.Eq, ColRef(7, PType.Int64, "k"), Const(3L, PType.Int64))
    JoinCondition(BExpr(e), Map(e.sig -> 1))

  test("estimate a read of a computed CTE from its result's statistics") {
    val ref = RuntimePlan.CTERef(Computed(Some(50), Some(4)), IndexedSeq(k))
    Stats.distinct(ref, k.id) shouldBe Some(50.0)
    Stats.filteredRows(ref, Seq(eq3), OptimizerOptions()) shouldBe 4.0
    Stats.filteredRows(ref, Nil, OptimizerOptions()) shouldBe 1000.0
  }

  test("fall back to the selectivity estimate without the result's statistics") {
    val ref = RuntimePlan.CTERef(Computed(None, None), IndexedSeq(k))
    Stats.distinct(ref, k.id) shouldBe None
    Stats.filteredRows(ref, Seq(eq3), OptimizerOptions()) shouldBe 1000.0 * 0.3
    // With the distinct count only, `k = 3` keeps 1 / 50 of the rows
    val withDistinct = RuntimePlan.CTERef(Computed(Some(50), None), IndexedSeq(k))
    Stats.filteredRows(withDistinct, Seq(eq3), OptimizerOptions()) shouldBe 1000.0 / 50
  }

  test("estimate a prefix LIKE as a range with two bounds") {
    val ref  = RuntimePlan.CTERef(Computed(None, None), IndexedSeq(k))
    val name = ColRef(7, PType.Str, "k")
    def like(pattern: String, negate: Boolean = false) = JoinCondition(
      BExpr(LikeExpr(name, pattern, negate)),
      Map.empty
    )
    val on = OptimizerOptions()
    Stats.selectivity(ref, like("forest%"), on) shouldBe 0.3 * 0.3
    // Not a range: the default
    Stats.selectivity(ref, like("%forest"), on) shouldBe 0.3
    Stats.selectivity(ref, like("forest%", negate = true), on) shouldBe 0.3
    val off = on.withDisabled(Optimization.PrefixLikeSelectivity)
    Stats.selectivity(ref, like("forest%"), off) shouldBe 0.3
  }

  /**
    * A fact table of 1M rows with 1000 distinct dimension keys, joined with a dimension of 1000
    * rows filtered to one (`d = 3`)
    */
  private def starJoin: RuntimePlan =
    val f     = Column(ColumnId(1), "f", PType.Int64)
    val d     = Column(ColumnId(2), "d", PType.Int64)
    val fact  = RuntimePlan.CTERef(Computed(Some(1000), None, rowCount = 1000000), IndexedSeq(f))
    val dimEq = Compare(CmpOp.Eq, ColRef(2, PType.Int64, "d"), Const(3L, PType.Int64))
    val dim   = RuntimePlan.Filter(
      RuntimePlan.CTERef(Computed(Some(1000), None), IndexedSeq(d)),
      BExpr(dimEq),
      Map(dimEq.sig -> 1)
    )
    RuntimePlan.Join(
      fact,
      dim,
      wvlet.lang.engine.exec.JoinKind.Inner,
      IndexedSeq(BExpr(ColRef(1, PType.Int64, "f"))),
      IndexedSeq(BExpr(ColRef(2, PType.Int64, "d"))),
      None,
      buildLeft = false
    )

  test("estimate an aggregated star join from the joins and filters below the aggregation") {
    // The star join grouped by the fact key
    val join = starJoin
    val agg  = RuntimePlan.Aggregate(
      join,
      IndexedSeq(BExpr(ColRef(1, PType.Int64, "f"))),
      IndexedSeq.empty,
      IndexedSeq(Column(ColumnId(3), "f", PType.Int64))
    )
    // Stats.rows passes the fact table's rows through the join and the aggregation
    Stats.rows(agg) shouldBe 1e6
    // 1M rows * 1 dimension row / 1000 distinct keys
    Stats.aggregatedRows(agg, OptimizerOptions()) shouldBe Some(1000.0)
    // Not an aggregation, or the toggle off: no estimate
    Stats.aggregatedRows(join, OptimizerOptions()) shouldBe None
    Stats.aggregatedRows(
      agg,
      OptimizerOptions().withDisabled(Optimization.AggregateInputRows)
    ) shouldBe None
  }

  test("choose a semi join's build side from join-tree estimates") {
    // EXISTS over the star join, whose subquery reads 1M rows
    val sub    = Column(ColumnId(4), "s", PType.Int64)
    val subRef = RuntimePlan.CTERef(Computed(None, None, rowCount = 1000000), IndexedSeq(sub))
    val on     = OptimizerOptions()
    val off    = on.withDisabled(Optimization.JoinTreeBuildEstimates)
    // The star join keeps 1M * 1 / 1000 rows, while Stats.rows reads the fact table's rows
    Stats.buildRows(starJoin, on) shouldBe 1000.0
    Stats.buildRows(starJoin, off) shouldBe 1e6
    // Building on the star join's rows, which the subquery's scan probes
    Stats.swapsSemiBuild(starJoin, subRef, on) shouldBe true
    Stats.swapsSemiBuild(starJoin, subRef, off) shouldBe false
    // Plan-time rows count exactly
    val values = RuntimePlan.Values(IndexedSeq(sub), Batch(Array(longs(Some(1), Some(2))), 2))
    Stats.buildRows(values, on) shouldBe 2.0
    Stats.buildRows(values, off) shouldBe 1e3
  }

end StatsTest
