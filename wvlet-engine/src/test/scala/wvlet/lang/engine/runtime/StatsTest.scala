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

  /** A computed CTE of 1000 rows whose reads have the given statistics */
  private class Computed(distinctCount: Option[Double], passing: Option[Long]) extends SharedCTE:
    val name = "t"
    val body = RuntimePlan.Values(
      IndexedSeq(Column(ColumnId(1), "k", PType.Int64)),
      Batch(Array(longs()), 0)
    )

    override def rows: Option[Long]                                     = Some(1000)
    override def distinct(position: Int): Option[Double]                = distinctCount
    override def filteredRows(cond: BExpr): Option[Long]                = passing
    def reader(schema: Schema, filter: Option[JoinCondition]): Operator =
      throw UnsupportedOperationException("not read")

  private val k   = Column(ColumnId(7), "k", PType.Int64)
  private val eq3 =
    val e = Compare(CmpOp.Eq, ColRef(7, PType.Int64, "k"), Const(3L, PType.Int64))
    JoinCondition(BExpr(e), Map(e.sig -> 1))

  test("estimate a read of a computed CTE from its result's statistics") {
    val ref = RuntimePlan.CTERef(Computed(Some(50), Some(4)), IndexedSeq(k))
    Stats.distinct(ref, k.id) shouldBe Some(50.0)
    Stats.filteredRows(ref, Seq(eq3)) shouldBe 4.0
    Stats.filteredRows(ref, Nil) shouldBe 1000.0
  }

  test("fall back to the selectivity estimate without the result's statistics") {
    val ref = RuntimePlan.CTERef(Computed(None, None), IndexedSeq(k))
    Stats.distinct(ref, k.id) shouldBe None
    Stats.filteredRows(ref, Seq(eq3)) shouldBe 1000.0 * 0.3
    // With the distinct count only, `k = 3` keeps 1 / 50 of the rows
    val withDistinct = RuntimePlan.CTERef(Computed(Some(50), None), IndexedSeq(k))
    Stats.filteredRows(withDistinct, Seq(eq3)) shouldBe 1000.0 / 50
  }

end StatsTest
