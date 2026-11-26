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
import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/** Group estimates over rows clustered by their keys (`clustered_group_estimates`) */
class ClusteredGroupsTest extends UniTest:
  private val on  = OptimizerOptions()
  private val off = on.withDisabled(Optimization.ClusteredGroupEstimates)

  /** A table of `rows` rows whose columns have the given distinct values and sampled runs */
  private def table(rows: Long, columns: Seq[(String, Double, Double)]): MemTable =
    new MemTable("t", Schema(columns.map((n, _, _) => Field(n, PType.Int64)).toIndexedSeq), Nil):
      override def estimatedRows: Option[Long]                       = Some(rows)
      override def estimatedDistinct(column: String): Option[Double] = columns
        .find(_._1 == column)
        .map(_._2)

      override def keyRunLength(column: String): Option[Double] = columns
        .find(_._1 == column)
        .map(_._3)

  // 1M sales lines in orders of 10 sharing the customer and the date; amounts vary per line
  private val sales = table(1000000, Seq(("cust", 100000, 10), ("dt", 1000, 10), ("amt", 1000, 1)))
  private val cust  = Column(ColumnId(0), "cust", PType.Int64)
  private val dt    = Column(ColumnId(1), "dt", PType.Int64)
  private val amt   = Column(ColumnId(2), "amt", PType.Int64)
  private val salesScan = RuntimePlan.Scan(sales, IndexedSeq(cust, dt, amt))

  private val dSk  = Column(ColumnId(10), "d_sk", PType.Int64)
  private val year = Column(ColumnId(11), "yr", PType.Int64)

  private def dateScan(keyDistinct: Double) = RuntimePlan.Scan(
    table(1000, Seq(("d_sk", keyDistinct, 1), ("yr", 5, 100))),
    IndexedSeq(dSk, year)
  )

  private def ref(c: Column) = BExpr.ref(c)

  private def groups(p: RuntimePlan, keys: Column*)(options: OptimizerOptions): Double = Stats
    .treeRows(
      RuntimePlan.Aggregate(p, keys.map(ref).toIndexedSeq, IndexedSeq.empty, keys.toIndexedSeq),
      options
    )

  test("estimate the groups of clustered rows over their clusters") {
    // 100K orders of 10 lines over 100K customers: 100K (1 - e^-1)
    math.round(groups(salesScan, cust)(on)) shouldBe 63212L
    groups(salesScan, cust)(off) shouldBe 100000.0
  }

  test("keep the row estimate when a filter selects rows of shorter runs") {
    val filtered = RuntimePlan.Filter(
      salesScan,
      BExpr(Compare(CmpOp.Gt, ColRef(2, PType.Int64, "amt"), Const(0L, PType.Int64)))
    )
    groups(filtered, cust)(on) shouldBe groups(filtered, cust)(off)
    groups(filtered, cust)(on) shouldBe 100000.0
  }

  test("trace a dimension column through a join on its unique key") {
    def joined(keyDistinct: Double) = RuntimePlan.Join(
      salesScan,
      dateScan(keyDistinct),
      JoinKind.Inner,
      IndexedSeq(ref(dt)),
      IndexedSeq(ref(dSk)),
      None,
      buildLeft = false
    )
    // 100K orders over 500K (customer, year) combinations: 500K (1 - e^-0.2)
    math.round(groups(joined(1000), cust, year)(on)) shouldBe 90635L
    // A build key that isn't unique doesn't tell the year's run
    val notUnique = joined(100)
    groups(notUnique, cust, year)(on) shouldBe groups(notUnique, cust, year)(off)
  }

  test("ignore short runs") {
    val lines = table(1000000, Seq(("cust", 100000, 1.5)))
    val scan  = RuntimePlan.Scan(lines, IndexedSeq(cust))
    groups(scan, cust)(on) shouldBe 100000.0
  }

end ClusteredGroupsTest
