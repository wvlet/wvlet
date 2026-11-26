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
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/** Row estimates of top-N rankings (`rank_filter_rows`) */
class RankFilterRowsTest extends UniTest:
  // A shared result of 1M rows whose columns have 50 distinct values each
  private class Computed extends SharedCTE:
    val name = "t"
    val body = RuntimePlan.Values(
      IndexedSeq(Column(ColumnId(1), "k", PType.Int64)),
      Batch(Array(LongVec(PType.Int64, Array.empty[Long], Array.empty[Boolean])), 0)
    )

    override def rows: Option[Long]                      = Some(1000000)
    override def distinct(position: Int): Option[Double] = Some(50.0)
    override def filteredRows(cond: BExpr): Option[Long] = None
    def reader(schema: Schema, filter: Option[JoinCondition], setContext: Boolean): Operator =
      throw UnsupportedOperationException("not read")

  private val g     = Column(ColumnId(1), "g", PType.Int64)
  private val v     = Column(ColumnId(2), "v", PType.Int64)
  private val rnk   = Column(ColumnId(3), "rnk", PType.Int64)
  private val input = RuntimePlan.CTERef(Computed(), IndexedSeq(g, v))

  private def ref(c: Column): ColRef = ColRef(c.id.value, c.ptype, c.name)

  private def window(name: String, partitioned: Boolean): RuntimePlan.Window = RuntimePlan.Window(
    input,
    IndexedSeq(
      WindowFunction(
        name,
        Seq.empty,
        if partitioned then
          Seq(BExpr(ref(g)))
        else
          Seq.empty
        ,
        Seq(OrderKey(BExpr(ref(v)), ascending = true, nullsFirst = false)),
        None,
        ignoreNulls = false,
        PType.Int64
      )
    ),
    IndexedSeq(rnk)
  )

  private def bounded(w: RuntimePlan, op: CmpOp, n: Long): RuntimePlan = RuntimePlan.Filter(
    w,
    BExpr(Compare(op, ref(rnk), Const(n, PType.Int64)))
  )

  test("estimate a top-N ranking without PARTITION BY as N rows") {
    RankFilterRows.rows(bounded(window("rank", false), CmpOp.Lt, 11)) shouldBe Some(10.0)
    RankFilterRows.rows(bounded(window("row_number", false), CmpOp.Le, 5)) shouldBe Some(5.0)
    // Through projections and other filters above it
    val above = RuntimePlan.Project(
      RuntimePlan.Filter(
        bounded(window("rank", false), CmpOp.Eq, 1),
        BExpr(Const(true, PType.Bool))
      ),
      IndexedSeq(BExpr(ref(g))),
      IndexedSeq(g)
    )
    RankFilterRows.rows(above) shouldBe Some(1.0)
  }

  test("estimate a top-N ranking as N rows per partition") {
    // 50 partitions of the g values
    RankFilterRows.rows(bounded(window("rank", true), CmpOp.Le, 3)) shouldBe Some(150.0)
    // The tightest bound among the conjuncts, written either way round
    val both = RuntimePlan.Filter(
      window("row_number", true),
      BExpr(
        AndExpr(
          Compare(CmpOp.Le, ref(rnk), Const(10L, PType.Int64)),
          Compare(CmpOp.Gt, Const(3L, PType.Int64), ref(rnk))
        )
      )
    )
    RankFilterRows.rows(both) shouldBe Some(100.0)
  }

  test("not bound dense_rank, lower bounds, or plans without a ranking filter") {
    RankFilterRows.rows(bounded(window("dense_rank", false), CmpOp.Lt, 11)) shouldBe None
    RankFilterRows.rows(bounded(window("rank", false), CmpOp.Gt, 11)) shouldBe None
    RankFilterRows.rows(window("rank", false)) shouldBe None
    RankFilterRows.rows(input) shouldBe None
  }

end RankFilterRowsTest
