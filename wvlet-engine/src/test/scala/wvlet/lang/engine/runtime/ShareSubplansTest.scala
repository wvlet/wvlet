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
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.vector.Field
import wvlet.lang.engine.vector.PType
import wvlet.lang.engine.vector.Schema
import wvlet.uni.test.UniTest

class ShareSubplansTest extends UniTest:
  private val schema = Schema(IndexedSeq(Field("a", PType.Int64), Field("b", PType.Int64)))
  private val t      = MemTable("t", schema, Nil)
  private val u      = MemTable("u", schema, Nil)

  /** `from <source> where a > <bound> select b`, with ids starting after `base` */
  private def plan(base: Int, bound: Long = 1L, source: MemTable = t, column: Int = 0) =
    val cols = IndexedSeq(
      Column(ColumnId(base + 1), "a", PType.Int64),
      Column(ColumnId(base + 2), "b", PType.Int64)
    )
    val scan = RuntimePlan.Scan(source, cols)
    val c    = cols(column)
    val cond = BExpr(
      Compare(CmpOp.Gt, ColRef(c.id.value, c.ptype, c.name), Const(bound, PType.Int64))
    )
    val filter = RuntimePlan.Filter(scan, cond, Map(cond.expr.sig -> 1))
    val out    = Column(ColumnId(base + 3), "b", PType.Int64)
    RuntimePlan.Project(filter, IndexedSeq(BExpr.ref(cols(1))), IndexedSeq(out))

  test("give plans equal up to column ids the same key") {
    val k = ShareSubplans.key(plan(0))
    k shouldNotBe None
    ShareSubplans.key(plan(100)) shouldBe k
  }

  test("give plans that differ in a constant, a column, or a source different keys") {
    val k = ShareSubplans.key(plan(0))
    ShareSubplans.key(plan(100, bound = 2L)) shouldNotBe k
    ShareSubplans.key(plan(100, column = 1)) shouldNotBe k
    ShareSubplans.key(plan(100, source = u)) shouldNotBe k
  }

  test("tell apart constants that compare equal as boxed values") {
    def project(v: Double) =
      val scan = RuntimePlan.Scan(t, IndexedSeq(Column(ColumnId(1), "a", PType.Int64)))
      RuntimePlan.Project(
        scan,
        IndexedSeq(BExpr(Const(v, PType.Float64))),
        IndexedSeq(Column(ColumnId(2), "v", PType.Float64))
      )
    ShareSubplans.key(project(0.0)) shouldNotBe ShareSubplans.key(project(-0.0))
  }

  test("give aggregates that differ only in their ORDER BY different keys") {
    def agg(order: Seq[wvlet.lang.engine.exec.SortDir]) =
      val cols = IndexedSeq(
        Column(ColumnId(1), "a", PType.Int64),
        Column(ColumnId(2), "b", PType.Int64)
      )
      val scan = RuntimePlan.Scan(t, cols)
      val call = AggregateCall(
        "array_agg",
        cols.map(BExpr.ref),
        distinct = false,
        filter = None,
        PType.AnyT,
        order
      )
      RuntimePlan.Aggregate(
        scan,
        IndexedSeq.empty,
        IndexedSeq(call),
        IndexedSeq(Column(ColumnId(3), "x", PType.AnyT))
      )
    val asc = wvlet.lang.engine.exec.SortDir(ascending = true, nullsFirst = false)
    ShareSubplans.key(agg(Seq(asc))) shouldNotBe
      ShareSubplans.key(agg(Seq(asc.copy(ascending = false))))
  }

  test("give no key to plans with outer references") {
    val scan  = RuntimePlan.Scan(t, IndexedSeq(Column(ColumnId(1), "a", PType.Int64)))
    val outer = BExpr(Compare(CmpOp.Gt, ColRef(1, PType.Int64, "a"), ColRef(99, PType.Int64, "x")))
    ShareSubplans.key(RuntimePlan.Filter(scan, outer)) shouldBe None
  }

end ShareSubplansTest
