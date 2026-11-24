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
package wvlet.lang.engine

import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/** Runtime join filters: probe-side scans skip row groups outside the range of the build keys */
class EngineRuntimeJoinFiltersTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val engine  = WvletEngine(optimizer = options)
  private val off     = engine.withOptimizer(options.withDisabled(Optimization.RuntimeJoinFilters))
  EngineTPCHBench
    .tables
    .foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](sql: String)(f: Context ?=> wvlet.lang.model.plan.Relation => A): A =
    val unit      = CompilationUnit.fromSqlString(sql)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${unit}"))
    f(GenSQL.expand(q, summon[Context]))

  private def scans(op: Operator): Seq[ScanOp] =
    op match
      case s: ScanOp =>
        Seq(s)
      case other =>
        other.children.flatMap(scans)

  /** Run the query, returning its rows and the filter each scan read with, by table */
  private def run(e: WvletEngine, sql: String): (Seq[IndexedSeq[Any]], Map[String, String]) =
    withQuery(sql) { r =>
      val op   = e.physicalPlan(r)
      val rows = op
        .drain()
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toIndexedSeq))
      op.close()
      val filters = scans(op).map(s => s.source.name -> s.spec.filter.map(_.toString).getOrElse(""))
      (rows, filters.toMap)
    }

  private def explain(e: WvletEngine, sql: String): String = withQuery(sql)(e.explain(_))

  private val innerJoin =
    """select o_orderkey, l_linenumber from lineitem join orders on l_orderkey = o_orderkey
      |where o_orderdate < date '1992-01-10' order by 1, 2""".stripMargin

  test("filter the probe scan by the range of the build keys") {
    explain(engine, innerJoin) shouldContain "join_filter=l_orderkey"
    val (rows, filters) = run(engine, innerJoin)
    debug(filters)
    filters("lineitem") shouldContain "Compare(l_orderkey,GtEq,"
    filters("lineitem") shouldContain "Compare(l_orderkey,LtEq,"
    rows.nonEmpty shouldBe true
    run(off, innerJoin)._1 shouldBe rows
  }

  test("filter the probe scan of a semi join") {
    val q =
      """select l_orderkey, l_linenumber from lineitem
        |where l_orderkey in (select o_orderkey from orders where o_orderdate < date '1992-01-10')
        |order by 1, 2""".stripMargin
    val (rows, filters) = run(engine, q)
    filters("lineitem") shouldContain "Compare(l_orderkey,GtEq,"
    run(off, q)._1 shouldBe rows
  }

  test("filter each input scan of a union all on the probe side") {
    val q =
      """select u.k, u.n from (
        |  select l_partkey k, l_linenumber n from lineitem
        |  union all
        |  select ps_partkey k, ps_suppkey n from partsupp
        |) u join part on u.k = p_partkey
        |where p_name like 'forest%' order by 1, 2""".stripMargin
    val (rows, filters) = run(engine, q)
    debug(filters)
    filters("lineitem") shouldContain "Compare(l_partkey,GtEq,"
    filters("partsupp") shouldContain "Compare(ps_partkey,GtEq,"
    rows.nonEmpty shouldBe true
    run(off, q)._1 shouldBe rows
  }

  test("leave the probe side of joins that keep unmatched probe rows unfiltered") {
    val q =
      """select l_orderkey, l_linenumber, o_orderkey from lineitem
        |left join (select o_orderkey from orders where o_orderdate < date '1992-01-10') o
        |  on l_orderkey = o_orderkey
        |order by 1, 2""".stripMargin
    explain(engine, q) shouldNotContain "join_filter=l_orderkey"
    val (rows, _) = run(engine, q)
    run(off, q)._1 shouldBe rows
  }

  test("leave scans below an aggregation unfiltered") {
    val q =
      """select o_orderkey, n from orders
        |join (select l_orderkey, count(*) n from lineitem group by l_orderkey) l
        |  on l_orderkey = o_orderkey
        |where o_orderdate < date '1992-01-10' order by 1""".stripMargin
    // Without the semi join on the group key, which filters the aggregation's input on purpose
    val noGroupKeySemiJoins = engine.withOptimizer(
      options.withDisabled(Optimization.GroupKeySemiJoins)
    )
    explain(noGroupKeySemiJoins, q) shouldNotContain "join_filter=l_orderkey"
  }

  test("turn off with runtime_join_filters") {
    explain(off, innerJoin) shouldNotContain "join_filter="
  }

end EngineRuntimeJoinFiltersTest
