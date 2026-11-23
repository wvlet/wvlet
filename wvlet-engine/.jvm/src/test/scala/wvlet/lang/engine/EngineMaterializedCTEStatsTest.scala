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
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** Joins over shared CTEs ordered again with the CTEs' exact row counts */
class EngineMaterializedCTEStatsTest extends UniTest:
  private val options  = OptimizerOptions().withValidatePlans(true)
  private val engine   = WvletEngine(optimizer = options)
  private val disabled = engine.withOptimizer(
    options.withDisabled(Optimization.ReplanMaterializedCTEs)
  )

  private val rootOnly = engine.withOptimizer(
    options.withDisabled(Optimization.ReplanBoundCTEJoins)
  )

  private val boundBuilds = engine.withOptimizer(
    options.withDisabled(Optimization.ReplanSemiJoinBuilds)
  )

  private val noReaderStats = engine.withOptimizer(
    options.withDisabled(Optimization.CTEReaderStats)
  )

  private val rootBodies = engine.withOptimizer(options.withDisabled(Optimization.ReplanCTEBodies))

  EngineTPCHBench
    .tables
    .foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromSqlString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    f(GenSQL.expand(q, summon[Context]))

  private def explain(e: WvletEngine, query: String): String =
    val plan = withQuery(query)(q => e.explain(q))
    debug(plan)
    plan

  private def rows(e: WvletEngine, query: String): Seq[String] =
    withQuery(query) { q =>
      e.execute(q)
        .batches
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).mkString("|")))
        .sorted
    }

  // Two rows (one per l_linestatus), which the planner estimated as 1e6 before it was computed
  private val cte =
    """with t as (
      |  select l_linestatus as s, count(*) as c from lineitem group by l_linestatus
      |)
      |""".stripMargin

  /**
    * The first source of a flow-style plan at the top level: the probe side of its first join (a
    * shared body printed before it is indented)
    */
  private def firstScan(plan: String): String = plan
    .linesIterator
    .find(!_.startsWith(" "))
    .getOrElse("")
    .stripPrefix("↙ ")
    .takeWhile(_ != '(')

  test("build hash joins on a small shared CTE once its rows are known") {
    val query =
      s"""${cte}select o.o_orderkey, a.c, b.c
         |from orders o join t a on o.o_orderstatus = a.s join t b on a.s = b.s""".stripMargin
    // With the 1e6 estimate, the CTE reads are the probe side; with two rows, orders is
    firstScan(explain(disabled, query)) shouldBe "Scan[CTE[t]]"
    firstScan(explain(engine, query)) shouldBe "Scan[orders]"
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

  test("order joins by the rows each CTE reader's filter keeps") {
    // 2298 rows, one per customer and order status: `c > 0` keeps them all, `c > 20` one row
    val query =
      """with t as (
        |  select o_custkey as k, o_orderstatus as s, count(*) as c from orders
        |  group by o_custkey, o_orderstatus
        |)
        |select cu.c_name, a.c, b.c from customer cu
        |join t a on cu.c_custkey = a.k join t b on a.k = b.k
        |where a.c > 0 and b.c > 20""".stripMargin
    // Both reads estimated at 0.3 of the rows: customer joins each in turn. With the counts, the
    // read keeping one row joins the other read first, and customer probes their join
    val readsJoined = "HashJoin[Inner](#0 = #0) -> [k:long, c:long, k:long, c:long]"
    explain(noReaderStats, query) shouldNotContain readsJoined
    explain(engine, query) shouldContain readsJoined
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

  test("re-plan joins whose CTE readers' filters were pushed into the body") {
    val query =
      s"""${cte}select o.o_orderkey, a.c, b.c
         |from orders o join t a on o.o_orderstatus = a.s join t b on a.s = b.s
         |where a.s = 'F' and b.s in ('F', 'O')""".stripMargin
    firstScan(explain(engine, query)) shouldBe "Scan[orders]"
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

  test("keep the plan when the exact rows don't change the join order") {
    val query = s"""${cte}select a.s, a.c, b.c from t a join t b on a.s = b.s""".stripMargin
    explain(engine, query) shouldBe explain(disabled, query)
    rows(engine, query) shouldBe rows(disabled, query)
  }

  test("re-plan joins of an aggregation input with the CTE's exact rows") {
    // The binder orders the aggregation input's joins before the CTE is computed
    val query =
      s"""${cte}select a.s, count(*) as n, sum(b.c) as m
         |from orders o join t a on o.o_orderstatus = a.s join t b on a.s = b.s
         |group by a.s""".stripMargin
    firstScan(explain(rootOnly, query)) shouldBe "Scan[CTE[t]]"
    firstScan(explain(engine, query)) shouldBe "Scan[orders]"
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

  test("keep the column order of a re-planned input read by position") {
    val query =
      s"""${cte}select * from (
         |  select o.o_orderkey, a.c from orders o join t a on o.o_orderstatus = a.s
         |  join t b on a.s = b.s group by o.o_orderkey, a.c
         |  union all
         |  select 0, c from t
         |)""".stripMargin
    explain(engine, query) shouldNotBe explain(rootOnly, query)
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

  test("build semi joins on a small shared CTE once its rows are known") {
    // With the 1e6 estimate, the IN semi joins build on orders (RightSemi); with two rows, on t
    val query =
      s"""${cte}select o_orderkey from orders
         |where o_orderstatus in (select s from t) and o_orderstatus in (select s from t where c > 0)
         |""".stripMargin
    explain(boundBuilds, query) shouldContain "HashJoin[RightSemi]"
    val plan = explain(engine, query)
    plan shouldNotContain "HashJoin[RightSemi]"
    plan shouldContain "HashJoin[Semi]"
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

  test("build anti joins on a small shared CTE once its rows are known") {
    val query =
      s"""${cte}select o_orderkey from orders o
         |where not exists (select 1 from t where t.s = o.o_orderstatus and t.c > 1000000)
         |  and exists (select 1 from t where t.s = o.o_orderstatus)""".stripMargin
    explain(boundBuilds, query) shouldContain "HashJoin[RightAnti]"
    val plan = explain(engine, query)
    plan shouldNotContain "HashJoin[RightAnti]"
    plan shouldContain "HashJoin[Anti]"
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

  test("re-plan the joins of a shared CTE body that reads a small shared CTE") {
    // u, read twice, joins t twice in its body, which is finalized before t
    val query =
      s"""${cte}, u as (
         |  select o.o_orderkey as k, a.c as c from orders o
         |  join t a on o.o_orderstatus = a.s join t b on a.s = b.s
         |)
         |select x.k, x.c, y.c from u x join u y on x.k = y.k""".stripMargin
    // The probe side of the body's join: the first scan of u's body (t's body, which reads
    // lineitem, is printed under t's first reader)
    def bodyScan(plan: String): String = plan
      .linesIterator
      .map(_.trim.stripPrefix("↙ "))
      .find(l => l.startsWith("Scan[") && !l.startsWith("Scan[lineitem]"))
      .getOrElse("")
      .takeWhile(_ != '(')
    // With the 1e6 estimate, the reads of t are the probe side; with two rows, orders is
    bodyScan(explain(rootBodies, query)) shouldBe "Scan[CTE[t]]"
    bodyScan(explain(engine, query)) shouldBe "Scan[orders]"
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

  test("re-plan a shared CTE body whose readers' filters were pushed into it") {
    // The filters of u's readers are pushed into u's body, and those of t's readers into t's
    val query =
      s"""${cte}, u as (
         |  select o.o_orderkey as k, o.o_totalprice as p, a.c as c from orders o
         |  join t a on o.o_orderstatus = a.s join t b on a.s = b.s
         |  where a.s = 'F' and b.s in ('F', 'O')
         |)
         |select x.k, x.c, y.c from u x join u y on x.k = y.k
         |where x.p > 1000 and y.p > 1000""".stripMargin
    explain(engine, query) shouldNotBe explain(rootBodies, query)
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

end EngineMaterializedCTEStatsTest
