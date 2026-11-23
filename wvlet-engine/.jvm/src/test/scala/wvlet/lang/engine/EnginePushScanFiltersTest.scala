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
import wvlet.lang.engine.runtime.*
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** The PushScanFilters pass: row-group filters and scan columns decided on the runtime plan */
class EnginePushScanFiltersTest extends UniTest:
  import RuntimePlan.*

  private val options = OptimizerOptions().withValidatePlans(true)
  private val engine  = WvletEngine(optimizer = options)
  EngineTPCHBench
    .tables
    .foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](unit: CompilationUnit)(f: Context ?=> Relation => A): A =
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${unit}"))
    f(GenSQL.expand(q, summon[Context]))

  private def runtimePlan(e: WvletEngine, query: String): RuntimePlan =
    val p = withQuery(CompilationUnit.fromWvletString(query))(e.runtimePlan(_))
    debug(p.explain)
    p

  private def explain(query: String): String =
    val plan = withQuery(CompilationUnit.fromSqlString(query))(engine.explain(_))
    debug(plan)
    plan

  private def scans(p: RuntimePlan): Seq[Scan] =
    p match
      case s: Scan =>
        Seq(s)
      case other =>
        other.children.flatMap(scans)

  test("attach the filter over a scan as its row-group filter") {
    val p = runtimePlan(
      engine,
      """from lineitem
        |where l_shipdate >= '1994-01-01'.to_date and l_quantity < 24
        |select l_orderkey""".stripMargin
    )
    p shouldMatch { case Project(Filter(s: Scan, _, _), _, _) =>
      // l_quantity is a decimal column, which row-group filters don't compare yet
      s.filter.map(_.toString) shouldBe Some("Compare(l_shipdate,GtEq,1994-01-01)")
      // The scan reads only the columns the filter and the projection read
      s.columns.map(_.name).toSet shouldBe Set("l_orderkey", "l_quantity", "l_shipdate")
    }
  }

  test("attach filters pushed below a join to each scan") {
    val p = runtimePlan(
      engine,
      """from orders join customer on o_custkey = c_custkey
        |where o_orderdate < '1995-03-15'.to_date and c_mktsegment = 'BUILDING'
        |select o_orderkey""".stripMargin
    )
    scans(p).map(s => s.source.name -> s.filter.map(_.toString)).toMap shouldBe
      Map(
        "orders"   -> Some("Compare(o_orderdate,Lt,1995-03-15)"),
        "customer" -> Some("Compare(c_mktsegment,Eq,BUILDING)")
      )
  }

  test("leave scans without a filter over them unfiltered") {
    val p = runtimePlan(engine, "from nation select n_name")
    scans(p).map(_.filter) shouldBe Seq(None)
  }

  test("turn off with push_scan_filters") {
    val off = engine.withOptimizer(options.withDisabled(Optimization.PushScanFilters))
    val q   = "from region where r_name = 'ASIA' select r_regionkey"
    scans(runtimePlan(off, q)).map(_.filter) shouldBe Seq(None)
    val plan = withQuery(CompilationUnit.fromWvletString(q))(off.explain(_))
    plan shouldNotContain "filter="
  }

  test("filter a scan once in the physical plan") {
    val plan = explain("select r_regionkey from region where r_name = 'ASIA'")
    plan shouldContain "Scan[region](columns=r_regionkey,r_name filter=Compare(r_name,Eq,ASIA))"
  }

  test("filter the scans of a shared CTE body with the readers' conditions") {
    val plan = explain(
      """with o as (select o_orderkey, o_orderdate from orders)
        |select count(*) from o a, o b
        |where a.o_orderkey = b.o_orderkey
        |  and a.o_orderdate < date '1995-01-01' and b.o_orderdate < date '1994-01-01'"""
        .stripMargin
    )
    plan shouldContain "Scan[orders](columns=o_orderkey,o_orderdate filter=Or("
  }

end EnginePushScanFiltersTest
