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
import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.plan.PlannerMode
import wvlet.lang.engine.runtime.*
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** Uncorrelated EXISTS / IN / NOT IN filter conjuncts as semi and anti joins on the runtime plan */
class EngineDecorrelateTest extends UniTest:
  import RuntimePlan.*

  private val options = OptimizerOptions().withValidatePlans(true)
  private val engine  = WvletEngine(optimizer = options)
  private val legacy  = engine.withOptimizer(options.withPlanner(PlannerMode.Legacy))
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

  private def runtimePlan(query: String): RuntimePlan =
    val p = withQuery(query)(engine.runtimePlan(_))
    debug(p.explain)
    p

  private def nodes(p: RuntimePlan): Seq[RuntimePlan] = p +: p.children.flatMap(nodes)

  private def joinKinds(p: RuntimePlan): Seq[JoinKind] = nodes(p).collect { case j: Join =>
    j.kind
  }

  private def hasLegacy(p: RuntimePlan): Boolean = nodes(p).exists(_.isInstanceOf[Legacy])

  private def rows(e: WvletEngine, query: String): Seq[String] =
    withQuery(query) { q =>
      e.execute(q)
        .batches
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).mkString("|")))
        .sorted
    }

  /** The rows of the query, the same on both planners */
  private def sameRows(query: String): Seq[String] =
    val r = rows(engine, query)
    r shouldBe rows(legacy, query)
    r

  test("bind IN over an uncorrelated subquery as a semi join") {
    val query =
      """select n_name from nation
        |where n_regionkey in (select r_regionkey from region where r_name = 'ASIA')""".stripMargin
    val p = runtimePlan(query)
    joinKinds(p) shouldBe Seq(JoinKind.Semi)
    hasLegacy(p) shouldBe false
    sameRows(query).size shouldBe 5
  }

  test("bind NOT IN as an anti join over the subquery's values") {
    val query =
      """select n_name from nation
        |where n_regionkey not in (select r_regionkey from region where r_name = 'ASIA')"""
        .stripMargin
    val p = runtimePlan(query)
    joinKinds(p) shouldBe Seq(JoinKind.Anti)
    sameRows(query).size shouldBe 20
  }

  test("return no rows for NOT IN when the subquery has a NULL") {
    val query =
      """select n_name from nation
        |where n_regionkey not in (
        |  select case when r_name = 'ASIA' then null else r_regionkey end from region
        |)""".stripMargin
    joinKinds(runtimePlan(query)) shouldBe Nil
    sameRows(query) shouldBe Nil
  }

  test("decide uncorrelated EXISTS at plan time") {
    val none =
      """select n_name from nation
        |where exists (select 1 from region where r_name = 'MARS')""".stripMargin
    hasLegacy(runtimePlan(none)) shouldBe false
    sameRows(none) shouldBe Nil
    val all =
      """select n_name from nation
        |where not exists (select 1 from region where r_name = 'MARS')""".stripMargin
    hasLegacy(runtimePlan(all)) shouldBe false
    sameRows(all).size shouldBe 25
  }

  test("filter by the plain conjuncts before the semi join") {
    val query =
      """select n_name from nation
        |where n_regionkey in (select r_regionkey from region where r_name <> 'ASIA')
        |  and n_name like 'A%'""".stripMargin
    val semi =
      nodes(runtimePlan(query))
        .collectFirst { case j: Join =>
          j
        }
        .get
    semi.kind shouldBe JoinKind.Semi
    semi.left.isInstanceOf[Filter] shouldBe true
    sameRows(query) shouldBe Seq("ALGERIA", "ARGENTINA")
  }

  /** The inner join directly above the first semi or anti join, if any */
  private def joinAboveSemi(p: RuntimePlan): Option[Join] = nodes(p).collectFirst {
    case j @ Join(l, r, JoinKind.Inner, _, _, _, _)
        if Seq(l, r).exists(c =>
          joinKinds(c).headOption.exists(k => k == JoinKind.Semi || k == JoinKind.Anti)
        ) =>
      j
  }

  test("join IN over one input of a join tree onto that input") {
    val query =
      """select n_name, r_name from nation, region
        |where n_regionkey = r_regionkey
        |  and n_nationkey in (select s_nationkey from supplier where s_acctbal > 9000)"""
        .stripMargin
    val p = runtimePlan(query)
    joinKinds(p) shouldContain JoinKind.Semi
    hasLegacy(p) shouldBe false
    // The semi join is below the inner join
    joinAboveSemi(p).isDefined shouldBe true
    sameRows(query).nonEmpty shouldBe true
  }

  test("join NOT IN over one input of a join tree onto that input") {
    val query =
      """select n_name, r_name from nation, region
        |where n_regionkey = r_regionkey
        |  and n_nationkey not in (select s_nationkey from supplier where s_acctbal > 9000)"""
        .stripMargin
    val p = runtimePlan(query)
    joinKinds(p) shouldContain JoinKind.Anti
    joinAboveSemi(p).isDefined shouldBe true
    sameRows(query).nonEmpty shouldBe true
  }

  test("return no rows for NOT IN over a join tree input when the subquery has a NULL") {
    val query =
      """select n_name, r_name from nation, region
        |where n_regionkey = r_regionkey
        |  and n_regionkey not in (
        |    select case when r_name = 'ASIA' then null else r_regionkey end from region
        |  )""".stripMargin
    joinKinds(runtimePlan(query)) shouldNotContain JoinKind.Anti
    sameRows(query) shouldBe Nil
  }

  test("keep IN over several inputs of a join tree above the ordered tree") {
    val query =
      """select n_name, r_name from nation, region
        |where n_regionkey = r_regionkey
        |  and n_nationkey + r_regionkey in (select s_nationkey from supplier)""".stripMargin
    val p = runtimePlan(query)
    joinKinds(p).headOption shouldBe Some(JoinKind.Semi)
    joinAboveSemi(p) shouldBe None
    sameRows(query).nonEmpty shouldBe true
  }

  test("keep IN above the ordered tree with place_semi_joins disabled") {
    val query =
      """select n_name, r_name from nation, region
        |where n_regionkey = r_regionkey
        |  and n_nationkey in (select s_nationkey from supplier where s_acctbal > 9000)"""
        .stripMargin
    val off = engine.withOptimizer(options.withDisabled(Optimization.PlaceSemiJoins))
    val p   = withQuery(query)(off.runtimePlan(_))
    joinKinds(p).headOption shouldBe Some(JoinKind.Semi)
    joinAboveSemi(p) shouldBe None
  }

  test("keep correlated EXISTS as a legacy node") {
    val query =
      """select n_name from nation
        |where exists (select 1 from region where r_regionkey = n_regionkey and r_name = 'ASIA')"""
        .stripMargin
    hasLegacy(runtimePlan(query)) shouldBe true
    sameRows(query).size shouldBe 5
  }

end EngineDecorrelateTest
