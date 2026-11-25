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

/**
  * Bind-time constant folding (`fold_bound_constants`) and cast unwrapping (`unwrap_casts`): folded
  * bounds and uncast columns reach the scans' row-group filters
  */
class EngineBoundExprSimplifierTest extends UniTest:
  import RuntimePlan.*

  private val options = OptimizerOptions().withValidatePlans(true)
  private val engine  = WvletEngine(optimizer = options)
  EngineTPCHBench
    .tables
    .foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](sql: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromSqlString(sql)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${unit}"))
    f(GenSQL.expand(q, summon[Context]))

  private def scanFilters(e: WvletEngine, sql: String): Seq[Option[String]] =
    val p = withQuery(sql)(e.runtimePlan(_))
    debug(p.explain)
    def scans(p: RuntimePlan): Seq[Scan] =
      p match
        case s: Scan =>
          Seq(s)
        case other =>
          other.children.flatMap(scans)
    scans(p).map(_.filter.map(_.toString))

  private def explain(e: WvletEngine, sql: String): String =
    val plan = withQuery(sql)(e.explain(_))
    debug(plan)
    plan

  private def rows(e: WvletEngine, sql: String): Seq[Seq[Any]] =
    withQuery(sql)(e.execute(_)).rows.map(_.toSeq).toSeq

  private def without(o: Optimization*): WvletEngine = engine.withOptimizer(
    options.withDisabled(o*)
  )

  private val betweenQuery =
    "select n_name from nation where n_nationkey between 10 and 10 + 4 order by n_name"

  test("should put a folded bound into the scan filter") {
    scanFilters(engine, betweenQuery) shouldBe
      Seq(Some("And(Compare(n_nationkey,GtEq,10),Compare(n_nationkey,LtEq,14))"))
    // Unfolded, the upper bound `10 + 4` is not a literal
    scanFilters(without(Optimization.FoldBoundConstants), betweenQuery) shouldBe
      Seq(Some("Compare(n_nationkey,GtEq,10)"))
    rows(engine, betweenQuery) shouldBe rows(without(Optimization.FoldBoundConstants), betweenQuery)
  }

  private val castQuery =
    "select n_name from nation where n_nationkey <= cast(3 as bigint) order by n_name"

  test("should filter the scan by a column compared through a widening cast") {
    scanFilters(engine, castQuery) shouldBe Seq(Some("Compare(n_nationkey,LtEq,3)"))
    scanFilters(without(Optimization.UnwrapCasts), castQuery) shouldBe Seq(None)
    rows(engine, castQuery) shouldBe rows(without(Optimization.UnwrapCasts), castQuery)
  }

  private val decimalQuery =
    "select count(*) from lineitem where l_quantity <= cast(1 + 10 as decimal(21,2))"

  test("should compare a decimal column without a per-row cast") {
    explain(engine, decimalQuery) shouldContain "Filter((#0<=lit(11.00:decimal(15,2)))"
    explain(without(Optimization.UnwrapCasts), decimalQuery) shouldContain
      "Filter((cast(#0 as decimal(21,2))<=lit(11.00:decimal(21,2)))"
    rows(engine, decimalQuery) shouldBe rows(without(Optimization.UnwrapCasts), decimalQuery)
  }

end EngineBoundExprSimplifierTest
