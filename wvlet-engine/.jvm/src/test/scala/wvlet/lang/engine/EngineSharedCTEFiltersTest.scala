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

/** Shared CTEs computing only the rows their readers' filters agree on */
class EngineSharedCTEFiltersTest extends UniTest:
  private val options  = OptimizerOptions().withValidatePlans(true)
  private val engine   = WvletEngine(optimizer = options)
  private val disabled = engine.withOptimizer(options.withDisabled(Optimization.PushDownCTEFilters))
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

  /** The lines of the shared CTE body in a physical plan */
  private def sharedBody(plan: String): String = plan
    .linesIterator
    .dropWhile(!_.trim.startsWith("(shared)"))
    .drop(1)
    .takeWhile(_.startsWith("    "))
    .mkString("\n")

  private def bodyFilters(e: WvletEngine, query: String): Seq[String] =
    sharedBody(explain(e, query)).linesIterator.map(_.trim).filter(_.startsWith("↓ Filter")).toSeq

  private def rows(e: WvletEngine, query: String): Seq[String] =
    withQuery(query) { q =>
      e.execute(q)
        .batches
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).mkString("|")))
        .sorted
    }

  /** The query returns the same rows with the shared filter and without it */
  private def sameRows(query: String): Unit =
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)

  private val cte =
    """with t as (
      |  select l_returnflag, l_linestatus, sum(l_quantity) as q from lineitem
      |  group by l_returnflag, l_linestatus
      |)
      |""".stripMargin

  test("filter a shared CTE body by the or of its readers' conditions on a column") {
    val query =
      s"""${cte}select a.l_linestatus, a.q, b.q
         |from t a join t b on a.l_linestatus = b.l_linestatus
         |where a.l_returnflag = 'R' and b.l_returnflag = 'A' and a.q > 0""".stripMargin
    val filters = bodyFilters(engine, query)
    // Below the aggregation, before its rows are grouped; `a.q > 0` stays with its reader
    filters.size shouldBe 1
    filters.head shouldContain "lit(R:string)"
    filters.head shouldContain " or "
    filters.head shouldContain "lit(A:string)"
    filters.head shouldNotContain "lit(0"
    sameRows(query)
  }

  test("push a condition every reader has as it is") {
    val query =
      s"""${cte}select a.l_returnflag, a.q, b.q
         |from t a join t b on a.l_returnflag = b.l_returnflag
         |where a.l_linestatus = 'F' and b.l_linestatus = 'F'""".stripMargin
    val filters = bodyFilters(engine, query)
    filters.size shouldBe 1
    filters.head shouldContain "lit(F:string)"
    filters.head shouldNotContain " or "
    sameRows(query)
  }

  test("push below a filter over the body's aggregation") {
    val query =
      """with t as (
        |  select l_returnflag, l_linestatus, sum(l_quantity) as q from lineitem
        |  group by l_returnflag, l_linestatus
        |  having sum(l_quantity) > 0
        |)
        |select a.l_linestatus, a.q, b.q
        |from t a join t b on a.l_linestatus = b.l_linestatus
        |where a.l_returnflag = 'R' and b.l_returnflag = 'A'""".stripMargin
    val body = sharedBody(explain(engine, query)).linesIterator.map(_.trim).toSeq
    // The readers' condition runs before the aggregation, not after the HAVING filter
    val agg = body.indexWhere(_.contains("Agg"))
    val f   = body.indexWhere(l => l.startsWith("↓ Filter") && l.contains("lit(R:string)"))
    f shouldNotBe -1
    (f < agg) shouldBe true
    sameRows(query)
  }

  test("keep the body unfiltered when a reader has no filter") {
    val query =
      s"""${cte}select a.l_linestatus, a.q, b.q
         |from t a join t b on a.l_linestatus = b.l_linestatus
         |where a.l_returnflag = 'R'""".stripMargin
    bodyFilters(engine, query) shouldBe Nil
    sameRows(query)
  }

  test("keep the body unfiltered when an IN subquery reads it") {
    // The IN subquery's reader reports no filter
    val query =
      s"""${cte}select l_linestatus, q from t
         |where l_returnflag = 'R'
         |  and q in (select q from t where l_returnflag = 'A')""".stripMargin
    bodyFilters(engine, query) shouldBe Nil
    rows(engine, query) shouldBe rows(disabled, query)
  }

  test("keep the body unfiltered when readers restrict different columns") {
    val query =
      s"""${cte}select l_linestatus, q from t where l_returnflag = 'R'
         |union all
         |select l_linestatus, q from t where l_linestatus = 'F'""".stripMargin
    bodyFilters(engine, query) shouldBe Nil
    sameRows(query)
  }

  test("filter a shared CTE read by another shared CTE by that CTE's filter") {
    val query =
      s"""${cte}, u as (
         |  select l_returnflag, l_linestatus, q * 2 as q2 from t where l_returnflag = 'N'
         |)
         |select a.l_linestatus, a.q2, b.q2
         |from u a join u b on a.l_linestatus = b.l_linestatus
         |join t c on c.l_linestatus = a.l_linestatus
         |where a.l_linestatus = 'O' and b.l_linestatus = 'O' and c.l_returnflag = 'N'"""
        .stripMargin
    val plan = explain(engine, query)
    // t's readers are u's body (l_returnflag = 'N') and c; u's are a and b (l_linestatus = 'O')
    plan shouldContain "lit(N:string)"
    plan shouldContain "lit(O:string)"
    sameRows(query)
  }

  test("keep the body unfiltered with push_down_cte_filters off") {
    val query =
      s"""${cte}select a.l_linestatus, a.q, b.q
         |from t a join t b on a.l_linestatus = b.l_linestatus
         |where a.l_returnflag = 'R' and b.l_returnflag = 'A'""".stripMargin
    bodyFilters(disabled, query) shouldBe Nil
  }

end EngineSharedCTEFiltersTest
