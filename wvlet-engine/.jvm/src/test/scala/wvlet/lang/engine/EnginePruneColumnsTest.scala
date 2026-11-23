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
import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.plan.Planner
import wvlet.lang.engine.plan.SourceResolver
import wvlet.lang.engine.runtime.*
import wvlet.lang.model.plan.RawSQL
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** The PruneColumns pass, and shared CTEs computing only the columns their readers read */
class EnginePruneColumnsTest extends UniTest:
  import RuntimePlan.*

  private val options  = OptimizerOptions().withValidatePlans(true)
  private val engine   = WvletEngine(optimizer = options)
  private val disabled = engine.withOptimizer(options.withDisabled(Optimization.PruneColumns))
  EngineTPCHBench
    .tables
    .foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](unit: CompilationUnit)(f: Context ?=> Relation => A): A =
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${unit}"))
    f(GenSQL.expand(q, summon[Context]))

  private def wv(query: String)  = CompilationUnit.fromWvletString(query)
  private def sql(query: String) = CompilationUnit.fromSqlString(query)

  /** The plan as the binder builds it, before the passes */
  private def bound(query: String): RuntimePlan =
    withQuery(wv(query)) { r =>
      val planner = Planner(
        engine.catalog,
        new SourceResolver:
          override def tableSource(name: String, fullName: String): Option[TableSource] = None
          override def fileSource(path: String): Option[TableSource]                    = None
          override def parseSql(r: RawSQL): Relation = fail("No SQL")
        ,
        options
      )
      RuntimeBinder(planner).bind(r)
    }

  private def pruned(p: RuntimePlan): (RuntimePlan, Optimizer) =
    val opt = Optimizer.only(PruneColumns, options)
    val out = opt.run(p)
    debug(out.explain)
    out.output shouldBe p.output
    (out, opt)

  private def explain(query: String): String =
    val plan = withQuery(sql(query))(q => engine.explain(q))
    debug(plan)
    plan

  /** The lines of the shared CTE body in a physical plan */
  private def sharedBody(plan: String): Seq[String] =
    plan
      .linesIterator
      .dropWhile(!_.trim.startsWith("(shared)"))
      .drop(1)
      .takeWhile(_.startsWith("    "))
      .toSeq

  private def rows(e: WvletEngine, query: String): Seq[String] =
    withQuery(sql(query)) { q =>
      e.execute(q)
        .batches
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).mkString("|")))
        .sorted
    }

  /** The query returns the same rows with pruning and without it */
  private def sameRows(query: String): Unit =
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)

  test("drop projected expressions that nothing reads") {
    val (p, o) = pruned(bound("""from lineitem
              |select l_orderkey, l_quantity + 1 as q
              |select l_orderkey""".stripMargin))
    p shouldMatch { case Project(Project(_: Scan, exprs, _), _, _) =>
      exprs.size shouldBe 1
    }
    o.firedRules shouldBe Map("prune_columns" -> 1)
  }

  test("keep the calls of an aggregation") {
    val (p, _) = pruned(bound("""from lineitem
              |group by l_orderkey
              |agg l_quantity.sum as s, l_extendedprice.max as m
              |select l_orderkey""".stripMargin))
    def calls(p: RuntimePlan): Option[Int] =
      p match
        case Aggregate(_, _, calls, _) =>
          Some(calls.size)
        case other =>
          other.children.iterator.flatMap(calls).nextOption()
    calls(p) shouldBe Some(2)
  }

  test("leave a plan unchanged when every column is read") {
    val p      = bound("from nation select n_nationkey, n_name")
    val (q, o) = pruned(p)
    q shouldBe p
    o.firedRules shouldBe Map.empty
  }

  private val cte =
    """with t as (
      |  select l_orderkey, l_partkey, l_suppkey, l_quantity, l_comment from lineitem
      |)
      |""".stripMargin

  test("compute a shared CTE with only the columns its readers read") {
    val query =
      s"""${cte}select a.l_orderkey, b.l_partkey
         |from t a join t b on a.l_orderkey = b.l_orderkey and a.l_suppkey = b.l_suppkey
         |where a.l_partkey < 100""".stripMargin
    val body = sharedBody(explain(query)).mkString("\n")
    body shouldContain "columns=l_orderkey,l_partkey,l_suppkey)"
    sameRows(query)
  }

  test("include the columns read by readers the recursive planner plans") {
    // The IN subquery is planned by the recursive planner
    val query =
      s"""${cte}select l_orderkey from t
         |where l_partkey in (select l_quantity from t)""".stripMargin
    val body = sharedBody(explain(query)).mkString("\n")
    body shouldContain "columns=l_orderkey,l_partkey,l_quantity)"
    sameRows(query)
  }

  test("prune a CTE read by another shared CTE") {
    val query =
      s"""${cte}, u as (
         |  select l_orderkey, l_partkey, l_quantity * 2 as q from t where l_suppkey < 50
         |)
         |select a.l_orderkey, b.q
         |from u a join u b on a.l_orderkey = b.l_orderkey
         |join t c on c.l_orderkey = a.l_orderkey""".stripMargin
    val plan = explain(query)
    plan shouldContain "columns=l_orderkey,l_suppkey,l_quantity)"
    plan shouldNotContain "l_comment"
    sameRows(query)
  }

  test("compute every column of a CTE read while planning") {
    // The scalar subquery runs at plan time, before the other readers are planned
    val query =
      s"""${cte}select l_orderkey from t
         |where l_quantity = (select max(l_quantity) from t)""".stripMargin
    sharedBody(explain(query)).mkString("\n") shouldContain "l_comment"
    sameRows(query)
  }

end EnginePruneColumnsTest
