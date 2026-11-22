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
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.logical.*
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.plan.PlannerMode
import wvlet.lang.engine.vector.PType
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** The logical binder, the plan validator, and lowering, over the TPC-H sample data */
class EngineLogicalPlanTest extends UniTest:
  private val logical = OptimizerOptions().withPlanner(PlannerMode.Logical).withValidatePlans(true)
  private val engine  = WvletEngine(optimizer = logical)
  private val legacy  = engine.withOptimizer(OptimizerOptions())
  EngineTPCHBench
    .tables
    .foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    f(GenSQL.expand(q, summon[Context]))

  private def logicalPlan(query: String): LogicalPlan = withQuery(query)(engine.logicalPlan(_))

  test("bind a filter, projection, and top-n sort over a scan") {
    val p = logicalPlan("""from lineitem
        |where l_quantity > 10 and l_returnflag = 'R'
        |select l_orderkey, l_quantity
        |order by l_orderkey desc
        |limit 3""".stripMargin)
    debug(p.explain)
    p match
      case LogicalPlan.Sort(
            LogicalPlan.Project(LogicalPlan.Filter(s: LogicalPlan.Scan, _), _, cols),
            keys,
            limit
          ) =>
        s.source.name shouldBe "lineitem"
        cols.map(_.name) shouldBe IndexedSeq("l_orderkey", "l_quantity")
        keys.map(_.ascending) shouldBe IndexedSeq(false)
        // The sort key reads the projected l_orderkey by id
        keys.head.expr.refs shouldBe Set(cols.head.id)
        limit shouldBe Some(3L)
      case other =>
        fail(s"Unexpected plan:\n${other.explain}")
    PlanValidator.violations(p) shouldBe Nil
  }

  test("show column ids in the plan text") {
    val p = logicalPlan("from nation select n_name")
    p.explain.linesIterator.toSeq.head shouldMatch { case s"Scan[nation] -> [n_nationkey#$_" =>
    }
    p.explain shouldContain "Project(#"
  }

  test("keep relations the binder doesn't cover as legacy leaves") {
    val p = logicalPlan("from lineitem group by l_returnflag select l_returnflag, count(*)")
    p match
      case LogicalPlan.Legacy(op, cols) =>
        cols.map(_.name) shouldBe op.schema.names
      case other =>
        fail(s"Unexpected plan:\n${other.explain}")
  }

  test("order by a column outside the select list through a hidden select item") {
    val p = logicalPlan("from nation select n_name order by n_regionkey, n_name")
    p match
      case LogicalPlan.Project(LogicalPlan.Sort(proj: LogicalPlan.Project, keys, _), _, cols) =>
        cols.map(_.name) shouldBe IndexedSeq("n_name")
        proj.columns.size shouldBe 2
        keys.size shouldBe 2
      case other =>
        fail(s"Unexpected plan:\n${other.explain}")
  }

  test("return the same rows as the recursive planner") {
    val query =
      """from orders
        |where o_totalprice > 100000
        |select o_orderkey, o_custkey, o_totalprice
        |order by o_totalprice desc, o_orderkey
        |limit 10""".stripMargin
    def rows(e: WvletEngine): Seq[Seq[Any]] =
      withQuery(query) { q =>
        val r = e.execute(q)
        r.batches.flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toSeq))
      }
    val expected = rows(legacy)
    expected.size shouldBe 10
    rows(engine) shouldBe expected
  }

  test("report expressions that read columns their input doesn't output") {
    val p   = logicalPlan("from nation select n_name")
    val bad =
      p match
        case LogicalPlan.Project(child, _, cols) =>
          LogicalPlan.Filter(child, BExpr(ColRef(cols.head.id.value + 100, PType.Bool, "missing")))
        case other =>
          fail(s"Unexpected plan:\n${other.explain}")
    PlanValidator.violations(bad).head shouldContain "doesn't output"
    intercept[wvlet.lang.api.WvletLangException] {
      PlanValidator.validate(bad)
    }
  }

end EngineLogicalPlanTest
