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
import wvlet.lang.engine.runtime.*
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.plan.PlannerMode
import wvlet.lang.engine.vector.PType
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** The runtime binder, the plan validator, and lowering, over the TPC-H sample data */
class EngineRuntimePlanTest extends UniTest:
  private val runtime = OptimizerOptions().withPlanner(PlannerMode.Runtime).withValidatePlans(true)
  private val engine  = WvletEngine(optimizer = runtime)
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

  private def runtimePlan(query: String): RuntimePlan = withQuery(query)(engine.runtimePlan(_))

  test("bind a filter, projection, and top-n sort over a scan") {
    val p = runtimePlan("""from lineitem
        |where l_quantity > 10 and l_returnflag = 'R'
        |select l_orderkey, l_quantity
        |order by l_orderkey desc
        |limit 3""".stripMargin)
    debug(p.explain)
    p match
      case RuntimePlan.Sort(
            RuntimePlan.Project(RuntimePlan.Filter(s: RuntimePlan.Scan, _), _, cols),
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
    val p = runtimePlan("from nation select n_name")
    p.explain.linesIterator.toSeq.head shouldMatch { case s"Scan[nation] -> [n_nationkey#$_" =>
    }
    p.explain shouldContain "Project(#"
  }

  test("keep relations the binder doesn't cover as legacy leaves") {
    val p = runtimePlan("from nation select distinct n_regionkey")
    p match
      case RuntimePlan.Legacy(op, cols) =>
        cols.map(_.name) shouldBe op.schema.names
      case other =>
        fail(s"Unexpected plan:\n${other.explain}")
  }

  private def joins(p: RuntimePlan): Seq[RuntimePlan.Join] =
    p match
      case j: RuntimePlan.Join =>
        j +: p.children.flatMap(joins)
      case RuntimePlan.Legacy(_, _) =>
        Nil
      case _ =>
        p.children.flatMap(joins)

  test("bind an inner join tree with its filters to joins over scans") {
    val p = runtimePlan("""from nation, region
        |where n_regionkey = r_regionkey and r_name = 'ASIA'
        |select n_name, r_name""".stripMargin)
    debug(p.explain)
    val js = joins(p)
    js.size shouldBe 1
    val j = js.head
    j.kind shouldBe wvlet.lang.engine.exec.JoinKind.Inner
    j.leftKeys.size shouldBe 1
    // The single-input condition is pushed below the join
    j.children
      .collect { case f: RuntimePlan.Filter =>
        f
      }
      .size shouldBe 1
    // The keys read the inputs' columns by id
    j.leftKeys.head.refs.subsetOf(j.left.output.map(_.id).toSet) shouldBe true
    j.rightKeys.head.refs.subsetOf(j.right.output.map(_.id).toSet) shouldBe true
    PlanValidator.violations(p) shouldBe Nil
  }

  test("bind a left join with a residual condition") {
    val p = runtimePlan("""from customer
        |left join orders on c_custkey = o_custkey and o_comment not like '%special%'
        |select c_custkey, o_orderkey""".stripMargin)
    debug(p.explain)
    val js = joins(p)
    js.size shouldBe 1
    js.head.kind shouldBe wvlet.lang.engine.exec.JoinKind.Left
    js.head.residual.isDefined shouldBe true
    PlanValidator.violations(p) shouldBe Nil
  }

  test("bind the joins under an aggregation") {
    val p = runtimePlan("""from lineitem, orders
        |where l_orderkey = o_orderkey and o_orderstatus = 'F'
        |group by o_orderpriority
        |select o_orderpriority, count(*)""".stripMargin)
    debug(p.explain)
    p match
      case RuntimePlan.Project(RuntimePlan.Aggregate(j: RuntimePlan.Join, keys, calls, _), _, _) =>
        keys.size shouldBe 1
        calls.map(_.name) shouldBe IndexedSeq("count_star")
        // The key reads o_orderpriority of the join's output by id
        keys.head.refs.subsetOf(j.output.map(_.id).toSet) shouldBe true
      case other =>
        fail(s"Unexpected plan:\n${other.explain}")
    PlanValidator.violations(p) shouldBe Nil
  }

  test("bind a HAVING condition as a filter over the aggregation") {
    val p = runtimePlan("""from lineitem
        |group by l_returnflag
        |where count(*) > 100
        |select l_returnflag, sum(l_quantity)""".stripMargin)
    debug(p.explain)
    p match
      case RuntimePlan.Project(RuntimePlan.Filter(a: RuntimePlan.Aggregate, cond), _, _) =>
        // The condition reads an aggregate of the aggregation's output
        cond.refs.subsetOf(a.output.map(_.id).toSet) shouldBe true
        a.calls.map(_.name).toSet shouldBe Set("count_star", "sum")
      case other =>
        fail(s"Unexpected plan:\n${other.explain}")
    PlanValidator.violations(p) shouldBe Nil
  }

  test("keep grouping sets as a legacy node over the bound input") {
    val p = runtimePlan("""from lineitem
        |group by rollup(l_returnflag, l_linestatus)
        |select l_returnflag, l_linestatus, count(*)""".stripMargin)
    debug(p.explain)
    p match
      case RuntimePlan.Project(RuntimePlan.Legacy(_, _), _, _) =>
      case other                                               =>
        fail(s"Unexpected plan:\n${other.explain}")
    PlanValidator.violations(p) shouldBe Nil
  }

  test("keep a join with a subquery condition as a legacy node over the joined inputs") {
    val p = runtimePlan("""from nation, region
        |where n_regionkey = r_regionkey
        |  and n_nationkey in (from supplier select s_nationkey)
        |select n_name""".stripMargin)
    debug(p.explain)
    p.explain shouldContain "Legacy["
    PlanValidator.violations(p) shouldBe Nil
  }

  test("order by a column outside the select list through a hidden select item") {
    val p = runtimePlan("from nation select n_name order by n_regionkey, n_name")
    p match
      case RuntimePlan.Project(RuntimePlan.Sort(proj: RuntimePlan.Project, keys, _), _, cols) =>
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
    val p   = runtimePlan("from nation select n_name")
    val bad =
      p match
        case RuntimePlan.Project(child, _, cols) =>
          RuntimePlan.Filter(child, BExpr(ColRef(cols.head.id.value + 100, PType.Bool, "missing")))
        case other =>
          fail(s"Unexpected plan:\n${other.explain}")
    PlanValidator.violations(bad).head shouldContain "doesn't output"
    intercept[wvlet.lang.api.WvletLangException] {
      PlanValidator.validate(bad)
    }
  }

end EngineRuntimePlanTest
