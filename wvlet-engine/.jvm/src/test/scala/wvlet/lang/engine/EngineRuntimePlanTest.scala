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
            RuntimePlan.Project(RuntimePlan.Filter(s: RuntimePlan.Scan, _, _), _, cols),
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
    p.explain.linesIterator.toSeq.head shouldMatch { case s"Scan[nation] -> [n_name#$_" =>
    }
    p.explain shouldContain "Project(#"
  }

  test("keep relations the binder doesn't cover as legacy leaves") {
    // A pivot
    val p = runtimePlan("from nation pivot on n_regionkey in (0, 1) agg _.count")
    p match
      case RuntimePlan.Legacy(op, cols) =>
        cols.map(_.name) shouldBe op.schema.names
      case other =>
        fail(s"Unexpected plan:\n${other.explain}")
  }

  test("bind a projection with windows as a window under a projection") {
    val p = runtimePlan(
      "from nation select n_name, row_number() over (partition by n_regionkey) as rn"
    )
    p shouldMatch { case RuntimePlan.Project(w: RuntimePlan.Window, _, cols) =>
      cols.map(_.name) shouldBe IndexedSeq("n_name", "rn")
      w.calls.map(_.name) shouldBe IndexedSeq("row_number")
      w.calls.head.partitionColumns shouldBe Set(w.child.output(2).id)
    }
    PlanValidator.violations(p) shouldBe Nil
  }

  test("bind set operations and distinct as runtime-plan nodes") {
    val p = runtimePlan("""from nation
        |select n_regionkey
        |concat {
        |  from region
        |  select r_regionkey
        |}
        |dedup""".stripMargin)
    p shouldMatch { case RuntimePlan.Distinct(u: RuntimePlan.Union) =>
      u.columns.map(_.name) shouldBe IndexedSeq("n_regionkey")
    }
    PlanValidator.violations(p) shouldBe Nil
  }

  private def rowsOf(e: WvletEngine, query: String): Seq[Seq[Any]] =
    withQuery(query) { q =>
      val r = e.execute(q)
      r.batches.flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toSeq))
    }

  test("bind a sample of rows as a limit and a percentage sample as a sample node") {
    runtimePlan("from nation sample 5") shouldMatch {
      case RuntimePlan.Limit(_: RuntimePlan.Scan, 5L, 0L) =>
    }
    rowsOf(engine, "from nation sample 5") shouldBe rowsOf(legacy, "from nation sample 5")

    // A filter over the sample stays above it: moving it changes which rows the generator sees
    val query =
      """from lineitem
        |sample 10%
        |where l_quantity > 10
        |select l_orderkey, l_linenumber""".stripMargin
    val p = runtimePlan(query)
    debug(p.explain)
    p shouldMatch {
      case RuntimePlan.Project(RuntimePlan.Filter(RuntimePlan.Sample(_, 10.0), _, _), _, _) =>
    }
    PlanValidator.violations(p) shouldBe Nil
    val expected = rowsOf(legacy, query)
    expected.size > 0 shouldBe true
    rowsOf(engine, query) shouldBe expected
  }

  private def nodes(p: RuntimePlan): Seq[RuntimePlan] = p +: p.children.flatMap(nodes)

  private def unnests(p: RuntimePlan): Seq[RuntimePlan.Unnest] = nodes(p).collect {
    case u: RuntimePlan.Unnest =>
      u
  }

  private def legacyLeafCount(p: RuntimePlan): Int = nodes(p).count {
    case _: RuntimePlan.Legacy =>
      true
    case _ =>
      false
  }

  test("bind unnest as a relation over one row") {
    val query = "from unnest([1, 2, 3]) as t(number)"
    val p     = runtimePlan(query)
    debug(p.explain)
    legacyLeafCount(p) shouldBe 0
    unnests(p) shouldMatch {
      case Seq(RuntimePlan.Unnest(RuntimePlan.Values(cols, _), _, false, false, _)) =>
        cols shouldBe IndexedSeq.empty
    }
    p.output.map(_.name) shouldBe IndexedSeq("number")
    PlanValidator.violations(p) shouldBe Nil
    rowsOf(engine, query) shouldBe Seq(Seq(1L), Seq(2L), Seq(3L))
    rowsOf(engine, query) shouldBe rowsOf(legacy, query)
  }

  test("bind a lateral unnest and move filters on its input below it") {
    val query =
      """from [[1, [10, 20]], [2, [30, 40]], [3, null]] as t(id, xs)
        |cross join unnest(xs) as u(x)
        |where id >= 2 and x > 30
        |select id, x""".stripMargin
    val p = runtimePlan(query)
    debug(p.explain)
    legacyLeafCount(p) shouldBe 0
    val Seq(u) = unnests(p)
    u.keepInput shouldBe true
    u.outer shouldBe false
    // id >= 2 reads only the input, and filters it; x > 30 stays above the unnest
    nodes(u.child).exists(_.isInstanceOf[RuntimePlan.Filter]) shouldBe true
    nodes(p).exists {
      case RuntimePlan.Filter(_: RuntimePlan.Unnest, _, _) =>
        true
      case _ =>
        false
    } shouldBe true
    PlanValidator.violations(p) shouldBe Nil
    rowsOf(engine, query) shouldBe Seq(Seq(2L, 40L))
    rowsOf(engine, query) shouldBe rowsOf(legacy, query)

    // LEFT JOIN ... ON true keeps rows without elements
    val left =
      """from [[1, [1, 2]], [2, []], [3, null]] as t(id, xs)
        |left join unnest(xs) as u(x) on true
        |where id > 1
        |select id, x
        |order by id""".stripMargin
    val l = runtimePlan(left)
    unnests(l).map(_.outer) shouldBe Seq(true)
    legacyLeafCount(l) shouldBe 0
    PlanValidator.violations(l) shouldBe Nil
    rowsOf(engine, left) shouldBe Seq(Seq(2L, null), Seq(3L, null))
    rowsOf(engine, left) shouldBe rowsOf(legacy, left)

    // Inside a join tree, with a condition between the unnest and another input
    val tree =
      """from [[1, [10, 20]], [2, [30]]] as t(id, xs)
        |cross join unnest(xs) as u(x)
        |join nation on n_nationkey = t.id
        |select id, x, n_name
        |order by id, x""".stripMargin
    val j = runtimePlan(tree)
    debug(j.explain)
    legacyLeafCount(j) shouldBe 0
    unnests(j).size shouldBe 1
    PlanValidator.violations(j) shouldBe Nil
    rowsOf(engine, tree).size shouldBe 3
    rowsOf(engine, tree) shouldBe rowsOf(legacy, tree)
  }

  test("bind unnest select items as a projection over an unnest") {
    val query =
      """from nation
        |where n_nationkey < 3
        |select *, unnest([n_regionkey, n_nationkey * 10]) as v
        |order by n_nationkey, v""".stripMargin
    val p = runtimePlan(query)
    debug(p.explain)
    legacyLeafCount(p) shouldBe 0
    nodes(p).exists {
      case RuntimePlan.Project(
            RuntimePlan.Unnest(_: RuntimePlan.Project, _, true, false, _),
            _,
            _
          ) =>
        true
      case _ =>
        false
    } shouldBe true
    p.output.map(_.name).last shouldBe "v"
    PlanValidator.violations(p) shouldBe Nil
    val expected = rowsOf(legacy, query)
    expected.size shouldBe 6
    rowsOf(engine, query) shouldBe expected
  }

  test("bind column edits as projections over their input") {
    val query =
      """from nation
        |add n_nationkey + 1 as k, upper(n_name) as n_name
        |prepend n_regionkey * 10 as r10
        |exclude n_comment
        |rename n_nationkey as id
        |shift k
        |order by id
        |limit 3""".stripMargin
    val p = runtimePlan(query)
    debug(p.explain)
    def legacyLeaves(p: RuntimePlan): Int =
      p match
        case RuntimePlan.Legacy(_, _) =>
          1
        case _ =>
          p.children.map(legacyLeaves).sum
    legacyLeaves(p) shouldBe 0
    p.output.map(_.name) shouldBe IndexedSeq("k", "r10", "id", "n_name", "n_regionkey")
    PlanValidator.violations(p) shouldBe Nil
    val expected = rowsOf(legacy, query)
    expected.size shouldBe 3
    rowsOf(engine, query) shouldBe expected
  }

  test("bind a column edit with window functions over a window") {
    val query =
      """from nation
        |add row_number() over (partition by n_regionkey order by n_nationkey) as rn
        |order by n_nationkey
        |limit 5""".stripMargin
    val p = runtimePlan(query)
    p shouldMatch {
      case RuntimePlan.Sort(RuntimePlan.Project(w: RuntimePlan.Window, _, cols), _, _) =>
        cols.map(_.name).last shouldBe "rn"
        w.calls.map(_.name) shouldBe IndexedSeq("row_number")
    }
    PlanValidator.violations(p) shouldBe Nil
    rowsOf(engine, query) shouldBe rowsOf(legacy, query)
  }

  test("bind describe and a query without from as values") {
    val d = runtimePlan("from nation describe")
    d shouldMatch { case RuntimePlan.Values(cols, rows) =>
      cols.map(_.name) shouldBe IndexedSeq("column_name", "column_type")
      rows.size shouldBe 4
    }
    rowsOf(engine, "from nation describe") shouldBe rowsOf(legacy, "from nation describe")

    val one = runtimePlan("select 1 + 2 as x")
    one shouldMatch { case RuntimePlan.Project(RuntimePlan.Values(cols, rows), _, _) =>
      cols shouldBe IndexedSeq.empty
      rows.size shouldBe 1
    }
    rowsOf(engine, "select 1 + 2 as x") shouldBe Seq(Seq(3L))
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
      case RuntimePlan.Project(RuntimePlan.Filter(a: RuntimePlan.Aggregate, cond, _), _, _) =>
        // The condition reads an aggregate of the aggregation's output
        cond.refs.subsetOf(a.output.map(_.id).toSet) shouldBe true
        a.calls.map(_.name).toSet shouldBe Set("count_star", "sum")
      case other =>
        fail(s"Unexpected plan:\n${other.explain}")
    PlanValidator.violations(p) shouldBe Nil
  }

  test("bind a count of groups as a global count over the aggregation") {
    val query = "from lineitem group by l_returnflag, l_linestatus count"
    val p     = runtimePlan(query)
    debug(p.explain)
    p match
      case RuntimePlan.Aggregate(
            RuntimePlan.Project(g: RuntimePlan.Aggregate, _, _),
            keys,
            calls,
            _
          ) =>
        // A global count over the groups
        keys.isEmpty shouldBe true
        calls.map(_.name) shouldBe IndexedSeq("count_star")
        g.keys.size shouldBe 2
      case other =>
        fail(s"Unexpected plan:\n${other.explain}")
    PlanValidator.violations(p) shouldBe Nil
    def rows(e: WvletEngine): Seq[Seq[Any]] =
      withQuery(query) { q =>
        val r = e.execute(q)
        r.batches.flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toSeq))
      }
    val expected = rows(legacy)
    expected shouldBe Seq(Seq(4L))
    rows(engine) shouldBe expected
  }

  test("bind grouping sets as a union of aggregations over a shared input") {
    val p = runtimePlan("""from lineitem
        |group by rollup(l_returnflag, l_linestatus)
        |select l_returnflag, l_linestatus, count(*)""".stripMargin)
    debug(p.explain)
    p match
      case RuntimePlan.Project(RuntimePlan.Union(sets, _), _, _) =>
        sets.size shouldBe 3
        // Each set groups by its non-constant keys over a reader of the shared input; the grand
        // total is a global aggregation
        sets.map {
          case RuntimePlan.Project(
                RuntimePlan.Aggregate(r: RuntimePlan.CTERef, keys, _, _),
                _,
                _
              ) =>
            r.cte.name shouldBe "GroupingSetInput"
            keys.size
          case other =>
            fail(s"Unexpected grouping set:\n${other.explain}")
        } shouldBe IndexedSeq(2, 1, 0)
      case other =>
        fail(s"Unexpected plan:\n${other.explain}")
    p.explain shouldNotContain "Legacy["
    PlanValidator.violations(p) shouldBe Nil
  }

  test("apply a subquery condition over a join tree as a semi join over the ordered tree") {
    val p = runtimePlan("""from nation, region
        |where n_regionkey = r_regionkey
        |  and n_nationkey in (from supplier select s_nationkey)
        |select n_name""".stripMargin)
    debug(p.explain)
    p.explain shouldContain "Join[Semi]"
    p.explain shouldNotContain "Legacy["
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
