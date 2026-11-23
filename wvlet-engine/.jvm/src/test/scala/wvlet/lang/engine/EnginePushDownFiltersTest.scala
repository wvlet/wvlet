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
import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.plan.Planner
import wvlet.lang.engine.plan.SourceResolver
import wvlet.lang.engine.runtime.*
import wvlet.lang.model.plan.RawSQL
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** The PushDownFilters pass over runtime plans of the TPC-H sample data */
class EnginePushDownFiltersTest extends UniTest:
  import RuntimePlan.*

  private val options  = OptimizerOptions().withValidatePlans(true)
  private val engine   = WvletEngine(optimizer = options)
  private val disabled = engine.withOptimizer(options.withDisabled(Optimization.PushDownFilters))
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

  /** The plan as the binder builds it, before the passes */
  private def bound(query: String): RuntimePlan =
    withQuery(query) { r =>
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

  private def pushed(p: RuntimePlan, o: OptimizerOptions = options): (RuntimePlan, Optimizer) =
    val opt = Optimizer.only(PushDownFilters, o)
    val out = opt.run(p)
    debug(out.explain)
    out.output shouldBe p.output
    (out, opt)

  private def find[A](p: RuntimePlan)(f: PartialFunction[RuntimePlan, A]): Option[A] = f
    .lift(p)
    .orElse(p.children.iterator.flatMap(find(_)(f)).nextOption())

  private def count(e: WvletEngine, query: String): Long =
    withQuery(query)(q => e.execute(q).batches.map(_.size.toLong).sum)

  /** The query returns the same rows with the pass and without it */
  private def sameRows(query: String): Unit = count(engine, query) shouldBe count(disabled, query)

  test("push a filter through a projection to the scan") {
    val query =
      """from lineitem
        |select l_orderkey, l_quantity + 1 as q
        |where q > 10""".stripMargin
    val (p, o) = pushed(bound(query))
    p shouldMatch { case Project(Filter(_: Scan, cond, _), _, _) =>
      cond.toString shouldContain "+"
    }
    o.firedRules shouldBe Map("push_down_filters" -> 1)
    sameRows(query)
  }

  test("push only conjuncts over grouping keys below an aggregation") {
    val query =
      """from lineitem
        |group by l_returnflag
        |agg _.count as n
        |where l_returnflag = 'R' and n > 10""".stripMargin
    val (p, _) = pushed(bound(query))
    // The key conjunct filters the aggregation input; the one over the count stays above
    find(p) { case Aggregate(Filter(_, cond, _), _, _, _) =>
      cond.toString shouldContain "lit(R"
    } shouldNotBe None
    find(p) { case Filter(_: Aggregate, cond, _) =>
      cond.toString shouldNotContain "lit(R"
    } shouldNotBe None
    sameRows(query)
  }

  test("keep a filter with a non-deterministic conjunct in place") {
    val query =
      """from lineitem
        |group by l_returnflag
        |agg _.count as n
        |where l_returnflag = 'R' and random() < 2.0""".stripMargin
    val (p, o) = pushed(bound(query))
    find(p) { case Aggregate(_: Filter, _, _, _) =>
      ()
    } shouldBe None
    o.firedRules shouldBe Map.empty
  }

  test("push conjuncts over the preserved input of a left join, and extract its keys") {
    val query =
      """from orders
        |left join customer on o_custkey = c_custkey
        |where o_orderstatus = 'F' and c_mktsegment = 'BUILDING'
        |select o_orderkey""".stripMargin
    val b = bound(query)
    find(b) { case j: Join =>
      j.leftKeys.isEmpty shouldBe true
      j.residual shouldNotBe None
    } shouldNotBe None
    val (p, _) = pushed(b)
    find(p) {
      case Filter(Join(Filter(_: Scan, l, _), _, JoinKind.Left, lk, _, residual, _), c, _) =>
        l.toString shouldContain "lit(F"
        c.toString shouldContain "BUILDING"
        lk.size shouldBe 1
        residual shouldBe None
    } shouldNotBe None
    sameRows(query)
  }

  test("push conjuncts over the preserved input of a right join") {
    val query =
      """from customer
        |right join orders on c_custkey = o_custkey
        |where o_orderstatus = 'F' and c_mktsegment = 'BUILDING'
        |select o_orderkey""".stripMargin
    val (p, _) = pushed(bound(query))
    find(p) { case Filter(Join(_: Scan, Filter(_: Scan, r, _), JoinKind.Right, _, _, _, _), c, _) =>
      r.toString shouldContain "lit(F"
      c.toString shouldContain "BUILDING"
    } shouldNotBe None
    sameRows(query)
  }

  test("keep conjuncts over a full join") {
    val query =
      """from customer
        |full join orders on c_custkey = o_custkey
        |where o_orderstatus = 'F'
        |select o_orderkey""".stripMargin
    val (p, _) = pushed(bound(query))
    find(p) { case Filter(Join(_: Scan, _: Scan, JoinKind.Full, lk, _, _, _), _, _) =>
      lk.size shouldBe 1
    } shouldNotBe None
    sameRows(query)
  }

  test("move conjuncts below a left join into the inner join tree on its left") {
    val query =
      """from orders
        |join customer on o_custkey = c_custkey
        |left join nation on c_nationkey = n_nationkey
        |where c_mktsegment = 'BUILDING'
        |select o_orderkey""".stripMargin
    val (p, _) = pushed(bound(query))
    val conds  = find(p) { case j: InnerJoins =>
      j.conds
    }.getOrElse(fail(s"No inner joins in\n${p.explain}"))
    val segment = conds.find(_.cond.toString.contains("BUILDING")).getOrElse(fail("not pushed"))
    // The literal count survives, for the input's selectivity
    segment.literals shouldBe Some(1)
    sameRows(query)
  }

  test("infer equalities to constants across an inner join") {
    val query =
      """from orders
        |join customer on o_custkey = c_custkey
        |where o_custkey = 100
        |select o_orderkey""".stripMargin
    val b     = bound(query)
    val conds =
      find(pushed(b)._1) { case j: InnerJoins =>
        j.conds
      }.get
    val inferred = conds.filter(c => c.cond.toString.contains("lit(100"))
    inferred.size shouldBe 2
    inferred.forall(_.literals == Some(1)) shouldBe true
    // A second run adds nothing
    val again =
      find(pushed(pushed(b)._1)._1) { case j: InnerJoins =>
        j.conds
      }.get
    again.size shouldBe conds.size
    sameRows(query)
  }

  test("keep conjuncts in place when the pass is disabled, but still extract outer join keys") {
    val query =
      """from orders
        |left join customer on o_custkey = c_custkey
        |where o_orderstatus = 'F'
        |select o_orderkey""".stripMargin
    val (p, o) = pushed(bound(query), options.withDisabled(Optimization.PushDownFilters))
    find(p) { case Filter(Join(_: Scan, _, JoinKind.Left, lk, _, _, _), _, _) =>
      lk.size shouldBe 1
    } shouldNotBe None
    o.firedRules shouldBe Map.empty
  }

  test("choose the build side of an outer join after the inputs are optimized") {
    // orders has 10x the rows of customer: the hash table holds customer
    val query =
      """from customer
        |left join orders on c_custkey = o_custkey
        |select c_custkey, o_orderkey""".stripMargin
    val p = withQuery(query)(engine.runtimePlan(_))
    find(p) { case j: Join =>
      j.kind shouldBe JoinKind.Left
      j.buildLeft shouldBe true
    } shouldNotBe None
  }

  /** Whether every input of the set operation at the top of p filters its rows */
  private def filtersEveryInput(p: RuntimePlan): Boolean =
    def filtered(i: RuntimePlan): Boolean =
      find(i) { case f: Filter =>
        f
      }.isDefined
    find(p) {
      case u: Union =>
        u.inputs.forall(filtered)
      case s: SetOp =>
        filtered(s.left) && filtered(s.right)
    }.getOrElse(false)

  test("push a filter into every input of a concat, cast to the output type") {
    // o_shippriority is an int, l_linenumber a bigint
    val query =
      """from lineitem
        |select l_linenumber as n
        |concat {
        |  from orders
        |  select o_shippriority as n
        |}
        |where n < 2""".stripMargin
    val b      = bound(query)
    val (p, o) = pushed(b)
    p shouldMatch { case u: Union =>
      u.inputs.exists(_.output.head.ptype != u.columns.head.ptype) shouldBe true
    }
    filtersEveryInput(p) shouldBe true
    o.firedRules.get("push_down_filters").isDefined shouldBe true
    sameRows(query)
  }

  test("push a filter below a dedup") {
    val query =
      """from orders
        |select o_orderstatus
        |dedup
        |where o_orderstatus = 'F'""".stripMargin
    val (p, _) = pushed(bound(query))
    p shouldMatch { case Distinct(Project(_: Filter, _, _)) =>
    }
    sameRows(query)
  }

  test("push a filter into both inputs of intersect and except") {
    for op <- Seq("intersect", "intersect all", "except", "except all") do
      val query =
        s"""from orders
           |select o_custkey as k
           |${op} {
           |  from customer
           |  select c_custkey as k
           |}
           |where k < 100""".stripMargin
      val (p, _) = pushed(bound(query))
      p shouldMatch { case _: SetOp =>
      }
      filtersEveryInput(p) shouldBe true
      sameRows(query)
  }

  test("push single-input join conditions into the inputs of a set operation") {
    val query =
      """from {
        |  from orders
        |  select o_custkey as k
        |  concat {
        |    from customer
        |    select c_custkey as k
        |  }
        |} as u
        |join customer on u.k = c_custkey
        |where u.k < 100
        |select c_name""".stripMargin
    val p = withQuery(query)(engine.runtimePlan(_))
    filtersEveryInput(p) shouldBe true
    sameRows(query)
  }

end EnginePushDownFiltersTest
