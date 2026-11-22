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
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.Planner
import wvlet.lang.engine.plan.SourceResolver
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.runtime.*
import wvlet.lang.engine.vector.PType
import wvlet.lang.model.plan.RawSQL
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** The ReorderJoins pass over runtime plans of the TPC-H sample data */
class EngineReorderJoinsTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val engine  = WvletEngine(optimizer = options)
  EngineTPCHBench
    .tables
    .foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def relation(query: String): (Relation, Context) =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    (GenSQL.expand(q, summon[Context]), r.context)

  private def plan(query: String): RuntimePlan =
    val (r, ctx) = relation(query)
    engine.runtimePlan(r)(using ctx)

  /** One planner for the inputs of a hand-built join, so that their column ids are distinct */
  private val leaves: Seq[RuntimePlan] =
    val rs        = Seq("from region", "from nation", "from supplier").map(relation)
    given Context = rs.head._2
    val planner   = Planner(
      engine.catalog,
      new SourceResolver:
        override def tableSource(name: String, fullName: String): Option[TableSource] = None
        override def fileSource(path: String): Option[TableSource]                    = None
        override def parseSql(r: RawSQL): Relation = fail("No SQL")
      ,
      options
    )
    rs.map((r, _) => planner.runtimePlan(r))

  private def column(p: RuntimePlan, name: String): BExpr = BExpr.ref(
    p.output.find(_.name == name).getOrElse(fail(s"No ${name} in ${p.output}"))
  )

  private def eq(l: BExpr, r: BExpr): JoinCondition = JoinCondition(
    BExpr(Compare(CmpOp.Eq, l.expr, r.expr))
  )

  // region (5 rows) <- nation (25) <- supplier, with a condition on region
  private val Seq(region, nation, supplier) = leaves: @unchecked
  private val joins                         = RuntimePlan.InnerJoins(
    IndexedSeq(region, nation, supplier),
    IndexedSeq(
      eq(column(nation, "n_regionkey"), column(region, "r_regionkey")),
      eq(column(supplier, "s_nationkey"), column(nation, "n_nationkey")), {
        val asia = Compare(CmpOp.Eq, column(region, "r_name").expr, Const("ASIA", PType.Str))
        JoinCondition(BExpr(asia), Map(asia.sig -> 1))
      }
    )
  )

  private def nodes(p: RuntimePlan): List[RuntimePlan] = p :: p.children.toList.flatMap(nodes)

  /** The input the bottom join of a left-deep tree starts from */
  private def start(p: RuntimePlan): RuntimePlan =
    p match
      case j: RuntimePlan.Join =>
        start(j.left)
      case f: RuntimePlan.Filter =>
        start(f.child)
      case other =>
        other

  test("join every input once on its equalities") {
    val o       = Optimizer.only(ReorderJoins, options)
    val ordered = o.run(joins)
    debug(ordered.explain)
    nodes(ordered).collect { case j: RuntimePlan.InnerJoins =>
      j
    } shouldBe Nil
    val js = nodes(ordered).collect { case j: RuntimePlan.Join =>
      j
    }
    js.size shouldBe 2
    js.foreach { j =>
      j.kind shouldBe JoinKind.Inner
      j.leftKeys.size shouldBe 1
    }
    Seq(region, nation, supplier).foreach(in => nodes(ordered).count(_ eq in) shouldBe 1)
    // The same columns, in join order
    ordered.output.toSet shouldBe joins.output.toSet
    o.firedRules shouldBe Map("reorder_joins" -> 1)
  }

  test("filter an input by its single-input conditions") {
    val ordered = Optimizer.only(ReorderJoins, options).run(joins)
    val pushed  = nodes(ordered).collect {
      case f: RuntimePlan.Filter if f.child eq region =>
        f
    }
    pushed.size shouldBe 1
    pushed.head.cond.toString shouldContain "ASIA"
  }

  test("join in written order without reorder_joins") {
    val o       = Optimizer.only(ReorderJoins, options.withDisabled(Optimization.ReorderJoins))
    val ordered = o.run(joins)
    start(ordered) shouldBeTheSameInstanceAs region
    nodes(ordered).count(_.isInstanceOf[RuntimePlan.Join]) shouldBe 2
    o.firedRules shouldBe Map.empty
  }

  test("reject unordered joins in lowering") {
    val e = intercept[wvlet.lang.api.WvletLangException] {
      Lowering(joins)
    }
    e.getMessage shouldContain "ReorderJoins"
  }

  test("order the inner joins of queries in the default pipeline") {
    val p = plan("""from supplier, nation, region
        |where s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA'
        |select s_name, n_name""".stripMargin)
    debug(p.explain)
    nodes(p).count(_.isInstanceOf[RuntimePlan.InnerJoins]) shouldBe 0
    nodes(p).count(_.isInstanceOf[RuntimePlan.Join]) shouldBe 2
  }

end EngineReorderJoinsTest
