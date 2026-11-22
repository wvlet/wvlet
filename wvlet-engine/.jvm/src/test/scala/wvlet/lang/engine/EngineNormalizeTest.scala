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
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.OrExpr
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.runtime.*
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** The optimizer pipeline and the Normalize pass over runtime plans of the TPC-H sample data */
class EngineNormalizeTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  // Bind without the rule, so that each test runs the pass alone
  private val unoptimized = WvletEngine(optimizer = options.withDisabled(Optimization.FactorOr))
  private val engine      = unoptimized.withOptimizer(options)
  EngineTPCHBench
    .tables
    .foreach(t => unoptimized.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    f(GenSQL.expand(q, summon[Context]))

  private def bound(query: String): RuntimePlan = withQuery(query)(unoptimized.runtimePlan(_))

  private def filterOf(p: RuntimePlan): RuntimePlan.Filter =
    p match
      case f: RuntimePlan.Filter =>
        f
      case other =>
        other.children.map(filterOf).headOption.getOrElse(fail(s"No filter in\n${p.explain}"))

  private def conjuncts(e: wvlet.lang.engine.expr.VExpr): List[wvlet.lang.engine.expr.VExpr] =
    e match
      case AndExpr(l, r) =>
        conjuncts(l) ++ conjuncts(r)
      case other =>
        List(other)

  test("factor a conjunct common to every branch of an or") {
    val p = bound("""from lineitem
        |where (l_returnflag = 'R' and l_quantity > 10) or (l_returnflag = 'R' and l_discount < 0.05)
        |select l_orderkey""".stripMargin)
    filterOf(p).cond.expr shouldMatch { case _: OrExpr =>
    }
    val o         = Optimizer.only(Normalize, options)
    val optimized = o.run(p)
    debug(optimized.explain)
    val cs = conjuncts(filterOf(optimized).cond.expr)
    cs.size shouldBe 2
    cs.head.sig shouldContain "lit(R"
    cs(1) shouldMatch { case _: OrExpr =>
    }
    o.firedRules shouldBe Map("factor_or" -> 1)
    optimized.output shouldBe p.output
    // A second run finds nothing more to factor
    Optimizer.only(Normalize, options).run(optimized) shouldBeTheSameInstanceAs optimized
  }

  test("drop an or whose branch is only the common conjuncts") {
    val p = bound("""from lineitem
        |where l_returnflag = 'R' or (l_returnflag = 'R' and l_quantity > 10)
        |select l_orderkey""".stripMargin)
    val optimized = Optimizer.only(Normalize, options).run(p)
    val cs        = conjuncts(filterOf(optimized).cond.expr)
    cs.size shouldBe 1
    cs.head.sig shouldNotContain " or "
  }

  test("factor a common conjunct nested in parentheses") {
    val p = bound("""from lineitem
        |where (l_quantity > 10 and (l_returnflag = 'R' and l_tax < 0.1))
        |   or (l_discount < 0.05 and (l_returnflag = 'R' and l_tax > 0.2))
        |select l_orderkey""".stripMargin)
    val cs = conjuncts(filterOf(Optimizer.only(Normalize, options).run(p)).cond.expr)
    cs.size shouldBe 2
    cs.head.sig shouldContain "lit(R"
  }

  test("keep a between as one conjunct") {
    val p = bound("""from lineitem
        |where (l_returnflag = 'R' and l_quantity between 1 and 10)
        |   or (l_returnflag = 'A' and l_quantity between 1 and 20)
        |select l_orderkey""".stripMargin)
    val o = Optimizer.only(Normalize, options)
    o.run(p) shouldBeTheSameInstanceAs p
  }

  test("leave an or without common conjuncts unchanged") {
    val p = bound("""from lineitem
        |where l_returnflag = 'R' or l_quantity > 10
        |select l_orderkey""".stripMargin)
    val o = Optimizer.only(Normalize, options)
    o.run(p) shouldBeTheSameInstanceAs p
    o.firedRules shouldBe Map.empty
  }

  test("skip the rule when it is disabled") {
    val p = bound("""from lineitem
        |where (l_returnflag = 'R' and l_quantity > 10) or (l_returnflag = 'R' and l_discount < 0.05)
        |select l_orderkey""".stripMargin)
    val o = Optimizer.only(Normalize, options.withDisabled(Optimization.FactorOr))
    o.run(p) shouldBeTheSameInstanceAs p
    o.firedRules shouldBe Map.empty
  }

  test("factor the filters of runtime plans in the default pipeline") {
    val query =
      """from lineitem
        |where (l_returnflag = 'R' and l_quantity > 10) or (l_returnflag = 'R' and l_discount < 0.05)
        |select l_orderkey""".stripMargin
    val p = withQuery(query)(engine.runtimePlan(_))
    conjuncts(filterOf(p).cond.expr).size shouldBe 2
    // The rewritten filter returns the same rows as the unfactored one
    def count(e: WvletEngine): Long =
      withQuery(query)(q => e.execute(q).batches.map(_.size.toLong).sum)
    count(engine) shouldBe count(unoptimized)
  }

  test("fail a rule batch that doesn't converge when plans are validated") {
    val p = bound("from nation select n_name")
    object Flip extends Rule:
      override def optimization: Optimization                 = Optimization.FactorOr
      override def apply(p: RuntimePlan): Option[RuntimePlan] =
        p match
          case RuntimePlan.Project(child, exprs, cols) =>
            Some(RuntimePlan.Project(child, exprs, cols))
          case _ =>
            None
    val e = intercept[wvlet.lang.api.WvletLangException] {
      Optimizer.only(RuleBatch("flip", Seq(Flip), maxIterations = 3), options).run(p)
    }
    e.getMessage shouldContain "didn't converge"
  }

end EngineNormalizeTest
