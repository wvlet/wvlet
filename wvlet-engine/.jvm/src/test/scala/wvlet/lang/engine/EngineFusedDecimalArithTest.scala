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
import wvlet.lang.engine.exec.HashAggOp
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.exec.ProjectOp
import wvlet.lang.engine.expr.FusedDecimalMul
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/**
  * `fused_decimal_arith`: lowering fuses TPC-H q1's decimal `x * (1 - y)` and `... * (1 + z)` into
  * [[FusedDecimalMul]], with the same sums as [[wvlet.lang.engine.expr.Arith]]
  */
class EngineFusedDecimalArithTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  EngineTPCHBench.tables.foreach(t => on.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))
  private val off = on.withOptimizer(options.withDisabled(Optimization.FusedDecimalArith))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private val query =
    """select l_returnflag, l_linestatus,
      |  sum(l_extendedprice * (1 - l_discount)) as disc_price,
      |  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as charge
      |from lineitem
      |where l_shipdate <= date '1998-09-02'
      |group by l_returnflag, l_linestatus
      |order by l_returnflag, l_linestatus""".stripMargin

  private def withQuery[A](f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromSqlString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${unit}"))
    f(GenSQL.expand(q, summon[Context]))

  /** The fused products in the physical plan's projections and hash aggregations */
  private def fusedCount(e: WvletEngine): Int =
    def count(x: VExpr): Int =
      (
        x match
          case _: FusedDecimalMul =>
            1
          case _ =>
            0
      ) + x.children.map(count).sum
    def collect(op: Operator): Int =
      (
        op match
          case p: ProjectOp =>
            p.exprs.map(count).sum
          case a: HashAggOp =>
            a.aggs.flatMap(_.args).map(count).sum
          case _ =>
            0
      ) + op.children.map(collect).sum
    withQuery(q => collect(e.physicalPlan(q)))

  test("fuse decimal products of constant sums only with fused_decimal_arith") {
    fusedCount(on) shouldBe 2
    fusedCount(off) shouldBe 0
  }

  test("return the same sums with and without fused_decimal_arith") {
    val r = withQuery(on.execute(_)).rows.map(_.toSeq).toSeq
    r shouldBe withQuery(off.execute(_)).rows.map(_.toSeq).toSeq
    r.size shouldBe 4
  }

end EngineFusedDecimalArithTest
