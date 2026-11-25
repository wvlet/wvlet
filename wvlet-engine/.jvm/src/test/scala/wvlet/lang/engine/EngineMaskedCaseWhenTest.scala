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
import wvlet.lang.engine.expr.CaseWhen
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

/**
  * `masked_case_when`: lowering marks CASE expressions to mask the THEN vector of a single WHEN
  * without ELSE, with the same rows as the per-row branch choice
  */
class EngineMaskedCaseWhenTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(options.withDisabled(Optimization.MaskedCaseWhen))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private val query =
    """from [[1, 'Sunday', 10], [2, 'Monday', 20], [3, 'Sunday', null], [4, null, 40]]
      |  as days(id, day, price)
      |select id, if day = 'Sunday' then price else null as sun
      |order by id""".stripMargin

  private def run[A](body: Relation => Context ?=> A): A =
    val unit           = CompilationUnit.fromWvletString(query)
    val r              = compiler.compileSingleUnit(unit)
    given ctx: Context = r.context
    val q              = queryOf(unit.executionPlan).getOrElse(fail(s"No query in ${query}"))
    body(q)

  private def queryOf(e: ExecutionPlan): Option[Relation] =
    e match
      case ExecuteQuery(q: Relation) =>
        Some(q)
      case ExecuteTasks(ts) =>
        ts.iterator.flatMap(queryOf).nextOption()
      case _ =>
        None

  /** The maskNulls flags of the CASE expressions in the physical plan's projections */
  private def maskFlags(e: WvletEngine): Seq[Boolean] =
    def cases(x: VExpr): Seq[Boolean] =
      (
        x match
          case c: CaseWhen =>
            Seq(c.maskNulls)
          case _ =>
            Nil
      ) ++ x.children.flatMap(cases)
    def collect(op: Operator): Seq[Boolean] =
      (
        op match
          case p: ProjectOp =>
            p.exprs.flatMap(cases)
          case _ =>
            Nil
      ) ++ op.children.flatMap(collect)
    run(q => collect(e.physicalPlan(GenSQL.expand(q, summon[Context]))))

  private def rows(e: WvletEngine): Seq[String] = run(q =>
    e.execute(q).rows.map(_.mkString(",")).toSeq
  )

  test("mark CASE expressions for masking only with masked_case_when") {
    maskFlags(on) shouldBe Seq(true)
    maskFlags(off) shouldBe Seq(false)
  }

  test("return the same rows with and without masked_case_when") {
    val r = rows(on)
    r shouldBe rows(off)
    r shouldBe Seq("1,10", "2,null", "3,null", "4,null")
  }

end EngineMaskedCaseWhenTest
