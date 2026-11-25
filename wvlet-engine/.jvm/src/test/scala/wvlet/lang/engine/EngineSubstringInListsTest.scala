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
import wvlet.lang.engine.exec.FilterOp
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.expr.InList
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

/**
  * `substring_in_lists`: lowering marks string IN lists to look up `substring(s, k, n)` regions
  * among their values, with the same rows as building the substrings
  */
class EngineSubstringInListsTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(options.withDisabled(Optimization.SubstringInLists))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private val query =
    """from [[1, '13-555'], [2, '31-1'], [3, null], [4, '7'], [5, '29-0'], [6, '1']]
      |  as customers(id, phone)
      |where phone.substring(1, 2).in('13', '29', '7')
      |select id
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

  /** The substringRegions flags of the IN lists in the physical plan's filters */
  private def regionFlags(e: WvletEngine): Seq[Boolean] =
    def lists(x: VExpr): Seq[Boolean] =
      (
        x match
          case i: InList =>
            Seq(i.substringRegions)
          case _ =>
            Nil
      ) ++ x.children.flatMap(lists)
    def collect(op: Operator): Seq[Boolean] =
      (
        op match
          case f: FilterOp =>
            lists(f.cond)
          case _ =>
            Nil
      ) ++ op.children.flatMap(collect)
    run(q => collect(e.physicalPlan(GenSQL.expand(q, summon[Context]))))

  private def rows(e: WvletEngine): Seq[String] = run(q =>
    e.execute(q).rows.map(_.mkString(",")).toSeq
  )

  test("mark string IN lists for region lookups only with substring_in_lists") {
    regionFlags(on) shouldBe Seq(true)
    regionFlags(off) shouldBe Seq(false)
  }

  test("return the same rows with and without substring_in_lists") {
    val r = rows(on)
    r shouldBe rows(off)
    r shouldBe Seq("1", "4", "5")
  }

end EngineSubstringInListsTest
