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
package wvlet.lang.compiler.planner

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogLevel
import wvlet.uni.test.UniTest

/**
  * Verifies the shape of generated execution plans, especially that queries carrying `test`
  * statements execute exactly once: trailing tests are evaluated against the query's own result
  * instead of re-running the (test-stripped) query a second time.
  */
class ExecutionPlannerTest extends UniTest:

  private def plan(query: String, debugRun: Boolean): ExecutionPlan =
    val workEnv       = WorkEnv(".", logLevel = LogLevel.WARN)
    val compiler      = Compiler(CompilerOptions(sourceFolders = List("."), workEnv = workEnv))
    val unit          = CompilationUnit.fromWvletString(query)
    val compileResult = compiler.compileSingleUnit(unit)
    compileResult.reportAllErrors
    val ctx = compileResult
      .context
      .withCompilationUnit(unit)
      .withDebugRun(debugRun)
      .newContext(Symbol.NoSymbol)
    ExecutionPlanner.plan(unit, ctx)

  /** Flatten the plan tree into evaluation order */
  private def flatten(p: ExecutionPlan): List[ExecutionPlan] =
    val buf                          = List.newBuilder[ExecutionPlan]
    def iter(x: ExecutionPlan): Unit =
      buf += x
      x match
        case ExecuteTasks(tasks) =>
          tasks.foreach(iter)
        case ExecuteSave(_, queryPlan) =>
          iter(queryPlan)
        case ExecuteDebug(_, debugPlan) =>
          iter(debugPlan)
        case _ =>
    iter(p)
    buf.result()

  private def queriesAndTests(p: ExecutionPlan): List[ExecutionPlan] = flatten(p).filter {
    case _: ExecuteQuery | _: ExecuteTest =>
      true
    case _ =>
      false
  }

  test("should execute a query without tests exactly once") {
    val steps = queriesAndTests(plan("from [[1], [2]] as t(a) select a", debugRun = true))
    steps.count(_.isInstanceOf[ExecuteQuery]) shouldBe 1
    steps.count(_.isInstanceOf[ExecuteTest]) shouldBe 0
  }

  test("should run a query with trailing tests only once in a debug-run") {
    val steps = queriesAndTests(
      plan(
        """from [[1], [2]] as t(a)
          |select a
          |test _.size should be 2
          |test _.columns should contain 'a'""".stripMargin,
        debugRun = true
      )
    )
    steps.count(_.isInstanceOf[ExecuteQuery]) shouldBe 1
    steps.count(_.isInstanceOf[ExecuteTest]) shouldBe 2
    // Tests are evaluated after the query result is materialized
    steps.head.isInstanceOf[ExecuteQuery] shouldBe true
  }

  test("should not execute test inputs when tests are skipped (non-debug run)") {
    val steps = queriesAndTests(
      plan(
        """from [[1], [2]] as t(a)
          |select a
          |test _.size should be 2""".stripMargin,
        debugRun = false
      )
    )
    steps.count(_.isInstanceOf[ExecuteQuery]) shouldBe 1
    steps.count(_.isInstanceOf[ExecuteTest]) shouldBe 0
  }

  test("should still execute intermediate stages for mid-query tests") {
    val steps = queriesAndTests(
      plan(
        """from [[1], [2]] as t(a)
          |test _.size should be 2
          |where a > 1
          |select a
          |test _.size should be 1""".stripMargin,
        debugRun = true
      )
    )
    // The intermediate stage runs for its own test; the full query runs once for the
    // trailing test
    steps.count(_.isInstanceOf[ExecuteQuery]) shouldBe 2
    steps.count(_.isInstanceOf[ExecuteTest]) shouldBe 2
    steps
      .map {
        case _: ExecuteQuery =>
          "query"
        case _ =>
          "test"
      }
      .shouldBe(List("query", "test", "query", "test"))
  }

  test("should plan one execution per statement in multi-statement scripts") {
    val steps = queriesAndTests(
      plan(
        """select 10 as x;
          |select 20 as y
          |test _.size should be 1""".stripMargin,
        debugRun = true
      )
    )
    steps.count(_.isInstanceOf[ExecuteQuery]) shouldBe 2
    steps.count(_.isInstanceOf[ExecuteTest]) shouldBe 1
  }

end ExecutionPlannerTest
