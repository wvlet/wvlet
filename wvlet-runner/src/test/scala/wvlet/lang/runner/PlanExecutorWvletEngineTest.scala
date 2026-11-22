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
package wvlet.lang.runner

import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.runner.connector.SqlConnectorProvider
import wvlet.lang.runner.engine.WvletEngineSession
import wvlet.uni.log.LogLevel
import wvlet.uni.test.UniTest

/**
  * [[PlanExecutor]] on the in-process wvlet engine (connector type `wvlet`): runs without DuckDB on
  * JVM, Node.js, and Native alike
  */
class PlanExecutorWvletEngineTest extends UniTest:
  private val engineConfig = ConnectorConfig(name = "wvlet", `type` = "wvlet", default = true)
  private val profile      = Profile("test", List(engineConfig))

  private def run(query: String, profile: Profile = profile): QueryResult =
    val workEnv  = WorkEnv(".", logLevel = LogLevel.WARN)
    val compiler = Compiler(
      CompilerOptions(sourceFolders = List("."), workEnv = workEnv, dbType = DBType.Wvlet)
    )
    val unit          = CompilationUnit.fromWvletString(query)
    val compileResult = compiler.compileSingleUnit(unit)
    compileResult.reportAllErrors
    val ctx = compileResult
      .context
      .withCompilationUnit(unit)
      .withDebugRun(true)
      .newContext(Symbol.NoSymbol)
    val provider = SqlConnectorProvider(profile)
    try
      val executor = PlanExecutor(provider, profile, workEnv)
      executor.execute(ExecutionPlanner.plan(unit, ctx), ctx)
    finally
      provider.close()

  private def tables(r: QueryResult): List[TableRows] =
    r match
      case l: QueryResultList =>
        l.list.toList.flatMap(tables)
      case t: TableRows =>
        List(t)
      case _ =>
        Nil

  test("tell the in-process engine from a remote wvlet server") {
    WvletEngineSession.isInProcess(engineConfig) shouldBe true
    WvletEngineSession.isInProcess(engineConfig.withHost("localhost")) shouldBe false
    WvletEngineSession.isInProcess(engineConfig.withType("duckdb")) shouldBe false
  }

  test("execute a query on the engine") {
    val result = run("""from [[1, 'a'], [2, 'b'], [3, 'c']] as t(id, name)
                       |where id >= 2
                       |select id, name""".stripMargin)
    result.getError shouldBe None
    val t = tables(result).last
    t.totalRows shouldBe 2
    t.schema.fields.map(_.name.name) shouldBe List("id", "name")
    t.rows.map(_("name")) shouldBe List("b", "c")
  }

  test("evaluate test statements on engine results") {
    val result = run("""from [[1], [2], [3]] as t(a)
                       |agg _.count as n
                       |test _.rows should be [[3]]""".stripMargin)
    result.getError shouldBe None
  }

  test("read tables saved earlier in the session") {
    val result = run("""from [[1, 10], [2, 20]] as t(id, v)
                       |save to saved_t
                       |
                       |from saved_t
                       |agg _.sum(v) as total
                       |test _.rows should be [[30]]""".stripMargin)
    result.getError shouldBe None
  }

  test("reject SQL statements the engine can't run") {
    val e = intercept[WvletLangException] {
      run("create schema engine_s if not exists")
    }
    e.getMessage shouldContain "not supported by the wvlet engine"
  }

  test("switch to the engine with use <connector>") {
    val p = Profile(
      "test",
      List(
        ConnectorConfig(name = "duckdb", `type` = "duckdb", default = true),
        engineConfig.withDefault(false)
      )
    )
    val result = run(
      """use wvlet
        |from [[1], [2]] as t(a)
        |test _.size should be 2""".stripMargin,
      p
    )
    result.getError shouldBe None
  }

end PlanExecutorWvletEngineTest
