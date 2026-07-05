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

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.LazyCatalog
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.test.UniTest

/**
  * End-to-end tests for `from <connector>.<table>` resolution and `use <connector>` switching with
  * a profile that activates two DuckDB connectors (#1861 Phase 2)
  */
class ConnectorNamespaceTest extends UniTest:

  private val workEnv = WorkEnv()
  private val first   = ConnectorConfig(
    name = "first",
    `type` = "duckdb",
    default = true,
    catalog = Some("memory"),
    schema = Some("main")
  )

  private val second = ConnectorConfig(
    name = "second",
    `type` = "duckdb",
    catalog = Some("memory"),
    schema = Some("main")
  )

  private val profile = Profile(name = "multi", connectors = Seq(first, second))

  private val provider = ConnectorProvider(workEnv)
  private val executor = QueryExecutor(provider, profile, workEnv)

  private val compiler = Compiler(
    CompilerOptions(sourceFolders = List("."), workEnv = workEnv, catalog = Some("memory"))
  )

  // Register both connectors by name, as WvletCompiler/WvletScriptRunner do from a profile
  profile
    .connectors
    .foreach { c =>
      val schema      = c.schema.getOrElse("main")
      val catalogName = c.catalog.get
      compiler.addConnectorCatalog(
        c.name,
        LazyCatalog(
          catalogName,
          c.dbType,
          () => provider.getDBConnector(c).getCatalog(catalogName, schema)
        ),
        schema
      )
    }

  compiler.setDefaultCatalog(provider.getDBConnector(first).getCatalog("memory", "main"))
  compiler.setDefaultSchema("main")

  // A table that only exists on the second connector's in-memory database
  provider.getDBConnector(second).execute("create table events as select 42 as id")

  override def afterAll: Unit =
    executor.close()
    provider.close()

  private def run(statement: String): QueryResult =
    val unit   = CompilationUnit.fromWvletString(statement)
    val result = compiler.compileSingleUnit(unit)
    executor.executeSingle(unit, result.context)

  test("should reject a cross-connector reference while another engine is active") {
    val exception = intercept[WvletLangException] {
      run("from second.events")
    }
    exception.statusCode shouldBe StatusCode.NOT_IMPLEMENTED
    exception.message shouldContain "use second"
  }

  test("should run a connector-qualified query after switching with use <connector>") {
    run("use second") shouldBe QueryResult.empty
    val result = run("from second.events")
    result.toTSV shouldContain "42"
  }

  test("should resolve connector-qualified schema.table references") {
    val result = run("from second.main.events")
    result.toTSV shouldContain "42"
  }

  test("should run use and a connector-qualified query within a single unit") {
    // The guard must evaluate against the engine active when the query runs, not when the
    // unit starts — otherwise the documented `use x` + `from x.t` batch pattern breaks
    run("use first") shouldBe QueryResult.empty
    val result = run("""use second
        |from second.events""".stripMargin)
    result.toTSV shouldContain "42"
    run("use first") shouldBe QueryResult.empty
  }

  test("should fail with a clear error for an unknown connector in use") {
    val exception = intercept[WvletLangException] {
      run("use connector no_such_connector")
    }
    exception.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    exception.message shouldContain "not defined in profile"
  }

end ConnectorNamespaceTest
