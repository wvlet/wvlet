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
  * Flows run from `wvlet run` (`run flow`) on the in-process wvlet engine, staging tables of
  * another connector into the engine
  */
class EngineFlowStagingTest extends UniTest:

  private val workEnv = WorkEnv()
  private val engine  = ConnectorConfig(name = "local", `type` = "wvlet", default = true)
  private val remote  = ConnectorConfig(
    name = "remote",
    `type` = "duckdb",
    catalog = Some("memory"),
    schema = Some("main")
  )

  private val profile  = Profile(name = "engine_staging", connectors = Seq(engine, remote))
  private val provider = ConnectorProvider(workEnv)
  private val executor = QueryExecutor(provider, profile, workEnv)

  private val compiler = Compiler(CompilerOptions(sourceFolders = List("."), workEnv = workEnv))

  private def lazyCatalogOf(name: String): LazyCatalog = LazyCatalog(
    name,
    remote.dbType,
    () => provider.getDBConnector(remote).getCatalog(name, "main")
  )

  compiler.addConnectorCatalog(
    remote.name,
    lazyCatalogOf("memory"),
    "main",
    catalogProvider = Some(lazyCatalogOf)
  )

  provider
    .getDBConnector(remote)
    .execute("create table remote_events as select * from (values (1, 'a'), (2, 'b')) t(id, tag)")

  override def afterAll: Unit =
    executor.close()
    provider.close()

  private def run(statement: String): QueryResult =
    val unit   = CompilationUnit.fromWvletString(statement)
    val result = compiler.compileSingleUnit(unit)
    executor.executeSingle(unit, result.context)

  test("run a flow from a query on the engine, staging a table of another connector") {
    val result = run("""flow engine_staging_flow = {
        |  stage copied = from remote.remote_events
        |  stage counted = from copied | select count(*) as cnt
        |}
        |
        |run flow engine_staging_flow""".stripMargin)
    result.isSuccess shouldBe true
    val summary = result.toPrettyBox()
    summary shouldContain "copied"
    summary shouldContain "counted"
    summary shouldNotContain "failed"
  }

end EngineFlowStagingTest
