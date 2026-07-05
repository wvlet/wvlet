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
  * Flows with per-stage engine selection (`stage x on <connector>`) and cross-connector staged
  * inputs (#1861 Phase 2)
  */
class FlowConnectorStagingTest extends UniTest:

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

  profile
    .connectors
    .foreach { c =>
      val schema                                   = c.schema.getOrElse("main")
      def lazyCatalogOf(name: String): LazyCatalog = LazyCatalog(
        name,
        c.dbType,
        () => provider.getDBConnector(c).getCatalog(name, schema)
      )
      compiler.addConnectorCatalog(
        c.name,
        lazyCatalogOf(c.catalog.get),
        schema,
        catalogProvider = Some(lazyCatalogOf)
      )
    }

  compiler.setDefaultCatalog(provider.getDBConnector(first).getCatalog("memory", "main"))
  compiler.setDefaultSchema("main")

  // Source data on the second (non-default) connector
  provider
    .getDBConnector(second)
    .execute("create table remote_events as select * from (values (1, 'a'), (2, 'b')) t(id, tag)")

  // A second catalog attached to the second connector, for 4-part staged references
  provider.getDBConnector(second).execute("attach ':memory:' as extra")
  provider
    .getDBConnector(second)
    .execute("create table extra.main.extra_events as select * from (values (9, 'x')) t(id, tag)")

  override def afterAll: Unit =
    executor.close()
    provider.close()

  private def run(statement: String): QueryResult =
    val unit   = CompilationUnit.fromWvletString(statement)
    val result = compiler.compileSingleUnit(unit)
    executor.executeSingle(unit, result.context)

  test("should stage a cross-connector table into the stage's engine") {
    val result = run("""flow staging_flow = {
        |  stage copied = from second.remote_events
        |  stage counted = from copied | select count(*) as cnt
        |}
        |
        |run flow staging_flow""".stripMargin)
    result.isSuccess shouldBe true
  }

  test("should stage a 4-part cross-connector table into the stage's engine") {
    val result = run("""flow four_part_flow = {
        |  stage copied = from second.extra.main.extra_events
        |  stage checked = from copied | select count(*) as cnt
        |}
        |
        |run flow four_part_flow""".stripMargin)
    result.isSuccess shouldBe true
  }

  test("should run a stage on an explicitly selected engine") {
    // `on first` names the default engine explicitly; resolution must go through the
    // engine-name path (not the default fallback)
    val result = run("""flow on_engine_flow = {
        |  stage base on first = from second.remote_events | select id
        |}
        |
        |run flow on_engine_flow""".stripMargin)
    result.isSuccess shouldBe true
  }

end FlowConnectorStagingTest
