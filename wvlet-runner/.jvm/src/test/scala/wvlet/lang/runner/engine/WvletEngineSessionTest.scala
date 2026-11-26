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
package wvlet.lang.runner.engine

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
import wvlet.lang.runner.PlanExecutor
import wvlet.lang.runner.connector.SqlConnectorProvider
import wvlet.uni.log.LogLevel
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path

class WvletEngineSessionTest extends UniTest:
  test("register every data file of dataDir by file name") {
    val session = WvletEngineSession(
      ConnectorConfig(name = "wvlet", `type` = "wvlet", properties = Map("dataDir" -> "data")),
      "spec/tpch"
    )
    session.tableNames.sorted shouldBe
      Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
    // Table schemas are visible to the compiler
    session.catalog.getTable("main", "nation").columns.map(_.name) shouldContain "n_name"
  }

  test("register tables mapped in profiles.json") {
    val dir = Files.createTempDirectory(Files.createDirectories(Path.of("target")), "wvlet-profile")
    Files.writeString(
      dir.resolve("profiles.json"),
      """{"profiles": [{"name": "local", "connectors": [{
        |  "name": "wvlet", "type": "wvlet",
        |  "properties": {"tables": {"nations": "spec/tpch/data/nation.parquet"}}
        |}]}]}""".stripMargin
    )
    val profile = Profile.getProfile("local", dir.toString).get
    val session = WvletEngineSession(profile.defaultEngine, ".")
    session.tableNames shouldBe Seq("nations")
    session.catalog.getTable("main", "nations").columns.size shouldBe 4
  }

  test("reject writes to tables backed by files") {
    val config = ConnectorConfig(
      name = "wvlet",
      `type` = "wvlet",
      properties = Map("dataDir" -> "data")
    )
    val profile  = Profile("test", List(config))
    val workEnv  = WorkEnv("spec/tpch", logLevel = LogLevel.WARN)
    val compiler = Compiler(
      CompilerOptions(sourceFolders = Nil, workEnv = workEnv, dbType = DBType.Wvlet)
    )
    val unit = CompilationUnit.fromWvletString("""from nation
                                                 |where n_nationkey = 0
                                                 |delete""".stripMargin)
    val result = compiler.compileSingleUnit(unit)
    result.reportAllErrors
    val ctx      = result.context.withCompilationUnit(unit).newContext(Symbol.NoSymbol)
    val provider = SqlConnectorProvider(profile)
    try
      val e = intercept[WvletLangException] {
        PlanExecutor(provider, profile, workEnv).execute(ExecutionPlanner.plan(unit, ctx), ctx)
      }
      e.getMessage shouldContain "read-only"
    finally
      provider.close()
  }

  test("derive table names from data file names") {
    WvletEngineSession.tableNameOf("orders.parquet") shouldBe "orders"
    WvletEngineSession.tableNameOf("events.jsonl.gz") shouldBe "events"
    WvletEngineSession.tableNameOf("iceberg_table") shouldBe "iceberg_table"
  }

  private def tpchConfig(props: (String, Any)*): ConnectorConfig = ConnectorConfig(
    name = "wvlet",
    `type` = "wvlet",
    properties = Map[String, Any]("prepareTPCH" -> true) ++ props
  )

  test("register TPC-H tables generated in memory with prepareTPCH") {
    val session = WvletEngineSession(tpchConfig(), ".")
    session.tableNames.sorted shouldBe
      Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
    // Typed for the compiler, so stdlib methods on columns resolve
    session.catalog.getTable("main", "orders").columns.map(_.name) shouldContain "o_comment"
    // The default scale factor is DuckDB's, 0.01
    session.engine.catalog.get("orders").estimatedRows shouldBe Some(15000L)
  }

  test("generate TPC-H tables at tpchScaleFactor") {
    val session = WvletEngineSession(tpchConfig("tpchScaleFactor" -> "0.1"), ".")
    session.engine.catalog.get("orders").estimatedRows shouldBe Some(150000L)
    val e = intercept[WvletLangException] {
      WvletEngineSession(tpchConfig("tpchScaleFactor" -> "large"), ".")
    }
    e.getMessage shouldContain "tpchScaleFactor"
  }

  test("reject prepareTPCDS on the engine and TPC-H names that files also register") {
    val ds = intercept[WvletLangException] {
      WvletEngineSession(tpchConfig("prepareTPCH" -> false, "prepareTPCDS" -> true), ".")
    }
    ds.getMessage shouldContain "TPC-DS"
    val clash = intercept[WvletLangException] {
      WvletEngineSession(tpchConfig("dataDir" -> "data"), "spec/tpch")
    }
    clash.getMessage shouldContain "prepareTPCH"
    clash.getMessage shouldContain "registered from files"
  }

  test("reject writes to TPC-H tables") {
    val profile  = Profile("test", List(tpchConfig()))
    val workEnv  = WorkEnv(".", logLevel = LogLevel.WARN)
    val compiler = Compiler(
      CompilerOptions(sourceFolders = Nil, workEnv = workEnv, dbType = DBType.Wvlet)
    )
    val unit = CompilationUnit.fromWvletString("""from nation
                                                 |where n_nationkey = 0
                                                 |delete""".stripMargin)
    val result = compiler.compileSingleUnit(unit)
    result.reportAllErrors
    val ctx      = result.context.withCompilationUnit(unit).newContext(Symbol.NoSymbol)
    val provider = SqlConnectorProvider(profile)
    try
      val e = intercept[WvletLangException] {
        PlanExecutor(provider, profile, workEnv).execute(ExecutionPlanner.plan(unit, ctx), ctx)
      }
      e.getMessage shouldContain "read-only"
    finally
      provider.close()
  }

end WvletEngineSessionTest
