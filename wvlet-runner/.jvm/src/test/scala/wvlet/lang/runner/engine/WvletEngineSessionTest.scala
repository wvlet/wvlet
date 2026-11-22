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

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
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

  test("derive table names from data file names") {
    WvletEngineSession.tableNameOf("orders.parquet") shouldBe "orders"
    WvletEngineSession.tableNameOf("events.jsonl.gz") shouldBe "events"
    WvletEngineSession.tableNameOf("iceberg_table") shouldBe "iceberg_table"
  }

end WvletEngineSessionTest
