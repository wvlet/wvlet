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
package wvlet.lang.runner.connector.trino

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.runner.QueryExecutor
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.control.Control
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path

/** `save to '<local file>'` on Trino, which can't write local files, in each supported format */
class TrinoSaveToFileTest extends UniTest:
  private val server  = TestTrinoServer().withCustomMemoryPlugin
  private val workEnv = WorkEnv(".")
  private val trino   = ConnectorConfig(
    name = "trino",
    `type` = "trino",
    default = true,
    user = Some("test"),
    password = Some(""),
    host = Some(server.address),
    catalog = Some("memory"),
    schema = Some("main"),
    properties = Map("useSSL" -> false)
  )

  private val profile  = Profile(name = s"local-trino@${server.address}", connectors = Seq(trino))
  private val provider = ConnectorProvider(workEnv)
  private val executor = QueryExecutor(provider, profile, workEnv)
  private val compiler = Compiler(
    CompilerOptions(
      workEnv = workEnv,
      catalog = Some("memory"),
      schema = Some("main"),
      dbType = DBType.Trino
    )
  )

  compiler.setDefaultCatalog(provider.getDBConnector(trino).getCatalog("memory", "main"))

  private val dir = Files.createTempDirectory(
    Files.createDirectories(Path.of("target")),
    "trino-save"
  )

  override def afterAll: Unit = Control.closeResources(executor, provider, server)

  private def saveTo(file: String, options: String = ""): Path =
    val path = dir.resolve(file)
    val unit = CompilationUnit.fromWvletString(
      s"from [[1, 'a'], [2, 'b,c']] as t(id, name) | save to '${path}'${options}"
    )
    val result = compiler.compileSingleUnit(unit)
    executor.executeSingle(unit, result.context).isSuccess shouldBe true
    path

  test("should save to a Parquet file") {
    val t = ParquetSource("t", saveTo("out.parquet").toString)
    t.schema.names shouldBe IndexedSeq("id", "name")
    t.estimatedRows shouldBe Some(2L)
  }

  test("should save to a CSV file with a header") {
    Files.readString(saveTo("out.csv")) shouldBe "id,name\n1,a\n2,\"b,c\"\n"
  }

  test("should save to a TSV file") {
    Files.readString(saveTo("out.tsv")) shouldBe "id\tname\n1\ta\n2\tb,c\n"
  }

  test("should save to a JSON lines file") {
    Files.readString(saveTo("out.json")) shouldBe """{"id":1,"name":"a"}
        |{"id":2,"name":"b,c"}
        |""".stripMargin
  }

  test("should pick the format from the format option") {
    Files.readString(saveTo("out.txt", " with format: 'csv'")) shouldContain "id,name"
  }

end TrinoSaveToFileTest
