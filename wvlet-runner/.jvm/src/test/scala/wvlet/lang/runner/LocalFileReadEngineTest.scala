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
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.test.UniTest

/**
  * A query reading only local files runs on the in-process wvlet engine when the active engine
  * can't read local files (here a Trino connector that is never contacted)
  */
class LocalFileReadEngineTest extends UniTest:

  private val workEnv = WorkEnv()
  // Port 1 never answers: the test fails if the query is sent to Trino
  private val trino = ConnectorConfig(
    name = "trino",
    `type` = "trino",
    default = true,
    host = Some("localhost"),
    port = Some(1)
  )

  private val provider = ConnectorProvider(workEnv)
  private val executor = QueryExecutor(provider, Profile("remote", Seq(trino)), workEnv)
  private val compiler = Compiler(
    CompilerOptions(sourceFolders = List("."), workEnv = workEnv, dbType = DBType.Trino)
  )

  override def afterAll: Unit =
    executor.close()
    provider.close()

  test("run a local file read on the wvlet engine for a Trino profile") {
    val unit = CompilationUnit.fromWvletString("from 'spec/tpch/data/region.parquet' select r_name")
    val result = executor.executeSingle(unit, compiler.compileSingleUnit(unit).context)
    val tsv    = result.toTSV
    tsv shouldContain "ASIA"
    tsv shouldContain "EUROPE"
  }

end LocalFileReadEngineTest
