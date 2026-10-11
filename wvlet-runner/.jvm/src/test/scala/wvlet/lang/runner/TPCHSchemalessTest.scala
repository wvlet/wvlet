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

import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.test.UniTest

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption

/**
  * Compile the TPC-H queries without `spec/tpch/schema.wv`, as `wvlet compile` does from a plain
  * work folder, and execute the generated SQL on DuckDB. Without column types, member calls such as
  * `o_comment.like(...)`, `o_orderkey.in(...)`, `p_size.between(...)` and `l_shipdate.extract(...)`
  * are not inlined by the analyzer and must be lowered by the SQL generator instead of being
  * emitted as invalid `x."like"(...)` function calls.
  */
class TPCHSchemalessTest extends UniTest:
  private val specDir = File("spec/tpch")
  private val workDir = File("target/tpch-schemaless")

  // Copy only the query files: no schema.wv, no test/ folder. Clear stale copies first so a
  // query removed from spec/tpch does not linger here
  workDir.mkdirs()
  Option(workDir.listFiles()).getOrElse(Array.empty[File]).foreach(_.delete())
  private val queryFiles: Seq[File] = Option(specDir.listFiles())
    .getOrElse(Array.empty[File])
    .toSeq
    .filter(f => f.isFile && f.getName.matches("q\\d+\\.wv"))
    .sortBy(f => f.getName.stripPrefix("q").stripSuffix(".wv").toInt)
    .map { f =>
      val dest = File(workDir, f.getName)
      Files.copy(f.toPath, dest.toPath, StandardCopyOption.REPLACE_EXISTING)
      dest
    }

  private val workEnv = WorkEnv(path = workDir.getPath, logLevel = logger.getLogLevel)
  private val profile = Profile.defaultDuckDBProfile.withProperty("prepareTPCH", true)

  private val connectorProvider = ConnectorProvider(workEnv)
  private val queryExecutor     = QueryExecutor(connectorProvider, profile, workEnv)
  override def afterAll: Unit   =
    queryExecutor.close()
    connectorProvider.close()

  // No default catalog is registered on purpose: the compiler must not learn the TPC-H table
  // schemas from the engine, so every column reference stays untyped
  private val compiler = Compiler(
    CompilerOptions(sourceFolders = List(workDir.getPath), workEnv = workEnv)
  )

  private val units = compiler.localCompilationUnits

  test("cover every TPC-H query") {
    units.size shouldBe queryFiles.size
  }

  for unit <- units do
    test(s"run ${unit.sourceFile.fileName} without a schema") {
      val compileResult = compiler.compileSingleUnit(unit)
      val result = queryExecutor.executeSingle(unit, compileResult.context.withDebugRun(true))
      result.getError.foreach(e => throw e)
      result.isEmpty shouldBe false
    }

end TPCHSchemalessTest
