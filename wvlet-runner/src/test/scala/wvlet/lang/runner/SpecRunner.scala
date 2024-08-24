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

import wvlet.lang.WvletLangException
import wvlet.lang.compiler.{Compiler, CompilerOptions}
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.airspec.AirSpec

import java.io.File
import java.sql.SQLException

trait SpecRunner(
    specPath: String,
    ignoredSpec: Map[String, String] = Map.empty,
    prepareTPCH: Boolean = false
) extends AirSpec:
  private val duckDB          = QueryExecutor(DuckDBConnector(prepareTPCH = prepareTPCH))
  override def afterAll: Unit = duckDB.close()

  private val compiler = Compiler(
    CompilerOptions(sourceFolders = List(specPath), workingFolder = specPath)
  )

  // Compile all files in the source paths first
  for unit <- compiler.localCompilationUnits do
    test(unit.sourceFile.fileName) {
      ignoredSpec.get(unit.sourceFile.fileName).foreach(reason => ignore(reason))

      try
        val compileResult = compiler.compileSingleUnit(unit)
        val result        = duckDB.executeSingle(unit, compileResult.context)
        debug(result.toPrettyBox(maxWidth = Some(120)))
      catch
        case e: WvletLangException if e.statusCode.isUserError =>
          fail(e.getMessage)
    }

class BasicSpec
    extends SpecRunner(
      "spec/basic",
      ignoredSpec = Map("values.wv" -> "Need to support triple quotes")
    )

class Model1Spec extends SpecRunner("spec/model1")
class TPCHSpec   extends SpecRunner("spec/tpch", prepareTPCH = true)
class DuckDBSpec extends SpecRunner("spec/duckdb")
