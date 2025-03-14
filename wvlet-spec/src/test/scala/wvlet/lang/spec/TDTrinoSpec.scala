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
package wvlet.lang.spec

import wvlet.airspec.AirSpec
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.{Compiler, CompilerOptions, WorkEnv}
import wvlet.lang.runner.QueryExecutor
import wvlet.lang.runner.connector.trino.{TrinoConfig, TrinoConnector}
import wvlet.lang.api.{StatusCode, WvletLangException}
import wvlet.lang.runner.connector.DBConnectorProvider

trait TDTrinoSpecRunner(specPath: String) extends AirSpec:
  if inCI then
    skip("Trino td-dev profile is not available in CI")

  private val profile: Profile = Profile
    .getProfile("td-dev")
    .getOrElse {
      skip("Skip as td-dev profile is not found in ~/.wvlet/profiles.yml").asInstanceOf[Profile]
    }

  private val defaultCatalog = profile.catalog.getOrElse("td")
  private val defaultSchema  = profile.schema.getOrElse("default")

  private val config = TrinoConfig(
    catalog = defaultCatalog,
    schema = defaultSchema,
    hostAndPort = profile.host.getOrElse("localhost"),
    user = profile.user,
    password = profile.password
  )

  private val workEnv             = WorkEnv(path = specPath, logLevel = logger.getLogLevel)
  private val dbConnectorProvider = DBConnectorProvider(workEnv)
  private val executor            = QueryExecutor(dbConnectorProvider, profile, workEnv)

  private val compiler = Compiler(
    CompilerOptions(
      sourceFolders = List(specPath),
      workEnv = workEnv,
      catalog = Some(defaultCatalog),
      schema = Some(defaultSchema)
    )
  )

  override def afterAll: Unit =
    executor.close()
    dbConnectorProvider.close()

  // Need to tell it's Trino
  compiler.setDefaultCatalog(
    dbConnectorProvider.getConnector(profile).getCatalog(defaultCatalog, defaultSchema)
  )

  // Compile all files in the source paths first
  for unit <- compiler.localCompilationUnits do
    test(unit.sourceFile.fileName) {
      // ignoredSpec.get(unit.sourceFile.fileName).foreach(reason => ignore(reason))

      try
        val compileResult = compiler.compileSingleUnit(unit)
        val result        = executor.executeSingle(unit, compileResult.context)
        debug(result.toPrettyBox(maxWidth = Some(120)))
      catch
        case e: WvletLangException if e.statusCode.isUserError =>
          trace(e)
          fail(e.getMessage)
    }

end TDTrinoSpecRunner

class TDTrinoSpec extends TDTrinoSpecRunner("spec/trino")
