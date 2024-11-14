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

import wvlet.airframe.Design
import wvlet.airspec.AirSpec
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}
import wvlet.lang.runner.connector.{DBConnector, DBConnectorProvider}
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.log.{LogLevel, LogSupport}

import scala.util.control.NonFatal

object SpecRunner extends LogSupport:

  def design(specPath: String, profile: Profile, logLevel: LogLevel) = Design
    .newDesign
    .bindInstance[WorkEnv](WorkEnv(path = specPath, logLevel = logLevel))
    .bindInstance[DBConnector](DBConnectorProvider.getConnector(profile))
    .bindSingleton[QueryExecutor]
    .bindProvider { (workEnv: WorkEnv) =>
      CompilerOptions(sourceFolders = List(workEnv.path), workEnv = workEnv)
    }
    .bindProvider { (opts: CompilerOption, dbConnector: DBConnector) =>
      val compiler = Compiler(opts)
      (profile.catalog, profile.schema) match
        case (Some(c), Some(s)) =>
          compiler.setDefaultCatalog(dbConnector.getCatalog(c, s))
        case _ =>
      compiler
    }

end SpecRunner

class SpecRunner(
    compiler: Compiler,
    executor: QueryExecutor
) extends AirSpec: 
  
  def localCompilationUnits: List[CompilationUnit] =
    compiler.localCompilationUnits
  
  def runSpec(unit: CompilationUnit): QueryResult =
    val compileResult = compiler.compileSingleUnit(unit)
    val result        = executor.executeSingle(unit, compileResult.context.withDebugRun(true))
    debug(result.toPrettyBox(maxWidth = Some(120)))
    result

  protected def handleResult(result: QueryResult): Unit =
    result
      .getWarning
      .foreach { w =>
        warn(w)
      }
    result.getError match
      case Some(e) =>
        throw e
      case None =>
      // ok

  protected def handleError: Throwable => Unit =
    case e: WvletLangException if e.statusCode.isUserError =>
      workEnv.errorLogger.error(e)
      fail(e.getMessage)
    case e: Throwable =>
      throw e

end SpecRunner

trait QuerySpec(specPath: String, profile: Profile = Profile.defaultDuckDBProfile, ignoreSpec: Map[String, String] = Map.empty) extends AirSpec:
  initDesign _ + SpecRunner.design(specPath, profile, logger.getLogLevel)
  
  def runQuery(): Unit = {
    // Compile all files in the source paths first
    for unit <- compiler.localCompilationUnits do
      test(unit.sourceFile.relativeFilePath.replaceAll("/", ":")) {
        ignoredSpec.get(unit.sourceFile.fileName).foreach(reason => ignore(reason))
        try
          handleResult(runSpec(unit))
        catch
          case NonFatal(e) =>
            handleError(e)
      }
  }
  
  
  
end QuerySpec


class BasicSpec
    extends SpecRunner(
      "spec/basic",
      ignoredSpec = Map("values.wv" -> "Need to support RawJSON data")
    )

class TPCHSpec extends SpecRunner("spec/tpch", profile = Profile.defaultDuckDBProfileWithTPCH)

// Negative tests, expecting some errors
class NegSpec extends SpecRunner("spec/neg"):
  override protected def handleError: Throwable => Unit =
    case e: WvletLangException if e.statusCode.isUserError =>
      // Expected error
      debug(e)
    case e: Throwable =>
      throw e
