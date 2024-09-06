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
package wvlet.lang.runner.cli

import wvlet.lang.WvletLangException
import wvlet.lang.compiler.{CompilationUnit, CompileResult, Compiler, CompilerOptions}
import wvlet.lang.runner.*
import org.jline.terminal.Terminal
import wvlet.airframe.control.{Control, Shell}
import wvlet.log.LogSupport

import java.io.{BufferedWriter, FilterOutputStream, OutputStreamWriter}
import scala.util.control.NonFatal

case class WvletScriptRunnerConfig(
    workingFolder: String = ".",
    interactive: Boolean,
    resultLimit: Int = 40,
    maxColWidth: Int = 150,
    catalog: Option[String],
    schema: Option[String]
)

case class LastOutput(
    line: String,
    output: String,
    result: QueryResult,
    error: Option[Throwable] = None
):
  def hasError: Boolean = error.isDefined

class WvletScriptRunner(val config: WvletScriptRunnerConfig, queryExecutor: QueryExecutor)
    extends AutoCloseable
    with LogSupport:
  private var units: List[CompilationUnit] = Nil

  private var resultRowLimits: Int   = config.resultLimit
  private var resultMaxColWidth: Int = config.maxColWidth

  def getResultRowLimit: Int              = resultRowLimits
  def setResultRowLimit(limit: Int): Unit = resultRowLimits = limit
  def setMaxColWidth(size: Int): Unit     = resultMaxColWidth = size

  override def close(): Unit = queryExecutor.close()

  private val compiler =
    val c = Compiler(
      CompilerOptions(
        sourceFolders = List(config.workingFolder),
        workingFolder = config.workingFolder,
        catalog = config.catalog,
        schema = config.schema
      )
    )

    // Set the default catalog given in the configuration
    config
      .catalog
      .foreach { catalog =>
        c.setDefaultCatalog(
          queryExecutor.getDBConnector.getCatalog(catalog, config.schema.getOrElse("main"))
        )
      }
    config
      .schema
      .foreach { schema =>
        c.setDefaultSchema(schema)
      }

    // Pre-compile files in the source paths
    c.compileSourcePaths(None)
    c

  def runStatement(line: String): QueryResult =
    val newUnit = CompilationUnit.fromString(line)
    units = newUnit :: units

    try
      val compileResult = compiler.compileSingleUnit(contextUnit = newUnit)
      val ctx           = compileResult.context.global.getContextOf(newUnit)
      val queryResult   = queryExecutor.executeSingle(newUnit, ctx, limit = resultRowLimits)
      trace(s"ctx: ${ctx.hashCode()} ${ctx.compilationUnit.knownSymbols}")

      queryResult
    catch
      case NonFatal(e) =>
        ErrorResult(e)
    end try

  end runStatement

  def displayOutput(query: String, queryResult: QueryResult, terminal: Terminal): LastOutput =
    def print: LastOutput =
      val str            = queryResult.toPrettyBox(maxColWidth = resultMaxColWidth)
      val resultMaxWidth = str.split("\n").map(_.size).max
      if !config.interactive || resultMaxWidth <= terminal.getWidth then
        // The result fits in the terminal width
        println(
          queryResult
            .toPrettyBox(maxWidth = Some(terminal.getWidth), maxColWidth = resultMaxColWidth)
        )
      else
        // Launch less command to enable scrolling of query results in the terminal
        // TODO Use jline3's internal less
        val proc = ProcessUtil.launchInteractiveProcess("less", "-FXRSn")
        val out =
          new BufferedWriter(
            new OutputStreamWriter(
              // Need to use a FilterOutputStream to accept keyboard events for less command along with the query result string
              new FilterOutputStream(proc.getOutputStream())
            )
          )
        out.write(str)
        out.flush()
        out.close()
        // Blocks until the process is finished
        proc.waitFor()

      LastOutput(query, str, queryResult)
    end print

    queryResult.getError match
      case None =>
        print
      case Some(e) =>
        error(e)
        LastOutput(query, e.getMessage, QueryResult.empty, error = Some(e))

  end displayOutput

end WvletScriptRunner
