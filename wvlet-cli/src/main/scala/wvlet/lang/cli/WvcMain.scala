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
package wvlet.lang.cli

import wvlet.airframe.control.Control
import wvlet.airframe.launcher.{Launcher, argument, command, option}
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.lang.runner.{QueryExecutor, QueryResultPrinter}
import wvlet.lang.BuildInfo
import wvlet.lang.api.{StatusCode, WvletLangException}
import wvlet.log.{LogLevel, LogSupport, Logger}

import java.io.File

/**
  * A command-line interface for the wvlet compiler (wvc)
  */
object WvcMain:
  private def withLauncher[U](body: Launcher => U): U =
    val l = Launcher.of[WvcMain]
    body(l)

  def main(argLine: String): Unit = withLauncher: l =>
    l.execute(argLine)

  def main(args: Array[String]): Unit = withLauncher: l =>
    l.execute(args)

class WvcMain(opts: WvletGlobalOption) extends LogSupport:

  @command(description = "show the help", isDefault = true)
  def defaultMessage: Unit = info(s"Type --help to see the list of available commands")

  @command(description = "Compile .wv files")
  def compile(
      @argument(description = "source folders to compile")
      sourceFolders: Array[String]
  ): Unit =
    debug(s"source folders: ${sourceFolders.mkString(", ")}")
    val contextDirectory = sourceFolders.headOption.getOrElse(new File(".").getAbsolutePath)
    debug(s"context directory: ${contextDirectory}")
    val compileResult = Compiler(
      CompilerOptions(
        phases = Compiler.allPhases,
        sourceFolders = sourceFolders.toList,
        workEnv = WorkEnv(contextDirectory, opts.logLevel)
      )
    ).compile()

    compileResult.reportAllErrors

    compileResult
      .typedPlans
      .collect:
        case p =>
          debug(p.pp)

  @command(description = "Run queries in a given file")
  def run(
      @option(prefix = "--tpch", description = "Prepare TPC-H data (scale factor 0.01) for testing")
      prepareTPCH: Boolean = false,
      @argument(description = "target query file to run")
      targetFile: String
  ): Unit =
    try
      val parts          = targetFile.split("/src/")
      var wvFile: String = null
      val contextDirectory =
        parts.length match
          case 1 =>
            wvFile = parts(0)
            new File(".").getPath
          case 2 =>
            wvFile = parts(1)
            new File(parts(0)).getPath
          case _ =>
            throw StatusCode.INVALID_ARGUMENT.newException(s"Invalid file path: ${targetFile}")

      info(s"context directory: ${contextDirectory}, wvlet file: ${wvFile}")

      val workEnv = WorkEnv(path = contextDirectory, opts.logLevel)
      Control.withResource(
        QueryExecutor(dbConnector = DuckDBConnector(prepareTPCH = prepareTPCH), workEnv)
      ): duckdb =>
        val compilationResult = Compiler(
          CompilerOptions(
            phases = Compiler.allPhases,
            sourceFolders = List(contextDirectory),
            workEnv = workEnv
          )
        ).compileSourcePaths(Some(wvFile))
        compilationResult
          .context
          .global
          .getAllContexts
          .map(_.compilationUnit)
          .find(_.sourceFile.fileName == wvFile) match
          case Some(contextUnit) =>
            val ctx    = compilationResult.context.global.getContextOf(contextUnit)
            val result = duckdb.executeSingle(contextUnit, ctx)
            val str    = result.toPrettyBox()
            if str.nonEmpty then
              println(str)
          case None =>
            throw StatusCode.INVALID_ARGUMENT.newException(s"Cannot find the context for ${wvFile}")
    catch
      case e: WvletLangException =>
        error(e.getMessage, e.getCause)

end WvcMain
