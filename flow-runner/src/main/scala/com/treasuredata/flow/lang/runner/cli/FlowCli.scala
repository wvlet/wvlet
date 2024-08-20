package com.treasuredata.flow.lang.runner.cli

import com.treasuredata.flow.BuildInfo
import com.treasuredata.flow.lang.compiler.{CompilationUnit, Compiler, CompilerOptions}
import com.treasuredata.flow.lang.runner.connector.duckdb.DuckDBConnector
import com.treasuredata.flow.lang.runner.{QueryExecutor, QueryResultPrinter}
import com.treasuredata.flow.lang.{FlowLangException, StatusCode}
import wvlet.airframe.launcher.{Launcher, argument, command, option}
import wvlet.log.{LogLevel, LogSupport, Logger}

import java.io.File

/**
  * A command-line interface for the Flow compiler
  */
object FlowCli:
  private def withLauncher[U](body: Launcher => U): U =
    val l = Launcher.of[FlowCli]
    body(l)

  def main(argLine: String): Unit = withLauncher: l =>
    l.execute(argLine)

  def main(args: Array[String]): Unit = withLauncher: l =>
    l.execute(args)

case class FlowCliOption(
    @option(prefix = "-h,--help", description = "Display help message", isHelp = true)
    displayHelp: Boolean = false,
    @option(prefix = "--debug", description = "Enable debug log")
    debugMode: Boolean = false,
    @option(prefix = "-l", description = "log level")
    logLevel: LogLevel = LogLevel.INFO,
    @option(prefix = "-L", description = "log level for a class pattern")
    logLevelPatterns: List[String] = List.empty
) extends LogSupport:

  Logger("com.treasuredata.flow.lang.runner").setLogLevel {
    if debugMode then
      LogLevel.DEBUG
    else
      logLevel
  }

  def versionString =
    s"treasure-flow version: ${BuildInfo.version} (Built at: ${BuildInfo.builtAtString})"

  debug(versionString)

  logLevelPatterns.foreach { p =>
    p.split("=") match
      case Array(pattern, level) =>
        debug(s"Set the log level for ${pattern} to ${level}")
        Logger.setLogLevel(pattern, LogLevel(level))
      case _ =>
        error(s"Invalid log level pattern: ${p}")
  }

end FlowCliOption

class FlowCli(opts: FlowCliOption) extends LogSupport:
  @command(description = "Compile flow files")
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
        workingFolder = contextDirectory
      )
    ).compile()

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
      val parts            = targetFile.split("/src/")
      var flowFile: String = null
      val contextDirectory =
        parts.length match
          case 1 =>
            flowFile = parts(0)
            new File(".").getPath
          case 2 =>
            flowFile = parts(1)
            new File(parts(0)).getPath
          case _ =>
            throw StatusCode.INVALID_ARGUMENT.newException(s"Invalid file path: ${targetFile}")

      info(s"context directory: ${contextDirectory}, flow file: ${flowFile}")

      val duckdb = QueryExecutor(dbConnector = DuckDBConnector(prepareTPCH = prepareTPCH))
      val compilationResult = Compiler(
        CompilerOptions(
          phases = Compiler.allPhases,
          sourceFolders = List(contextDirectory),
          workingFolder = contextDirectory
        )
      ).compileSingle(Some(flowFile))
      compilationResult
        .context
        .global
        .getAllContexts
        .map(_.compilationUnit)
        .find(_.sourceFile.fileName == flowFile) match
        case Some(contextUnit) =>
          val ctx    = compilationResult.context.global.getContextOf(contextUnit)
          val result = duckdb.execute(contextUnit, ctx)
          val str    = result.toPrettyBox()
          if str.nonEmpty then
            println(str)
        case None =>
          throw StatusCode.INVALID_ARGUMENT.newException(s"Cannot find the context for ${flowFile}")
    catch
      case e: FlowLangException =>
        error(e.getMessage, e.getCause)

end FlowCli
