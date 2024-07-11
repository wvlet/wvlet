package com.treasuredata.flow.lang.cli

import com.treasuredata.flow.BuildInfo
import com.treasuredata.flow.lang.{FlowLangException, StatusCode}
import com.treasuredata.flow.lang.compiler.Compiler
import com.treasuredata.flow.lang.runner.DuckDBExecutor
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
    @option(prefix = "-h,--help", description = "Display help message")
    displayHelp: Boolean = false,
    @option(prefix = "-l", description = "log level")
    logLevel: LogLevel = LogLevel.INFO
)

class FlowCli(opts: FlowCliOption) extends LogSupport:
  Logger("com.treasuredata.flow.lang").setLogLevel(opts.logLevel)

  @command(isDefault = true)
  def default: Unit = info(s"treasure-flow version: ${BuildInfo.version}")

  @command(description = "Compile flow files")
  def compile(
      @argument(description = "source folders to compile")
      sourceFolders: Array[String]
  ): Unit =
    debug(s"source folders: ${sourceFolders.mkString(", ")}")
    val contextDirectory = sourceFolders.headOption.getOrElse(new File(".").getAbsolutePath)
    debug(s"context directory: ${contextDirectory}")
    val compileResult = Compiler(Compiler.allPhases).compile(sourceFolders.toList, contextDirectory)
    compileResult
      .typedPlans
      .collect:
        case p =>
          debug(p.pp)

  @command(description = "Run queries in a given file")
  def run(
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

      info(s"context directory: ${contextDirectory}")
      val compilationResult = Compiler(Compiler.allPhases).compile(contextDirectory)
      compilationResult.units.find(_.sourceFile.fileName == flowFile) match
        case Some(unit) =>
          val ctx      = compilationResult.context.global.getContextOf(unit)
          val executor = DuckDBExecutor.execute(unit, ctx)
        case None =>
          throw StatusCode.INVALID_ARGUMENT.newException(s"File not found: ${targetFile}")
    catch
      case e: FlowLangException =>
        error(e.getMessage, e)

end FlowCli
