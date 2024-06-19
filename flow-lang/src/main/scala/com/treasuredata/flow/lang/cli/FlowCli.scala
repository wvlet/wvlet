package com.treasuredata.flow.lang.cli

import com.treasuredata.flow.BuildInfo
import com.treasuredata.flow.lang.compiler.Compiler
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
    logLevel: Option[LogLevel] = None
)

class FlowCli(opts: FlowCliOption) extends LogSupport:
  opts.logLevel.foreach: l =>
    Logger("com.treasuredata.flow.lang").setLogLevel(l)

  @command(isDefault = true)
  def default: Unit = info(s"treasure-flow version: ${BuildInfo.version}")

  @command(description = "Compile flow files")
  def compile(
      @argument(description = "source folders to compile")
      sourceFolders: Array[String]
  ): Unit =
    debug(s"source folders: ${sourceFolders.mkString(", ")}")
    debug(s"current directory: ${new File(".").getAbsolutePath}")
    val compileResult = Compiler(Compiler.allPhases).compile(sourceFolders.toList)
    compileResult.typedPlans.collect:
      case p => debug(p.pp)
