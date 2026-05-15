package wvlet.lang.compiler

import wvlet.uni.log.FileLogHandler
import wvlet.uni.log.FileLogHandlerConfig
import wvlet.uni.log.LogFormatter.SourceCodeLogFormatter
import wvlet.uni.log.Logger

trait WorkEnvCompat:
  self: WorkEnv =>

  def isScalaJS: Boolean = false

  protected def initLogger(l: Logger, fileName: String): Logger =
    val config = FileLogHandlerConfig(fileName).withFormatter(SourceCodeLogFormatter)
    l.resetHandler(FileLogHandler(config))
    l
