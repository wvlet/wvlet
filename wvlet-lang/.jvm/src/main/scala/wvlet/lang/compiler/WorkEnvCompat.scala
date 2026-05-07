package wvlet.lang.compiler

import wvlet.uni.log.LogFormatter.SourceCodeLogFormatter
import wvlet.uni.log.LogRotationHandler
import wvlet.uni.log.Logger

trait WorkEnvCompat:
  self: WorkEnv =>

  def isScalaJS: Boolean = false

  protected def initLogger(l: Logger, fileName: String): Logger =
    l.resetHandler(LogRotationHandler(fileName = fileName, formatter = SourceCodeLogFormatter))
    l
