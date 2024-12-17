package wvlet.lang.compiler

import wvlet.log.LogFormatter.SourceCodeLogFormatter
import wvlet.log.{LogRotationHandler, Logger}

trait WorkEnvCompat:
  self: WorkEnv =>
  protected def initLogger(l: Logger, fileName: String): Logger =
    l.resetHandler(LogRotationHandler(fileName = fileName, formatter = SourceCodeLogFormatter))
    l
