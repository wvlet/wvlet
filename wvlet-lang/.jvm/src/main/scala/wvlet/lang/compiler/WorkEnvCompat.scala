package wvlet.lang.compiler

import wvlet.log.LogFormatter.SourceCodeLogFormatter
import wvlet.log.{LogRotationHandler, Logger}

trait WorkEnvCompat:
  self: WorkEnv =>
  protected def initLogger(l: Logger): Logger =
    l.resetHandler(LogRotationHandler(fileName = errorFile, formatter = SourceCodeLogFormatter))
    l
