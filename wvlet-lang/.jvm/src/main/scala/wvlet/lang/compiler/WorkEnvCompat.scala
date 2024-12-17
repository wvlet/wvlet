package wvlet.lang.compiler

import wvlet.log.LogFormatter.SourceCodeLogFormatter
import wvlet.log.{LogRotationHandler, Logger}

trait WorkEnvCompat:
  self: WorkEnv =>
  lazy val hasWvletFiles: Boolean = Option(new java.io.File(path).listFiles())
    .exists(_.exists(_.getName.endsWith(".wv")))

  def isScalaJS: Boolean = false

  protected def initLogger(l: Logger, fileName: String): Logger =
    l.resetHandler(LogRotationHandler(fileName = fileName, formatter = SourceCodeLogFormatter))
    l
