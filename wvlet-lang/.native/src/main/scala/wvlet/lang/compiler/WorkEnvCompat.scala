package wvlet.lang.compiler

import wvlet.log.Logger

trait WorkEnvCompat:
  self: WorkEnv =>
  val hasWvletFiles: Boolean = Option(new java.io.File(path).listFiles())
    .exists(_.exists(_.getName.endsWith(".wv")))

  def isScalaJS: Boolean = false

  protected def initLogger(l: Logger, fileName: String): Logger = l
