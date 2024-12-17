package wvlet.lang.compiler

import wvlet.log.Logger

trait WorkEnvCompat:
  self: WorkEnv =>
  lazy val hasWvletFiles: Boolean = false

  def isScalaJS: Boolean = true

  protected def initLogger(l: Logger, fileName: String): Logger = l
