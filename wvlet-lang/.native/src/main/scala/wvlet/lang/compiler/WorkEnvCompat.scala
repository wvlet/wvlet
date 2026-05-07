package wvlet.lang.compiler

import wvlet.uni.log.Logger

trait WorkEnvCompat:
  self: WorkEnv =>

  def isScalaJS: Boolean = false

  // uni-log on Scala Native does not ship LogRotationHandler — keep the logger as-is.
  protected def initLogger(l: Logger, fileName: String): Logger = l
