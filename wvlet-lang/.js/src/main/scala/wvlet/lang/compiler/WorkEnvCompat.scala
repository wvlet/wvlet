package wvlet.lang.compiler

import wvlet.uni.log.Logger

trait WorkEnvCompat:
  self: WorkEnv =>

  def isScalaJS: Boolean = true

  // uni-log on Scala.js does not ship LogRotationHandler — keep the logger as-is. On Node, log
  // writes route through the default console handler instead of a rotating file.
  protected def initLogger(l: Logger, fileName: String): Logger = l
