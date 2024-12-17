package wvlet.lang.compiler

import wvlet.log.Logger

trait WorkEnvCompat:
  self: WorkEnv =>
  protected def initLogger(l: Logger, fileName: String): Logger = l
