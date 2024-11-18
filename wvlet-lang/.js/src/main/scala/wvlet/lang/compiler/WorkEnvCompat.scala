package wvlet.lang.compiler

import wvlet.log.Logger

trait WorkEnvCompat:
  self: WorkEnv =>
  protected def initLogger(l: Logger): Logger = l
