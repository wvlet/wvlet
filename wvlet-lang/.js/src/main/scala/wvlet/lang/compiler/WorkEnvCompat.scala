package wvlet.lang.compiler

import wvlet.log.Logger

trait WorkEnvCompat:
  self: WorkEnv =>
  lazy val hasWvletFiles: Boolean = false

  def isScalaJS: Boolean = true

  def saveToCache(path: String, content: String): Unit = {
    // no-op
  }

  def loadCache(path: String): Option[VirtualFile] = None

  protected def initLogger(l: Logger, fileName: String): Logger = l
