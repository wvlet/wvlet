package wvlet.lang.compiler

import wvlet.airframe.control.IO
import wvlet.log.Logger

trait WorkEnvCompat:
  self: WorkEnv =>
  val hasWvletFiles: Boolean = Option(new java.io.File(path).listFiles())
    .exists(_.exists(_.getName.endsWith(".wv")))

  def isScalaJS: Boolean = false

  def saveToCache(path: String, content: String): Unit =
    val f = new java.io.File(s"${cacheFolder}/${path}")
    Option(f.getParentFile).foreach(_.mkdirs())
    val out = new java.io.PrintWriter(f)
    try out.write(content)
    finally out.close()

  def loadCache(path: String): Option[VirtualFile] =
    val f = new java.io.File(s"${cacheFolder}/${path}")
    if f.exists() then
      Some(LocalFile(path = f.getPath))
    else
      None

  protected def initLogger(l: Logger, fileName: String): Logger = l
