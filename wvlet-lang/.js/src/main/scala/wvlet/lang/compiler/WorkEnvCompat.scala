package wvlet.lang.compiler

import wvlet.lang.model.DataType.SchemaType
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

  // File schema cache implementation for Scala.js (in-memory only)

  /**
    * Load a cached file schema - no persistence in Scala.js.
    */
  protected def loadFileSchemaCacheImpl(filePath: String): Option[CachedFileSchema] = None

  /**
    * Save a file schema - no-op in Scala.js.
    */
  protected def saveFileSchemaCacheImpl(
      filePath: String,
      schema: SchemaType,
      lastModified: Long
  ): Unit = {
    // no-op - Scala.js has no file system access
  }

end WorkEnvCompat
