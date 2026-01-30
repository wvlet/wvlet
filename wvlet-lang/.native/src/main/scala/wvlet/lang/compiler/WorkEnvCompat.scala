package wvlet.lang.compiler

import wvlet.lang.model.DataType.SchemaType
import wvlet.log.Logger

import java.io.File
import java.security.MessageDigest

trait WorkEnvCompat:
  self: WorkEnv =>
  val hasWvletFiles: Boolean = Option(new java.io.File(path).listFiles()).exists(
    _.exists(_.getName.endsWith(".wv"))
  )

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

  // File schema cache implementation for Scala Native

  /**
    * Generate a cache key from file path using MD5 hash.
    */
  private def cacheKeyFor(filePath: String): String =
    val digest = MessageDigest.getInstance("MD5")
    val hash   = digest.digest(filePath.getBytes("UTF-8"))
    hash.map("%02x".format(_)).mkString + ".schema"

  /**
    * Load a cached file schema from disk if it exists and the mtime matches.
    */
  protected def loadFileSchemaCacheImpl(filePath: String): Option[CachedFileSchema] =
    // TODO: Implement full schema deserialization from cache.
    // This is a placeholder for now and does not load from cache.
    None

  /**
    * Save a file schema to disk cache.
    */
  protected def saveFileSchemaCacheImpl(
      filePath: String,
      schema: SchemaType,
      lastModified: Long
  ): Unit =
    val cacheKey  = cacheKeyFor(filePath)
    val cacheFile = new File(s"${schemaCacheFolder}/${cacheKey}")
    Option(cacheFile.getParentFile).foreach(_.mkdirs())

    // For now, store basic metadata
    // Full schema serialization would require a schema serializer
    val content = s"${filePath}\n${lastModified}\n${schema.typeName}"
    val out     = new java.io.PrintWriter(cacheFile)
    try out.write(content)
    finally out.close()

end WorkEnvCompat
