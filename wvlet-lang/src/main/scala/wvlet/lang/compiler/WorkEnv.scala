package wvlet.lang.compiler

import wvlet.lang.model.DataType.SchemaType
import wvlet.log.LogFormatter.SourceCodeLogFormatter
import wvlet.log.LogLevel.ALL
import wvlet.log.LogLevel
import wvlet.log.LogRotationHandler
import wvlet.log.Logger

/**
  * Cache entry for file schemas, keyed by file path and modification time.
  *
  * @param filePath
  *   Absolute path to the source file
  * @param schema
  *   Inferred schema from the file
  * @param lastModified
  *   File modification timestamp used for cache invalidation
  */
case class CachedFileSchema(filePath: String, schema: SchemaType, lastModified: Long)

/**
  * Working directory for finding .wv files and target folders for logs and cache
  * @param path
  * @param logLevel
  */
case class WorkEnv(path: String = ".", logLevel: LogLevel = Logger.getDefaultLogLevel)
    extends WorkEnvCompat:

  def targetFolder: String =
    if hasWvletFiles then
      s"${path}/target"
    else
      s"${sys.props("user.home")}/.cache/wvlet/target"

  def logFile: String   = s"${targetFolder}/wvlet-out.log"
  def errorFile: String = s"${targetFolder}/wvlet-err.log"

  def cacheFolder: String =
    if hasWvletFiles then
      // Use the target folder for the folder containing .wv files
      s"${targetFolder}/.cache/wvlet"
    else
      // Use the global folder at the user home for an arbitrary directory
      s"${sys.props("user.home")}/.cache/wvlet"

  val compilerLogger: Logger = Logger("wvlet.lang.runner")

  lazy val errorLogger: Logger =
    val l = Logger("wvlet.lang.runner.error")
    initLogger(l, errorFile)
    l.setLogLevel(logLevel)
    l

  lazy val outLogger: Logger =
    val l = Logger("wvlet.lang.runner.out")
    initLogger(l, logFile)
    l.setLogLevel(logLevel)
    l

  def trace(msg: => Any): Unit = outLogger.trace(msg)

  def debug(msg: => Any): Unit = outLogger.debug(msg)

  def info(msg: => Any): Unit = outLogger.info(msg)

  def warn(msg: => Any): Unit =
    compilerLogger.warn(msg)

    if !isScalaJS then
      outLogger.warn(msg)
      errorLogger.warn(msg)

  inline def logWarn(e: Throwable): Unit =
    val msg = e.getMessage
    compilerLogger.warn(msg)
    if !isScalaJS then
      outLogger.warn(msg)
      errorLogger.warn(msg, e)

  inline def logError(e: Throwable): Unit =
    val msg = e.getMessage
    compilerLogger.error(msg)
    if !isScalaJS then
      outLogger.error(msg)
      errorLogger.error(msg, e)

  inline def error(msg: => Any): Unit =
    compilerLogger.error(msg)
    if !isScalaJS then
      outLogger.error(msg)
      errorLogger.error(msg)

  /**
    * Schema cache folder location for cached file schemas.
    */
  def schemaCacheFolder: String = s"${cacheFolder}/schemas"

  /**
    * Load a cached file schema from the cache folder.
    *
    * @param filePath
    *   The file path to look up
    * @return
    *   Cached schema if exists and mtime matches, None otherwise
    */
  def loadFileSchemaCache(filePath: String): Option[CachedFileSchema] = loadFileSchemaCacheImpl(
    filePath
  )

  /**
    * Save a file schema to the cache folder.
    *
    * @param filePath
    *   The file path as cache key
    * @param schema
    *   The inferred schema to cache
    * @param lastModified
    *   File modification timestamp for invalidation
    */
  def saveFileSchemaCache(filePath: String, schema: SchemaType, lastModified: Long): Unit =
    saveFileSchemaCacheImpl(filePath, schema, lastModified)

end WorkEnv
