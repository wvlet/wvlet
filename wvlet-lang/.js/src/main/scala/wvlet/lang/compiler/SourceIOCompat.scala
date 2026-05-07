package wvlet.lang.compiler

/**
  * Stubbed file I/O for Scala.js. Returning safe defaults (instead of throwing) lets the browser
  * playground link the wvlet compiler without dragging Node-only modules (`os`, `fs`, `path`,
  * `zlib`) into the bundle. A future Node-targeted JS consumer can wire in the real
  * `wvlet.uni.io.IO` backend by calling `wvlet.uni.io.FileSystemInit.init()` from its own entry
  * point and providing thin wrappers.
  */
trait SourceIOCompat:
  def readAsString(filePath: String): String = ???

  def readGzipAsString(filePath: String): String =
    throw new UnsupportedOperationException(
      "Gzip reading is not supported in Scala.js (browser environment)"
    )

  def existsFile(path: String): Boolean     = false
  def isDirectory(path: String): Boolean    = false
  def listFiles(path: String): List[String] = Nil
  def lastUpdatedAt(path: String): Long     = 0L
  def fileName(path: String): String        = path.split("/").lastOption.getOrElse(path)

  def writeString(path: String, content: String): Unit = ()
