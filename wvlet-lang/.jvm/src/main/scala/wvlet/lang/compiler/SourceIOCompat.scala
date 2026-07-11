package wvlet.lang.compiler

import wvlet.uni.io.FileSystemInit
import wvlet.uni.io.Gzip
import wvlet.uni.io.IO
import wvlet.uni.io.IOPath
import wvlet.uni.io.ListOptions
import wvlet.uni.io.WriteMode

import java.nio.charset.StandardCharsets

trait SourceIOCompat:
  // Bind uni's JVM FileSystem impl once at class-load time so callers don't need to know about
  // FileSystemInit. Same trait body lives in the Native folder — uni's `IO` API works
  // identically on both backends, but cross-project layout doesn't natively let JVM and Native
  // share a single source folder.
  FileSystemInit.init()

  def readAsString(filePath: String): String = IO.readString(IOPath.parse(filePath))

  def readGzipAsString(filePath: String): String =
    new String(Gzip.decompress(IO.readBytes(IOPath.parse(filePath))), StandardCharsets.UTF_8)

  def existsFile(path: String): Boolean = IO.exists(IOPath.parse(path))

  def isDirectory(path: String): Boolean =
    val p = IOPath.parse(path)
    IO.exists(p) && IO.isDirectory(p)

  def listFiles(path: String): List[String] =
    val p = IOPath.parse(path)
    if IO.exists(p) && IO.isDirectory(p) then
      IO.list(p, ListOptions.default).map(_.path).toList
    else
      Nil

  def lastUpdatedAt(path: String): Long =
    val p = IOPath.parse(path)
    if IO.exists(p) then
      IO.info(p).lastModified.map(_.toEpochMilli).getOrElse(0L)
    else
      0L

  def fileName(path: String): String = path.split("/").lastOption.getOrElse(path)

  def writeString(path: String, content: String): Unit =
    val target = IOPath.parse(path)
    target.parent.foreach(IO.createDirectoryIfNotExists)
    IO.writeString(target, content, WriteMode.Create)

  def deleteFile(path: String): Unit = IO.deleteIfExists(IOPath.parse(path))

end SourceIOCompat
