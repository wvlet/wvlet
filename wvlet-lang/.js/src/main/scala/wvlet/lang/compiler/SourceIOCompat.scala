package wvlet.lang.compiler

import wvlet.uni.io.FileSystemInit
import wvlet.uni.io.Gzip
import wvlet.uni.io.IO
import wvlet.uni.io.IOPath
import wvlet.uni.io.ListOptions
import wvlet.uni.io.WriteMode

import java.nio.charset.StandardCharsets

trait SourceIOCompat:
  // Bind uni's JS FileSystem impl (Node.js fs/path/os/zlib) once at class-load time. Same
  // body lives in `.jvm` and `.native` — uni's `IO` API is identical across backends, but the
  // crossProject layout doesn't natively share a single source folder. wvlet-lang.js targets
  // Node only; browser embedding (e.g. the legacy playground) is out of scope.
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
