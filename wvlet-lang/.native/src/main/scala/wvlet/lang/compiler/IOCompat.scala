package wvlet.lang.compiler

import wvlet.airframe.control.Control.withResource

import java.net.URI
import java.io.File
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

trait IOCompat:
  self: SourceIO.type =>

  def readAsString(filePath: String): String =
    val bytes = java.nio.file.Files.newInputStream(java.nio.file.Paths.get(filePath)).readAllBytes()
    new String(bytes, java.nio.charset.StandardCharsets.UTF_8)

  def readGzipAsString(filePath: String): String =
    withResource(new java.util.zip.GZIPInputStream(Files.newInputStream(Path.of(filePath)))) {
      gzipInputStream =>
        new String(gzipInputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
    }

  def readAsString(uri: java.net.URI): String = ???

  def listResources(path: String): List[VirtualFile] = listFiles(path).map(f =>
    URIResource(File(f).toURI)
  )

  def existsFile(path: String): Boolean = new java.io.File(path).exists()

  def lastUpdatedAt(path: String): Long = Files.getLastModifiedTime(Path.of(path)).toMillis

  def listFiles(path: String): List[String] =
    Files.list(Path.of(path)).toList.asScala.map(_.toString).toList

  def fileName(path: String): String     = path.split("/").lastOption.getOrElse(path)
  def isDirectory(path: String): Boolean = Files.isDirectory(Path.of(path))

  // Methods from FileIOCompat
  def isDirectory(path: Any): Boolean =
    path match
      case p: Path =>
        Files.exists(p) && Files.isDirectory(p)
      case s: String =>
        isDirectory(s)
      case _ =>
        false

  def listDirectories(path: Any): List[String] =
    path match
      case p: Path =>
        if Files.exists(p) && Files.isDirectory(p) then
          Files
            .list(p)
            .iterator()
            .asScala
            .filter(Files.isDirectory(_))
            .map(_.getFileName.toString)
            .toList
        else
          List.empty
      case s: String =>
        listDirectories(Path.of(s))
      case _ =>
        List.empty

  def resolvePath(basePath: Any, segments: String*): Any =
    basePath match
      case p: Path =>
        segments.foldLeft(p)((path, segment) => path.resolve(segment))
      case s: String =>
        val path = Path.of(s)
        segments.foldLeft(path)((p, segment) => p.resolve(segment))
      case _ =>
        basePath

  def readFileIfExists(path: Any): Option[String] =
    path match
      case p: Path =>
        if Files.exists(p) then
          Some(Files.readString(p))
        else
          None
      case s: String =>
        readFileIfExists(Path.of(s))
      case _ =>
        None

end IOCompat
