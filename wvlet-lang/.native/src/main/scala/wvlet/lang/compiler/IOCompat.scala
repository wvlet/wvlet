package wvlet.lang.compiler

import java.net.URI
import java.io.File
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

trait IOCompat:
  self: SourceIO.type =>

  def readAsString(filePath: String): String =
    val bytes = java.nio.file.Files.newInputStream(java.nio.file.Paths.get(filePath)).readAllBytes()
    new String(bytes, java.nio.charset.StandardCharsets.UTF_8)

  def readAsString(uri: java.net.URI): String = ???

  def listResources(path: String): List[VirtualFile] = listFiles(path)
    .map(f => URIResource(File(f).toURI))

  def existsFile(path: String): Boolean = new java.io.File(path).exists()

  def lastUpdatedAt(path: String): Long = Files.getLastModifiedTime(Path.of(path)).toMillis

  def listFiles(path: String): List[String] =
    Files.list(Path.of(path)).toList.asScala.map(_.toString).toList

  def fileName(path: String): String     = path.split("/").lastOption.getOrElse(path)
  def isDirectory(path: String): Boolean = Files.isDirectory(Path.of(path))

end IOCompat
