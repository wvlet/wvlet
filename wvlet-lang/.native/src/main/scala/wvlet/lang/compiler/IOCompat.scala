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

  def listResource(path: String): List[URI] = listFiles(path, 0).map(File(_).toURI).toList

  def existsFile(path: String): Boolean = new java.io.File(path).exists()

  def lastUpdatedAt(path: String): Long = Files.getLastModifiedTime(Path.of(path)).toMillis

  def listWvFiles(path: String, level: Int): Seq[String] =
    val f = new java.io.File(path)
    if f.isDirectory then
      if level == 1 && ignoredFolders.contains(f.getName) then
        Seq.empty
      else
        val files         = f.listFiles()
        val hasAnyWvFiles = files.exists(_.getName.endsWith(".wv"))
        if hasAnyWvFiles then
          // Only scan sub-folders if there is any .wv files
          files flatMap { file =>
            listFiles(file.getPath, level + 1)
          }
        else
          Seq.empty
    else if f.isFile && f.getName.endsWith(".wv") then
      Seq(f.getPath)
    else
      Seq.empty

  def listFiles(path: String): Seq[String] = Files
    .list(Path.of(path))
    .toList
    .asScala
    .map(_.toString)

end IOCompat
