package wvlet.lang.compiler

import wvlet.airframe.control.Control
import wvlet.airframe.control.Control.withResource

import java.io.File
import java.net.URI
import java.nio.file.Path
import java.nio.file.Files
import java.util.jar.JarFile
import scala.jdk.CollectionConverters.*

trait IOCompat:
  self: SourceIO.type =>

  def readAsString(filePath: String): String = wvlet
    .airframe
    .control
    .IO
    .readAsString(Path.of(filePath).toFile)

  def readAsString(uri: URI): String =
    withResource(uri.toURL.openStream()) { in =>
      wvlet.airframe.control.IO.readAsString(in)
    }

  def listResource(path: String): List[URI] =
    val uris = List.newBuilder[URI]
    import scala.jdk.CollectionConverters.*
    Option(this.getClass.getResource(path)).foreach: r =>
      r.getProtocol match
        case "file" =>
          val files = listWvFiles(r.getPath, 0)
          uris ++= files.map(File(_).toURI)
        case "jar" =>
          val jarPath     = r.getPath.split("!")(0).replaceAll("%20", " ").replaceAll("%25", "%")
          val jarFilePath = jarPath.replace("file:", "")
          val jf          = new JarFile(jarFilePath)
          val wvFilePaths = jf.entries().asScala.filter(_.getName.endsWith(".wv"))
          uris ++=
            wvFilePaths
              .map { j =>
                val url = s"jar:${jarPath}!/${j.getName}"
                URI(url)
              }
              .toList
        case _ =>
    uris.result()

  def listFiles(path: String): Seq[String] =
    Files.list(Path.of(path)).toList.asScala.map(_.toString).toSeq

  def listWvFiles(path: String, level: Int): Seq[String] =
    val f = new java.io.File(path)
    if f.isDirectory then
      if level == 1 && SourceIO.ignoredFolders.contains(f.getName) then
        Seq.empty
      else
        val files         = f.listFiles()
        val hasAnyWvFiles = files.exists(_.getName.endsWith(".wv"))
        if hasAnyWvFiles then
          // Only scan sub-folders if there is any .wv files
          files flatMap { file =>
            listWvFiles(file.getPath, level + 1)
          }
        else
          Seq.empty
    else if f.isFile && f.getName.endsWith(".wv") then
      Seq(f.getPath)
    else
      Seq.empty

  def existsFile(path: String): Boolean = new java.io.File(path).exists()

  def lastUpdatedAt(path: String): Long  = Files.getLastModifiedTime(Path.of(path)).toMillis
  def fileName(path: String): String     = path.split("/").lastOption.getOrElse(path)
  def isDirectory(path: String): Boolean = Files.isDirectory(Path.of(path))

end IOCompat
