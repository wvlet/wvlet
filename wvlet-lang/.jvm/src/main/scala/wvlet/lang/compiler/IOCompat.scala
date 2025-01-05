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

  def listResources(path: String): List[VirtualFile] =
    val resources = List.newBuilder[VirtualFile]
    import scala.jdk.CollectionConverters.*
    Option(this.getClass.getResource(path)).foreach: r =>
      r.getProtocol match
        case "file" =>
          val f = LocalFile(r.getPath)
          resources += f
          if f.isDirectory then
            resources ++= f.listFilesRecursively
        case "jar" =>
          val jarPath     = r.getPath.split("!")(0).replaceAll("%20", " ").replaceAll("%25", "%")
          val jarFilePath = jarPath.replace("file:", "")
          val jf          = new JarFile(jarFilePath)
          jf.entries()
            .asScala
            .foreach { j =>
              val url = s"jar:${jarPath}!/${j.getName}"
              resources += URIResource(URI(url))
            }
        case _ =>
    resources.result()

  def listFiles(path: String): List[String] =
    Files.list(Path.of(path)).toList.asScala.map(_.toString).toList

  def existsFile(path: String): Boolean = new java.io.File(path).exists()

  def lastUpdatedAt(path: String): Long  = Files.getLastModifiedTime(Path.of(path)).toMillis
  def fileName(path: String): String     = path.split("/").lastOption.getOrElse(path)
  def isDirectory(path: String): Boolean = Files.isDirectory(Path.of(path))

end IOCompat
