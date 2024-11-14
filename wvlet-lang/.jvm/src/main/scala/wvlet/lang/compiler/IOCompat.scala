package wvlet.lang.compiler

import wvlet.airframe.control.Control
import wvlet.airframe.control.Control.withResource

import java.io.File
import java.net.URI
import java.nio.file.Path
import java.util.jar.JarFile

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
          val files = listFiles(r.getPath, 0)
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

end IOCompat
