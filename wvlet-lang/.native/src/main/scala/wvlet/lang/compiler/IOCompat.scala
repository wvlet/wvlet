package wvlet.lang.compiler

import java.net.URI
import java.io.File

trait IOCompat:
  self: SourceIO.type =>

  def readAsString(filePath: String): String =
    val bytes = java.nio.file.Files.newInputStream(java.nio.file.Paths.get(filePath)).readAllBytes()
    new String(bytes, java.nio.charset.StandardCharsets.UTF_8)

  def readAsString(uri: java.net.URI): String = ???

  def listResource(path: String): List[URI] = listFiles(path, 0).map(File(_).toURI).toList
