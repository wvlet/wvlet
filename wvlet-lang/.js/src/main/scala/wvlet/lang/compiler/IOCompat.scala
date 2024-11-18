package wvlet.lang.compiler

import java.net.URI
import java.io.File

trait IOCompat:
  self: SourceIO.type =>

  def readAsString(filePath: String): String  = ???
  def readAsString(uri: java.net.URI): String = ???
  def listResource(path: String): List[URI]   = listFiles(path, 0).map(File(_).toURI).toList
