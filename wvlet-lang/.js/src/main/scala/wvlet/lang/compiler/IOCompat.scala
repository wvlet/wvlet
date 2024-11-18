package wvlet.lang.compiler

import java.net.URI
import java.io.File

trait IOCompat:
  self: SourceIO.type =>

  def readAsString(filePath: String): String  = ???
  def readAsString(uri: java.net.URI): String = ???
  def listResource(path: String): List[URI]   = listWvFiles(path, 0).map(File(_).toURI).toList

  def existsFile(path: String): Boolean                  = false
  def lastUpdatedAt(path: String): Long                  = ???
  def listWvFiles(path: String, level: Int): Seq[String] = Seq.empty
  def listFiles(path: String): Seq[String]               = Seq.empty
  def fileName(path: String): String     = path.split("/").lastOption.getOrElse(path)
  def fileExists(path: String): Boolean  = false
  def isDirectory(path: String): Boolean = false
