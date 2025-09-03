package wvlet.lang.compiler

import java.net.URI
import java.io.File

trait IOCompat:
  self: SourceIO.type =>

  def readAsString(filePath: String): String  = ???
  def readAsString(uri: java.net.URI): String = ???
  def listResources(path: String): List[VirtualFile] =
    listFiles(path).map(f => URIResource(File(f).toURI)).toList

  def existsFile(path: String): Boolean     = false
  def lastUpdatedAt(path: String): Long     = ???
  def listFiles(path: String): List[String] = Nil
  def fileName(path: String): String        = path.split("/").lastOption.getOrElse(path)
  def fileExists(path: String): Boolean     = false
  def isDirectory(path: String): Boolean    = false

  // Methods from FileIOCompat
  def isDirectory(path: Any): Boolean                    = false
  def listDirectories(path: Any): List[String]           = List.empty
  def resolvePath(basePath: Any, segments: String*): Any = basePath
  def readFileIfExists(path: Any): Option[String]        = None

  def readAsGzString(filePath: String): String =
    throw new UnsupportedOperationException("Gzip file reading not supported in browser environment")
