/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.compiler

import wvlet.log.io.IOUtil

import java.io.File
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*
import scala.util.Try

/**
  * An abstraction over local files or files in remote GitHub repositories
  */
trait VirtualFile extends Ordered[VirtualFile]:
  /**
    * Leaf file name
    */
  def name: String

  /**
    * relative file path
    * @return
    */
  def path: String

  def exists: Boolean
  def isDirectory: Boolean
  def isFile: Boolean = !isDirectory
  def listFiles: List[VirtualFile]
  def listFilesRecursively: List[VirtualFile] = listFiles.flatMap { f =>
    if f.isDirectory then
      f :: f.listFilesRecursively
    else
      List(f)
  }

  /**
    * Last updated time in milliseconds
    * @return
    */
  def lastUpdatedAt: Long
  def contentString: String
  def content: IArray[Char] = IArray.unsafeFromArray(contentString.toCharArray)

  def isWv: Boolean         = name.endsWith(".wv")
  def isSQL: Boolean        = name.endsWith(".sql")
  def isMarkdown: Boolean   = name.endsWith(".md")
  def isSourceFile: Boolean = isWv || isSQL

  override def compare(other: VirtualFile): Int =
    def split(s: String): List[Any] =
      val alphabetOrNumber = """[^\d]+|\d+""".r
      val num              = "[0-9]+".r
      val parts = alphabetOrNumber
        .findAllIn(s)
        .toList
        .map {
          case s if num.matches(s) =>
            Try(s.toLong).getOrElse(s)
          case x =>
            x
        }
      parts

    def cmp(x: List[Any], y: List[Any]): Int =
      (x, y) match
        case (Nil, Nil) =>
          0
        case (Nil, _) =>
          -1
        case (_, Nil) =>
          1
        case (hx :: tx, hy :: ty) =>
          (hx, hy) match
            case (hx: String, hy: String) =>
              val c = hx.compareTo(hy)
              if c == 0 then
                cmp(tx, ty)
              else
                c
            case (hx: Long, hy: Long) =>
              val c = hx.compareTo(hy)
              if c == 0 then
                cmp(tx, ty)
              else
                c
            case (hx: String, hy: Long) =>
              -1
            case (hx: Long, hy: String) =>
              1
            case (hx, hy) =>
              hx.toString.compareTo(hy.toString)

    cmp(split(path), split(other.path))

  end compare

end VirtualFile

case class LocalFile(path: String) extends VirtualFile:

  override def name: String         = SourceIO.fileName(path)
  override def exists: Boolean      = SourceIO.existsFile(path)
  override def isDirectory: Boolean = exists && SourceIO.isDirectory(path)

  override def contentString: String = SourceIO.readAsString(path)

  override def lastUpdatedAt: Long = SourceIO.lastUpdatedAt(path)
  override def listFiles: List[VirtualFile] =
    if isDirectory then
      SourceIO.listFiles(path).map(p => LocalFile(p.toString))
    else
      Nil

/**
  * TODO: Download GitHub archive to .cache/wvlet/repository/github/${owner}/${repo}/${ref} and
  * provide file paths
  * @param owner
  * @param repo
  * @param ref
  */
case class GitHubArchive(owner: String, repo: String, ref: String) extends VirtualFile:
  def name: String                   = s"github:${owner}/${repo}@${ref}"
  override def path: String          = s"https://github.com/${owner}/${repo}"
  override def exists: Boolean       = ???
  override def isDirectory: Boolean  = true
  override def listFiles             = ???
  override def lastUpdatedAt: Long   = ???
  override def contentString: String = ???

case class MemoryFile(path: String, contentString: String) extends VirtualFile:
  val lastUpdatedAt                 = System.currentTimeMillis()
  override def name: String         = path.split("/").last
  override def exists: Boolean      = true
  override def isDirectory: Boolean = false
  override def listFiles            = List.empty

case object EmptyFile extends VirtualFile:
  override def name: String                 = "N/A"
  override def path: String                 = ""
  override def exists: Boolean              = false
  override def isDirectory: Boolean         = false
  override def listFiles: List[VirtualFile] = Nil
  override def lastUpdatedAt: Long          = 0
  override def contentString: String        = ""

/**
  * A file in a resource folder or a jar file
  * @param path
  */
case class FileInResource(path: String) extends VirtualFile:
  val lastUpdatedAt: Long                   = System.currentTimeMillis()
  override def name: String                 = path.split("/").last
  override def exists: Boolean              = true
  override def isDirectory: Boolean         = false
  override def listFiles: List[VirtualFile] = Nil
  override def contentString: String        = SourceIO.readAsString(path)

case class URIResource(url: java.net.URI) extends VirtualFile:
  val lastUpdatedAt: Long                   = System.currentTimeMillis()
  override def name: String                 = url.getPath.split("/").last
  override def path: String                 = url.getPath
  override def exists: Boolean              = true
  override def isDirectory: Boolean         = false
  override def listFiles: List[VirtualFile] = Nil
  override def contentString: String        = SourceIO.readAsString(url)
