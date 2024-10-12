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

/**
  * An abstraction over local files or files in remote GitHub repositories
  */
trait VirtualFile:
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
  def listFiles: Seq[VirtualFile]

  /**
    * Last updated time in milliseconds
    * @return
    */
  def lastUpdatedAt: Long
  def contentString: String
  def content: IArray[Char] = IArray.unsafeFromArray(contentString.toCharArray)

case class LocalFile(path: String) extends VirtualFile:
  private val filePath              = Path.of(path)
  override def name: String         = filePath.getFileName.toString
  override def exists: Boolean      = Files.exists(filePath)
  override def isDirectory: Boolean = exists && Files.exists(filePath)

  override def contentString: String = IOUtil.readAsString(filePath.toFile)

  override def lastUpdatedAt: Long = Files.getLastModifiedTime(filePath).toMillis
  override def listFiles: Seq[VirtualFile] =
    if isDirectory then
      Files.list(filePath).toList.asScala.map(p => LocalFile(p.toString)).toSeq
    else
      Seq.empty

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

case class MemoryFile(name: String, contentString: String) extends VirtualFile:
  val lastUpdatedAt                 = System.currentTimeMillis()
  override def path: String         = name
  override def exists: Boolean      = true
  override def isDirectory: Boolean = false
  override def listFiles            = Seq.empty

case object EmptyFile extends VirtualFile:
  override def name: String                = "N/A"
  override def path: String                = ""
  override def exists: Boolean             = false
  override def isDirectory: Boolean        = false
  override def listFiles: Seq[VirtualFile] = Seq.empty
  override def lastUpdatedAt: Long         = 0
  override def contentString: String       = ""

/**
  * A file in a resource folder or a jar file
  * @param path
  */
case class FileInResource(path: String) extends VirtualFile:
  val lastUpdatedAt: Long                  = System.currentTimeMillis()
  override def name: String                = path.split("/").last
  override def exists: Boolean             = true
  override def isDirectory: Boolean        = false
  override def listFiles: Seq[VirtualFile] = Seq.empty
  override def contentString: String       = IOUtil.readAsString(path)

case class URLResource(url: java.net.URL) extends VirtualFile:
  val lastUpdatedAt: Long                  = System.currentTimeMillis()
  override def name: String                = url.getFile.split("/").last
  override def path: String                = url.getPath
  override def exists: Boolean             = true
  override def isDirectory: Boolean        = false
  override def listFiles: Seq[VirtualFile] = Seq.empty
  override def contentString: String       = IOUtil.readAsString(url)
