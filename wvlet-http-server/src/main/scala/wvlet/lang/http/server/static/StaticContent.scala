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
package wvlet.lang.http.server.static

import java.io.{File, IOException}
import java.net.URL
import wvlet.uni.control.{Control, IO}
import wvlet.uni.http.{HttpHeader, HttpStatus, Response}
import wvlet.uni.log.LogSupport

import java.nio.file.Paths
import scala.annotation.tailrec
import scala.util.Try

/**
  * Helper for returning static contents
  */
object StaticContent extends LogSupport:

  trait ResourceType:
    def basePath: String
    def find(relativePath: String): Option[URL]

    private val canonicalBasePath: Try[String] = Try(new File(basePath).getCanonicalPath)

    // Helper to check if a potential resource path is truly within the base path
    protected def isPathInsideBase(resourceFile: File): Boolean = canonicalBasePath
      .flatMap { cbPath =>
        Try(resourceFile.getCanonicalPath).map { rcPath =>
          // Check if the resource's canonical path starts with the base's canonical path,
          // ensuring it's truly contained within. Handle the separator correctly.
          rcPath.startsWith(cbPath) && (
            rcPath.length == cbPath.length || // Exactly the base path itself
              rcPath.charAt(cbPath.length) ==
              File.separatorChar || // Starts with base path + separator
              cbPath == "/"
          ) // Special case for root base path
        }
      }
      .recover { case e: IOException =>
        // Error getting canonical paths likely indicates issues (permissions, non-existent?)
        logger.warn(s"Failed to get canonical path for comparison: ${e.getMessage}", e)
        false
      }
      .getOrElse(false) // Default to false if any Try failed

  case class FileResource(basePath: String) extends ResourceType:
    override def find(relativePath: String): Option[URL] =
      val f = new File(s"${basePath}/${relativePath}")
      if f.exists() && f.isFile() && isPathInsideBase(f) then
        Some(f.toURI.toURL)
      else
        None

  case class ClasspathResource(basePath: String) extends ResourceType:
    override def find(relativePath: String): Option[URL] =
      // Match airframe Resource.find: callers may pass leading slashes ("/static"),
      // trailing slashes ("static/"), or an empty basePath. ClassLoader.getResource
      // rejects leading slashes and many loaders mishandle "//", so collapse the
      // joined path into a clean "a/b/c" before lookup.
      val resourcePath = s"${basePath}/${relativePath}".split("/").filter(_.nonEmpty).mkString("/")
      val tccl         = Option(Thread.currentThread().getContextClassLoader)
      val ownLoader    = Option(getClass.getClassLoader)
      tccl
        .flatMap(cl => Option(cl.getResource(resourcePath)))
        .orElse(ownLoader.flatMap(cl => Option(cl.getResource(resourcePath))))

  private def isSafeRelativePath(path: String): Boolean =
    @tailrec
    def loop(pos: Int, path: List[String]): Boolean =
      if pos < 0 then
        false
      else if path.isEmpty then
        true
      else if path.head == ".." then
        loop(pos - 1, path.tail)
      else
        loop(pos + 1, path.tail)

    // Check for null characters
    if path.indexOf(0) >= 0 then
      false
    else if path.startsWith("/") || path.isEmpty || path.contains("//") then
      false
    else if Try(Paths.get(path)).isFailure then
      false
    else
      loop(0, path.split("/").toList.filter(_.nonEmpty))

  private def findContentType(filePath: String): String =
    val leaf = filePath.split("/").lastOption.getOrElse("")
    val ext  =
      val pos = leaf.lastIndexOf(".")
      if pos > 0 then
        leaf.substring(pos + 1)
      else
        ""
    ext match
      case "html" | "htm" =>
        "text/html"
      case "gif" =>
        "image/gif"
      case "png" =>
        "image/png"
      case "jpeg" | "jpg" =>
        "image/jpeg"
      case "css" =>
        "text/css"
      case "gz" =>
        "application/gzip"
      case "txt" =>
        "text/plain"
      case "xml" =>
        "application/xml"
      case "json" =>
        "application/json"
      case "zip" =>
        "application/zip"
      case "js" =>
        "application/javascript"
      case _ =>
        "application/octet-stream"

  end findContentType

  def fromResource(basePath: String, relativePath: String): Response = StaticContent()
    .fromResource(basePath)
    .apply(relativePath)

  def fromResource(basePaths: List[String], relativePath: String): Response =
    val sc =
      basePaths.foldLeft(StaticContent()) { (sc, x) =>
        sc.fromResource(x)
      }
    sc.apply(relativePath)

  def fromDirectory(dirPath: String, relativePath: String): Response = StaticContent()
    .fromDirectory(dirPath)
    .apply(relativePath)

  def fromDirectory(dirPaths: List[String], relativePath: String): Response =
    val sc =
      dirPaths.foldLeft(StaticContent()) { (sc, x) =>
        sc.fromDirectory(x)
      }
    sc.apply(relativePath)

  def fromResource(basePath: String): StaticContent  = StaticContent().fromResource(basePath)
  def fromDirectory(basePath: String): StaticContent = StaticContent().fromDirectory(basePath)

end StaticContent

import wvlet.lang.http.server.static.StaticContent.*

case class StaticContent(resourcePaths: List[StaticContent.ResourceType] = List.empty)
    extends LogSupport:

  def fromDirectory(basePath: String): StaticContent = this.copy(resourcePaths =
    FileResource(basePath) :: resourcePaths
  )

  def fromResource(basePath: String): StaticContent = this.copy(resourcePaths =
    ClasspathResource(basePath) :: resourcePaths
  )

  def find(relativePath: String): Option[URL] =
    @tailrec
    def loop(lst: List[ResourceType]): Option[URL] =
      lst match
        case Nil =>
          None
        case resource :: tail =>
          resource.find(relativePath) match
            case url @ Some(x) =>
              url
            case None =>
              loop(tail)
    loop(resourcePaths)

  def apply(relativePath: String): Response =
    if !isSafeRelativePath(relativePath) then
      Response(HttpStatus.Forbidden_403)
    else
      find(relativePath)
        .map { uri =>
          val mediaType = findContentType(relativePath)
          // Read the resource file as binary
          Control.withResource(uri.openStream()) { in =>
            val content = IO.readFully(in)
            // TODO cache control (e.g., max-age, last-updated)
            Response(HttpStatus.Ok_200)
              .withBytesContent(content)
              .setHeader(HttpHeader.ContentType, mediaType)
          }
        }
        .getOrElse {
          Response(HttpStatus.NotFound_404)
        }

end StaticContent
