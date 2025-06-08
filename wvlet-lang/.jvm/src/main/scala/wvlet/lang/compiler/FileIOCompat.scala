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

import wvlet.lang.catalog.{Catalog, InMemoryCatalog, StaticCatalogProvider}
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*

/**
  * JVM implementation of FileIOCompat that can handle file paths
  */
trait FileIOCompatImpl extends FileIOCompat:
  override def loadStaticCatalog(compilerOptions: CompilerOptions): Catalog =
    if compilerOptions.useStaticCatalog && compilerOptions.staticCatalogPath.isDefined then
      val catalogPath = Paths.get(compilerOptions.staticCatalogPath.get)
      val catalogName = compilerOptions.catalog.getOrElse("default")
      val dbType      = compilerOptions.dbType

      StaticCatalogProvider.loadCatalog(catalogName, dbType, catalogPath) match
        case Some(staticCatalog) =>
          staticCatalog
        case None =>
          // Fall back to in-memory catalog if static catalog loading fails
          InMemoryCatalog(catalogName = catalogName, functions = Nil)
    else
      InMemoryCatalog(catalogName = compilerOptions.catalog.getOrElse("memory"), functions = Nil)

  override def isDirectory(path: Any): Boolean =
    path match
      case p: Path =>
        Files.exists(p) && Files.isDirectory(p)
      case _ =>
        false

  override def listDirectories(path: Any): List[String] =
    path match
      case p: Path =>
        if Files.exists(p) && Files.isDirectory(p) then
          Files
            .list(p)
            .iterator()
            .asScala
            .filter(Files.isDirectory(_))
            .map(_.getFileName.toString)
            .toList
        else
          List.empty
      case _ =>
        List.empty

  override def resolvePath(basePath: Any, segments: String*): Any =
    basePath match
      case p: Path =>
        segments.foldLeft(p)((path, segment) => path.resolve(segment))
      case _ =>
        basePath

  override def readFileIfExists(path: Any): Option[String] =
    path match
      case p: Path =>
        if Files.exists(p) then
          Some(Files.readString(p))
        else
          None
      case _ =>
        None

end FileIOCompatImpl
