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
package wvlet.lang.catalog

import wvlet.lang.compiler.DBType
import wvlet.log.LogSupport
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

/**
  * JVM implementation of StaticCatalogCompat with file I/O support
  */
trait StaticCatalogCompatImpl extends StaticCatalogCompat with LogSupport:

  override def loadCatalog(
      catalogName: String,
      dbType: DBType,
      basePath: Any
  ): Option[StaticCatalog] =
    basePath match
      case path: Path =>
        // Validate catalog name to prevent directory traversal
        if catalogName.contains("..") || catalogName.contains("/") || catalogName.contains("\\")
        then
          warn(s"Invalid catalog name: ${catalogName}")
          return None
        val catalogPath = path.resolve(dbType.toString.toLowerCase).resolve(catalogName)

        if Files.exists(catalogPath) && Files.isDirectory(catalogPath) then
          try
            debug(s"Loading static catalog from: ${catalogPath}")

            // Load schemas
            val schemasFile = catalogPath.resolve("schemas.json")
            val schemas =
              if Files.exists(schemasFile) then
                try
                  val json = Files.readString(schemasFile)
                  CatalogSerializer.deserializeSchemas(json)
                catch
                  case e: Exception =>
                    throw new Exception(
                      s"Failed to load schemas from ${schemasFile}: ${e.getMessage}",
                      e
                    )
              else
                List.empty

            // Load tables for each schema
            val tables =
              schemas
                .map { schema =>
                  val schemaFile = catalogPath.resolve(s"${schema.name}.json")
                  if Files.exists(schemaFile) then
                    try
                      val json      = Files.readString(schemaFile)
                      val tableDefs = CatalogSerializer.deserializeTables(json)
                      schema.name -> tableDefs
                    catch
                      case e: Exception =>
                        throw new Exception(
                          s"Failed to load tables from ${schemaFile}: ${e.getMessage}",
                          e
                        )
                  else
                    schema.name -> List.empty[Catalog.TableDef]
                }
                .toMap

            // Load functions
            val functionsFile = catalogPath.resolve("functions.json")
            val functions =
              if Files.exists(functionsFile) then
                try
                  val json = Files.readString(functionsFile)
                  CatalogSerializer.deserializeFunctions(json)
                catch
                  case e: Exception =>
                    throw new Exception(
                      s"Failed to load functions from ${functionsFile}: ${e.getMessage}",
                      e
                    )
              else
                List.empty

            Some(
              StaticCatalog(
                catalogName = catalogName,
                dbType = dbType,
                schemas = schemas,
                tablesBySchema = tables,
                functions = functions
              )
            )
          catch
            case e: Exception =>
              error(
                s"Failed to load static catalog '${catalogName}' from ${catalogPath}: ${e
                    .getMessage}",
                e
              )
              None
        else
          debug(s"Static catalog path does not exist: ${catalogPath}")
          None
        end if
      case _ =>
        warn("Invalid basePath type for JVM")
        None

  override def listAvailableCatalogs(basePath: Any): List[(String, DBType)] =
    basePath match
      case path: Path =>
        if Files.exists(path) && Files.isDirectory(path) then
          val dbTypeDirs = Files.list(path).iterator().asScala.filter(Files.isDirectory(_)).toList

          dbTypeDirs.flatMap { dbTypeDir =>
            val dbTypeName = dbTypeDir.getFileName.toString.toUpperCase
            DBType.values.find(_.toString.equalsIgnoreCase(dbTypeName)) match
              case Some(dbType) =>
                val catalogDirs =
                  Files.list(dbTypeDir).iterator().asScala.filter(Files.isDirectory(_)).toList
                catalogDirs.map { catalogDir =>
                  (catalogDir.getFileName.toString, dbType)
                }
              case None =>
                warn(s"Unknown database type: ${dbTypeName}")
                List.empty
          }
        else
          List.empty
      case _ =>
        warn("Invalid basePath type for JVM")
        List.empty

end StaticCatalogCompatImpl
