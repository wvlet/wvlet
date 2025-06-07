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

import wvlet.lang.compiler.{DBType, WorkEnv}
import wvlet.log.LogSupport
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*

/**
  * Provider for creating static catalogs from persisted metadata
  */
trait StaticCatalogProvider:
  /**
    * Load a static catalog from the specified path
    * @param catalogName
    *   the name of the catalog
    * @param dbType
    *   the database type (e.g., DuckDB, Trino)
    * @param basePath
    *   the base path where catalog metadata is stored
    * @return
    *   a static catalog instance
    */
  def loadCatalog(catalogName: String, dbType: DBType, basePath: Path): Option[StaticCatalog]

  /**
    * List available static catalogs at the specified base path
    * @param basePath
    *   the base path where catalog metadata is stored
    * @return
    *   list of available catalog names with their database types
    */
  def listAvailableCatalogs(basePath: Path): List[(String, DBType)]

object StaticCatalogProvider extends StaticCatalogProvider with LogSupport:

  override def loadCatalog(
      catalogName: String,
      dbType: DBType,
      basePath: Path
  ): Option[StaticCatalog] =
    val catalogPath = basePath.resolve(dbType.toString.toLowerCase).resolve(catalogName)

    if Files.exists(catalogPath) && Files.isDirectory(catalogPath) then
      try
        debug(s"Loading static catalog from: ${catalogPath}")

        // Load schemas
        val schemasFile = catalogPath.resolve("schemas.json")
        val schemas =
          if Files.exists(schemasFile) then
            val json = Files.readString(schemasFile)
            CatalogSerializer.deserializeSchemas(json)
          else
            List.empty

        // Load tables for each schema
        val tables =
          schemas
            .map { schema =>
              val schemaFile = catalogPath.resolve(s"${schema.name}.json")
              if Files.exists(schemaFile) then
                val json      = Files.readString(schemaFile)
                val tableDefs = CatalogSerializer.deserializeTables(json)
                schema.name -> tableDefs
              else
                schema.name -> List.empty[Catalog.TableDef]
            }
            .toMap

        // Load functions
        val functionsFile = catalogPath.resolve("functions.json")
        val functions =
          if Files.exists(functionsFile) then
            val json = Files.readString(functionsFile)
            CatalogSerializer.deserializeFunctions(json)
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
          error(s"Failed to load static catalog from ${catalogPath}: ${e.getMessage}")
          None
    else
      debug(s"Static catalog path does not exist: ${catalogPath}")
      None

    end if

  end loadCatalog

  override def listAvailableCatalogs(basePath: Path): List[(String, DBType)] =
    if Files.exists(basePath) && Files.isDirectory(basePath) then
      val dbTypeDirs = Files.list(basePath).iterator().asScala.filter(Files.isDirectory(_)).toList

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

end StaticCatalogProvider
