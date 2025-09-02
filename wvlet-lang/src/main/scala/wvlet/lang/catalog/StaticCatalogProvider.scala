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

import wvlet.lang.compiler.{DBType, SourceIO}
import wvlet.log.LogSupport

/**
  * Provider for creating static catalogs from persisted metadata
  */
object StaticCatalogProvider extends LogSupport:

  def loadCatalog(catalogName: String, dbType: DBType, basePath: Any): Option[StaticCatalog] =
    // Validate catalog name to prevent directory traversal
    if catalogName.contains("..") || catalogName.contains("/") || catalogName.contains("\\") then
      warn(s"Invalid catalog name: ${catalogName}")
      return None

    val catalogPath = SourceIO.resolvePath(basePath, dbType.toString.toLowerCase, catalogName)

    if SourceIO.isDirectory(catalogPath) then
      try
        debug(s"Loading static catalog from: ${catalogPath}")

        // Load schemas
        val schemasPath = SourceIO.resolvePath(catalogPath, "schemas.json")
        val schemasOpt =
          SourceIO.readFileIfExists(schemasPath) match
            case Some(json) =>
              try
                Some(CatalogSerializer.deserializeSchemas(json))
              catch
                case e: Exception =>
                  error(s"Failed to load schemas from ${schemasPath}: ${e.getMessage}", e)
                  None
            case None =>
              Some(List.empty)

        // If schemas failed to load due to corruption, return None
        val schemas = schemasOpt match
          case None => return None
          case Some(schemaList) => schemaList

        // Load tables for each schema
        val tables =
          schemas
            .map { schema =>
              val schemaPath = SourceIO.resolvePath(catalogPath, s"${schema.name}.json")
              val tableDefs =
                SourceIO.readFileIfExists(schemaPath) match
                  case Some(json) =>
                    try
                      CatalogSerializer.deserializeTables(json)
                    catch
                      case e: Exception =>
                        error(s"Failed to load tables from ${schemaPath}: ${e.getMessage}", e)
                        List.empty[Catalog.TableDef]
                  case None =>
                    List.empty[Catalog.TableDef]
              schema.name -> tableDefs
            }
            .toMap

        // Load functions
        val functionsPath = SourceIO.resolvePath(catalogPath, "functions.json")
        val functions =
          SourceIO.readFileIfExists(functionsPath) match
            case Some(json) =>
              try
                CatalogSerializer.deserializeFunctions(json)
              catch
                case e: Exception =>
                  error(s"Failed to load functions from ${functionsPath}: ${e.getMessage}", e)
                  List.empty
            case None =>
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
            s"Failed to load static catalog '${catalogName}' from ${catalogPath}: ${e.getMessage}",
            e
          )
          None
    else
      debug(s"Static catalog path does not exist: ${catalogPath}")
      None

    end if

  end loadCatalog

  def listAvailableCatalogs(basePath: Any): List[(String, DBType)] =
    if SourceIO.isDirectory(basePath) then
      val dbTypeDirs = SourceIO.listDirectories(basePath)

      dbTypeDirs.flatMap { dbTypeDir =>
        val dbTypeName = dbTypeDir.toUpperCase
        DBType.values.find(_.toString.equalsIgnoreCase(dbTypeName)) match
          case Some(dbType) =>
            val dbTypePath  = SourceIO.resolvePath(basePath, dbTypeDir)
            val catalogDirs = SourceIO.listDirectories(dbTypePath)
            catalogDirs.map { catalogName =>
              (catalogName, dbType)
            }
          case None =>
            warn(s"Unknown database type: ${dbTypeName}")
            List.empty
      }
    else
      List.empty

end StaticCatalogProvider
