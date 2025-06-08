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

import wvlet.lang.compiler.{DBType, Compat}
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

    val catalogPath = Compat.resolvePath(basePath, dbType.toString.toLowerCase, catalogName)

    if Compat.isDirectory(catalogPath) then
      try
        debug(s"Loading static catalog from: ${catalogPath}")

        // Load schemas
        val schemasPath = Compat.resolvePath(catalogPath, "schemas.json")
        val schemas =
          Compat.readFileIfExists(schemasPath) match
            case Some(json) =>
              try
                CatalogSerializer.deserializeSchemas(json)
              catch
                case e: Exception =>
                  throw new Exception(
                    s"Failed to load schemas from ${schemasPath}: ${e.getMessage}",
                    e
                  )
            case None =>
              List.empty

        // Load tables for each schema
        val tables =
          schemas
            .map { schema =>
              val schemaPath = Compat.resolvePath(catalogPath, s"${schema.name}.json")
              val tableDefs =
                Compat.readFileIfExists(schemaPath) match
                  case Some(json) =>
                    try
                      CatalogSerializer.deserializeTables(json)
                    catch
                      case e: Exception =>
                        throw new Exception(
                          s"Failed to load tables from ${schemaPath}: ${e.getMessage}",
                          e
                        )
                  case None =>
                    List.empty[Catalog.TableDef]
              schema.name -> tableDefs
            }
            .toMap

        // Load functions
        val functionsPath = Compat.resolvePath(catalogPath, "functions.json")
        val functions =
          Compat.readFileIfExists(functionsPath) match
            case Some(json) =>
              try
                CatalogSerializer.deserializeFunctions(json)
              catch
                case e: Exception =>
                  throw new Exception(
                    s"Failed to load functions from ${functionsPath}: ${e.getMessage}",
                    e
                  )
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
    if Compat.isDirectory(basePath) then
      val dbTypeDirs = Compat.listDirectories(basePath)

      dbTypeDirs.flatMap { dbTypeDir =>
        val dbTypeName = dbTypeDir.toUpperCase
        DBType.values.find(_.toString.equalsIgnoreCase(dbTypeName)) match
          case Some(dbType) =>
            val dbTypePath  = Compat.resolvePath(basePath, dbTypeDir)
            val catalogDirs = Compat.listDirectories(dbTypePath)
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
