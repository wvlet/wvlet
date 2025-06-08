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
package wvlet.lang.cli

import wvlet.airframe.launcher.{argument, command, option}
import wvlet.lang.catalog.{Catalog, CatalogSerializer, StaticCatalogProvider}
import wvlet.lang.compiler.{DBType, SourceIO, WorkEnv}
import wvlet.lang.runner.connector.{DBConnector, DBConnectorProvider}
import wvlet.log.{LogLevel, LogSupport}
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*

case class CatalogCommandOption(
    @option(prefix = "-p,--path", description = "Path to store static catalog files")
    catalogPath: String = "./catalog",
    @option(prefix = "-t,--type", description = "Database type (duckdb, trino)")
    dbType: String = "duckdb",
    @option(prefix = "-n,--name", description = "Catalog name to import")
    catalog: Option[String] = None,
    @option(prefix = "-s,--schema", description = "Schema name to import (default: all schemas)")
    schema: Option[String] = None,
    @option(prefix = "--profile", description = "Profile to use for connection")
    profile: Option[String] = None
)

/**
  * Catalog management commands for static catalog operations
  */
class CatalogCommand extends LogSupport:

  private def validateAndGetDBType(dbTypeStr: String): DBType =
    val dbType = DBType.fromString(dbTypeStr)
    // Check if it's a known database type (fromString returns Generic for unknown types)
    if !DBType.values.map(_.toString.toLowerCase).contains(dbTypeStr.toLowerCase) then
      error(s"Unknown database type: ${dbTypeStr}")
      throw new IllegalArgumentException(s"Unknown database type: ${dbTypeStr}")
    dbType

  private def sanitizeCatalogName(name: String): String =
    // Remove any path traversal attempts and dangerous characters
    name.replaceAll("[./\\\\:]", "_").trim

  @command(description = "Import catalog metadata from a database")
  def `import`(opts: CatalogCommandOption): Unit =
    // Note: Global log level is already set via WvletGlobalOption
    // The WorkEnv log level here is only for WorkEnv-specific logging
    val workEnv = WorkEnv(".", LogLevel.INFO)
    val dbType  = validateAndGetDBType(opts.dbType)

    info(s"Importing catalog metadata from ${dbType}")

    // Get the appropriate connector
    val connectorProvider = DBConnectorProvider(workEnv)
    val connector         = connectorProvider.getConnector(dbType, opts.profile)

    try
      // Get catalog name and schema
      val catalogName = sanitizeCatalogName(opts.catalog.getOrElse("default"))
      val schemaName  = opts.schema.getOrElse("main")

      // Get the catalog
      val catalog = connector.getCatalog(catalogName, schemaName)
      info(s"Importing catalog: ${catalogName}")

      // Create output directory
      val basePath   = Paths.get(opts.catalogPath)
      val catalogDir = basePath.resolve(dbType.toString.toLowerCase).resolve(catalogName)
      Files.createDirectories(catalogDir)

      // Import schemas
      val schemas =
        opts.schema match
          case Some(specificSchema) =>
            List(catalog.getSchema(specificSchema))
          case None =>
            catalog.listSchemas

      info(s"Found ${schemas.size} schemas to import")

      // Write schemas.json
      val schemasJson = CatalogSerializer.serializeSchemas(schemas.toList)
      Files.writeString(catalogDir.resolve("schemas.json"), schemasJson)

      // Import tables for each schema
      schemas.foreach { schema =>
        info(s"Importing tables from schema: ${schema.name}")
        val tables = catalog.listTables(schema.name)
        info(s"  Found ${tables.size} tables")

        if tables.nonEmpty then
          val tablesJson = CatalogSerializer.serializeTables(tables.toList)
          Files.writeString(catalogDir.resolve(s"${schema.name}.json"), tablesJson)
      }

      // Import functions
      info("Importing SQL functions")
      val functions = catalog.listFunctions
      info(s"Found ${functions.size} functions")

      val functionsJson = CatalogSerializer.serializeFunctions(functions.toList)
      Files.writeString(catalogDir.resolve("functions.json"), functionsJson)

      info(s"Successfully imported catalog metadata to: ${catalogDir}")

    finally
      connector.close()

    end try

  end `import`

  @command(description = "List available static catalogs")
  def list(
      @option(prefix = "-p,--path", description = "Path to static catalog files")
      catalogPath: String = "./catalog"
  ): Unit =
    val basePath = Paths.get(catalogPath)

    if !Files.exists(basePath) then
      info("No static catalogs found")
      return

    val catalogs = StaticCatalogProvider.listAvailableCatalogs(basePath)

    if catalogs.isEmpty then
      info("No static catalogs found")
    else
      info(s"Available static catalogs:")
      catalogs.foreach { case (name, dbType) =>
        info(s"  ${dbType.toString.toLowerCase}/${name}")
      }

  @command(description = "Show details of a static catalog")
  def show(
      @option(prefix = "-p,--path", description = "Path to static catalog files")
      catalogPath: String = "./catalog",
      @argument(description = "Catalog name (format: dbtype/catalog)")
      catalogSpec: String
  ): Unit =
    val parts = catalogSpec.split("/")
    if parts.length != 2 || parts.exists(_.trim.isEmpty) then
      error(s"Invalid catalog specification: ${catalogSpec}. Use format: dbtype/catalog")
      return

    val dbTypeStr   = parts(0).trim
    val catalogName = parts(1).trim

    val dbType =
      try
        validateAndGetDBType(dbTypeStr)
      catch
        case _: IllegalArgumentException =>
          return

    val basePath = Paths.get(catalogPath)
    StaticCatalogProvider.loadCatalog(catalogName, dbType, basePath) match
      case Some(catalog) =>
        info(s"Catalog: ${catalog.catalogName} (${catalog.dbType})")
        info(s"Schemas: ${catalog.listSchemas.size}")
        catalog
          .listSchemas
          .foreach { schema =>
            val tableCount = catalog.listTables(schema.name).size
            info(s"  ${schema.name}: ${tableCount} tables")
          }
        info(s"Functions: ${catalog.listFunctions.size}")
      case None =>
        error(s"Catalog not found: ${catalogSpec}")

  end show

  @command(description = "Refresh catalog metadata from a database")
  def refresh(opts: CatalogCommandOption): Unit =
    info("Refreshing catalog metadata...")
    // Refresh is essentially the same as import
    `import`(opts)

end CatalogCommand
