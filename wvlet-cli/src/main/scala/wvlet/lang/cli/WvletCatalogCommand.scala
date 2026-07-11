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

import wvlet.uni.cli.launcher.command
import wvlet.uni.cli.launcher.option
import wvlet.uni.control.Control
import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Profile
import wvlet.lang.catalog.StaticCatalogExporter
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.log.LogSupport

import java.nio.file.Path

case class WvletCatalogOption(
    @option(prefix = "-w", description = "Working folder")
    workFolder: String = ".",
    @option(prefix = "--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(prefix = "--catalog", description = "Catalog to import (default: profile catalog)")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Import only the specified schema")
    schema: Option[String] = None,
    @option(
      prefix = "--path",
      description = "Folder to write the generated .wv files (default: catalog)"
    )
    path: String = "catalog"
)

/**
  * `wvlet catalog` subcommands for importing database table schemas as Wvlet type definitions
  * (#1881), enabling offline query validation
  */
class WvletCatalogCommand(opts: WvletGlobalOption) extends LogSupport:

  @command(description = "Show the usage of catalog commands", isDefault = true)
  def help: Unit = info("Usage: wvlet catalog import [--profile p] [--catalog c] [--schema s]")

  @command(description = "Import database table schemas as Wvlet type definitions")
  def `import`(catalogOpts: WvletCatalogOption): Unit =
    val workEnv = WorkEnv(catalogOpts.workFolder)
    val profile = Profile.getProfile(catalogOpts.profile, catalogOpts.catalog, catalogOpts.schema)
    val engine  = profile.defaultEngine
    val catalogName = catalogOpts
      .catalog
      .orElse(engine.catalog)
      .getOrElse {
        if engine.dbType == DBType.DuckDB || engine.dbType == DBType.Generic then
          // The in-process DuckDB catalog is always named memory
          "memory"
        else
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(
              s"Specify --catalog or add a catalog to the '${engine.name}' connector in the profile"
            )
      }

    Control.withResource(ConnectorProvider(workEnv)) { connectorProvider =>
      val connector = connectorProvider.getConnector(profile)
      // Scan the database directly (not through the ConnectorCatalog metadata cache), so a
      // catalog import always reflects the current table schemas
      val schemaNames =
        catalogOpts.schema match
          case Some(s) =>
            List(s)
          case None =>
            connector.listSchemaNames(catalogName).filterNot(StaticCatalogExporter.isSystemSchema)
      val basePath =
        if Path.of(catalogOpts.path).isAbsolute then
          catalogOpts.path
        else
          s"${catalogOpts.workFolder}/${catalogOpts.path}"
      val written = StaticCatalogExporter.exportSchemas(
        catalogName,
        schemaNames,
        schemaName => connector.listTableDefs(catalogName, schemaName),
        basePath,
        // A full-catalog import removes generated files of schemas dropped from the database
        pruneStale = catalogOpts.schema.isEmpty
      )
      if written.isEmpty then
        warn(
          s"No tables found in catalog ${catalogName}. Check the --profile, --catalog, and --schema options"
        )
      else
        written.foreach(path => info(s"Generated ${path}"))
        info(s"Imported ${written.size} schema(s) from catalog ${catalogName}")
    }
  end `import`

end WvletCatalogCommand
