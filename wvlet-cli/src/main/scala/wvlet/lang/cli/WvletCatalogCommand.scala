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
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.catalog.SchemaDriftDetector
import wvlet.lang.catalog.StaticCatalogExporter
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.lang.runner.SchemaDriftChecker
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
    path: String = "catalog",
    @option(prefix = "--no-functions", description = "Skip importing engine functions")
    noFunctions: Boolean = false
)

case class WvletCatalogDiffOption(
    @option(prefix = "-w", description = "Working folder")
    workFolder: String = ".",
    @option(prefix = "--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(prefix = "--catalog", description = "Catalog to check (default: profile catalog)")
    catalog: Option[String] = None,
    @option(
      prefix = "--schema",
      description = "Default schema of unbound table declarations (default: profile schema)"
    )
    schema: Option[String] = None
)

/**
  * `wvlet catalog` subcommands for importing database table schemas as Wvlet type definitions
  * (#1881), enabling offline query validation
  */
class WvletCatalogCommand(opts: WvletGlobalOption) extends LogSupport:

  @command(description = "Show the usage of catalog commands", isDefault = true)
  def help: Unit = info(
    "Usage: wvlet catalog (import|diff) [--profile p] [--catalog c] [--schema s]"
  )

  private def handleError[U](body: => U): U =
    try
      body
    catch
      case e: WvletLangException =>
        error(e.getMessage)
        if !WvletMain.isInSbt then
          System.exit(1)
        throw e

  private def resolveCatalogName(engine: ConnectorConfig, catalogOpt: Option[String]): String =
    catalogOpt
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

  @command(description = "Import database table schemas as Wvlet type definitions")
  def `import`(catalogOpts: WvletCatalogOption): Unit =
    val workEnv = WorkEnv(catalogOpts.workFolder)
    val profile = Profile.getProfile(catalogOpts.profile, catalogOpts.catalog, catalogOpts.schema)
    val engine  = profile.defaultEngine
    val catalogName = resolveCatalogName(engine, catalogOpts.catalog)

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
      val functionsPath =
        if catalogOpts.noFunctions then
          None
        else if schemaNames.exists(_.equalsIgnoreCase("functions")) then
          // The functions file would collide with the schema file of a schema literally
          // named functions; the table schemas win
          warn(
            s"Skipping the functions import: catalog ${catalogName} has a schema named 'functions'"
          )
          None
        else
          // The Generic engine runs on an in-process DuckDB, so its functions are DuckDB's
          val contextDBType =
            if engine.dbType == DBType.Generic then
              DBType.DuckDB
            else
              engine.dbType
          val contextName = contextDBType.toString.toLowerCase
          StaticCatalogExporter.exportFunctions(
            catalogName,
            contextName,
            connector.listFunctions(catalogName),
            basePath
          )
      val written = StaticCatalogExporter.exportSchemas(
        catalogName,
        schemaNames,
        schemaName => connector.listTableDefs(catalogName, schemaName),
        basePath,
        // A full-catalog import removes generated files of schemas dropped from the database.
        // With --no-functions, this also removes a previously generated functions.wv
        pruneStale = catalogOpts.schema.isEmpty,
        keepPaths = functionsPath.toList
      )
      if written.isEmpty then
        // Engine functions are listed independently of the catalog name, so an empty table
        // import warns even when functions were generated
        warn(
          s"No tables found in catalog ${catalogName}. Check the --profile, --catalog, and --schema options"
        )
      functionsPath.foreach(path => info(s"Generated ${path}"))
      written.foreach(path => info(s"Generated ${path}"))
      if written.nonEmpty || functionsPath.nonEmpty then
        info(s"Imported ${written.size} schema(s) from catalog ${catalogName}")
    }
  end `import`

  @command(description =
    "Check table declarations against the connected catalog and print reshape migrations on drift"
  )
  def diff(diffOpts: WvletCatalogDiffOption): Unit = handleError {
    val workEnv = WorkEnv(diffOpts.workFolder)
    // Default to the DuckDB profile like `wvlet run`: the generic profile carries no catalog,
    // which would leave nothing to diff against
    val profile = Profile.getProfile(
      diffOpts.profile,
      diffOpts.catalog,
      diffOpts.schema,
      default = Profile.defaultDuckDBProfile
    )
    val engine      = profile.defaultEngine
    val catalogName = resolveCatalogName(engine, diffOpts.catalog)
    val schemaName  = diffOpts.schema.orElse(engine.schema).getOrElse("main")
    // The Generic engine runs on an in-process DuckDB, so types normalize with DuckDB rules
    val dbType =
      if engine.dbType == DBType.Generic then
        DBType.DuckDB
      else
        engine.dbType

    Control.withResource(ConnectorProvider(workEnv)) { connectorProvider =>
      val connector = connectorProvider.getConnector(profile)
      val report    = SchemaDriftChecker.check(
        sourceFolders = List(diffOpts.workFolder),
        workEnv = workEnv,
        connector = connector,
        defaultCatalog = catalogName,
        defaultSchema = schemaName,
        dbType = dbType
      )
      report
        .missingTables
        .foreach { table =>
          info(
            s"Table ${table} is not in the catalog yet; declared tables materialize on the first write"
          )
        }
      report
        .drifted
        .foreach { drift =>
          println(SchemaDriftDetector.render(drift))
        }
      if report.hasDrift then
        // A non-zero exit code so the check can gate CI
        throw StatusCode
          .SCHEMA_DRIFT_DETECTED
          .newException(s"Schema drift detected in ${report.drifted.size} table(s)")
      info(s"No schema drift detected (checked ${report.checkedTables} table(s))")
    }
  }

end WvletCatalogCommand
