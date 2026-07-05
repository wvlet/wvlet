package wvlet.lang.cli

import wvlet.uni.cli.launcher.argument
import wvlet.uni.cli.launcher.option
import wvlet.uni.control.Control
import wvlet.lang.api.StatusCode
import wvlet.lang.api.v1.query.QuerySelection
import wvlet.lang.catalog.LazyCatalog
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.codegen.CodeFormatterConfig
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.codegen.WvletGenerator
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.CompileResult
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.Phase
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.QueryExecutor
import wvlet.lang.connector.DBConnector
import wvlet.lang.runner.connector.ConnectorCatalogs
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.log.LogSupport

case class WvletCompilerOption(
    @option(prefix = "-w", description = "Working folder")
    workFolder: String = ".",
    @option(prefix = "-f,--file", description = "Read a query from the given .wv file")
    file: Option[String] = None,
    @argument(description = "query")
    query: Option[String] = None,
    @option(prefix = "-t,--target", description = "Target database type")
    targetDBType: Option[String] = None,
    @option(prefix = "--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(prefix = "--catalog", description = "Context database catalog to use")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Context database schema to use")
    schema: Option[String] = None
)

class WvletCompiler(
    opts: WvletGlobalOption,
    compilerOption: WvletCompilerOption,
    workEnv: WorkEnv,
    dbConnectorProvider: ConnectorProvider
) extends LogSupport
    with AutoCloseable:

  private lazy val currentProfile: Profile =
    // Resolve the profile from DBType or profile name
    compilerOption.targetDBType match
      case Some(dbType) =>
        if compilerOption.profile.isDefined then
          throw StatusCode
            .INVALID_ARGUMENT
            .newException("Specify either -t (--target) or --profile")
        val resolvedDBType = DBType.fromString(dbType)
        debug(s"Using syntax for ${resolvedDBType}")
        Profile.defaultProfileFor(resolvedDBType)
      case _ =>
        Profile.getProfile(compilerOption.profile, compilerOption.catalog, compilerOption.schema)

  private var _dbConnector: DBConnector = null

  private def getDBConnector: DBConnector = synchronized {
    if _dbConnector == null then
      _dbConnector = dbConnectorProvider.getConnector(currentProfile)
    _dbConnector
  }

  // Closing the provider closes every cached connector, including _dbConnector and the
  // scratch DuckDB connector QueryExecutor may have created for local file reads/exports
  override def close(): Unit = dbConnectorProvider.close()

  private def createCompiler(parseOnly: Boolean = false): Compiler =
    val dbType = compilerOption
      .targetDBType
      .map(DBType.fromString)
      .getOrElse(currentProfile.defaultEngine.dbType)

    val options = CompilerOptions(
      sourceFolders = List(compilerOption.workFolder),
      workEnv = workEnv,
      catalog = currentProfile.defaultEngine.catalog,
      schema = currentProfile.defaultEngine.schema,
      dbType = dbType
    )

    val compiler =
      if parseOnly then
        Compiler.parseOnly(options)
      else
        Compiler(options)
    // Set catalog from connector
    currentProfile
      .defaultEngine
      .catalog
      .foreach { catalog =>
        val schema = currentProfile.defaultEngine.schema.getOrElse("main")
        compiler.setDefaultCatalog(getDBConnector.getCatalog(catalog, schema))
      }

    currentProfile
      .defaultEngine
      .schema
      .foreach { schema =>
        compiler.setDefaultSchema(schema)
      }

    // Register every profile connector under its name so queries can reference
    // `from <connector>.<table>` and switch engines with `use <connector>` (#1861 Phase 2).
    // Non-default connectors get a LazyCatalog so they only connect on first reference.
    currentProfile
      .connectors
      .foreach { c =>
        val schema                                   = c.schema.getOrElse("main")
        val catalogName                              = c.catalog.getOrElse(c.name)
        def lazyCatalogOf(name: String): LazyCatalog = LazyCatalog(
          name,
          c.dbType,
          () => ConnectorCatalogs.catalogOf(dbConnectorProvider.getConnector(c), c, name, schema)
        )
        compiler.addConnectorCatalog(
          c.name,
          lazyCatalogOf(catalogName),
          schema,
          // Engines may span multiple catalogs (e.g. Trino), addressable with 4-part
          // <connector>.<catalog>.<schema>.<table> names
          catalogProvider =
            if dbConnectorProvider.isEngineType(c.`type`) then
              Some(lazyCatalogOf)
            else
              None
        )
      }

    compiler

  end createCompiler

  private def getInputUnit(forSQL: Boolean = false): CompilationUnit =
    (compilerOption.file, compilerOption.query) match
      case (Some(f), None) =>
        CompilationUnit.fromFile(s"${compilerOption.workFolder}/${f}".stripPrefix("./"))
      case (None, Some(q)) =>
        if forSQL then
          CompilationUnit.fromSqlString(q)
        else
          CompilationUnit.fromWvletString(q)
      case _ =>
        throw StatusCode.INVALID_ARGUMENT.newException("Specify either --file or a query argument")

  private def compile(inputUnit: CompilationUnit): CompileResult = createCompiler()
    .compileSingleUnit(inputUnit)

  private def compileInternal(inputUnit: CompilationUnit, parseOnly: Boolean = false): Context =
    val compileResult =
      if parseOnly then
        val parsingCompiler = createCompiler(parseOnly = true)
        parsingCompiler.compileSingleUnit(inputUnit)
      else
        compile(inputUnit)

    compileResult.reportAllErrors

    compileResult
      .context
      .withCompilationUnit(inputUnit)
      // Disable debug path as we can't run tests in plain SQL
      .withDebugRun(false)
      .newContext(Symbol.NoSymbol)

  def generateSQL: String =
    val inputUnit = getInputUnit(forSQL = false)
    val ctx       = compileInternal(inputUnit)
    GenSQL.generateSQL(inputUnit)(using ctx)

  def generateWvlet: String =
    val inputUnit = getInputUnit(forSQL = true)
    // For SQL to Wvlet conversion, we only need to parse the SQL, not run full compilation
    val ctx = compileInternal(inputUnit, parseOnly = inputUnit.sourceFile.isSQL)

    // Get the resolved logical plan from the compilation unit
    val logicalPlan = inputUnit.resolvedPlan

    // Create a WvletGenerator with the appropriate database type configuration
    val config    = CodeFormatterConfig(sqlDBType = ctx.dbType)
    val generator = WvletGenerator(config)(using ctx)

    // Convert the logical plan to Wvlet flow-style syntax
    generator.print(logicalPlan)

  def run(): Unit =
    val compiler = createCompiler()
    Control.withResource(
      QueryExecutor(dbConnectorProvider, currentProfile, compiler.compilerOptions.workEnv)
    ) { executor =>
      val inputUnit     = getInputUnit(forSQL = false)
      val compileResult = compile(inputUnit)
      given Context     = compileResult.context

      val queryResult = executor.executeSelectedStatement(
        inputUnit,
        QuerySelection.All,
        linePosition = inputUnit.resolvedPlan.sourceLocation.position,
        compileResult.context
      )
      println(queryResult.toPrettyBox())
    }

end WvletCompiler
