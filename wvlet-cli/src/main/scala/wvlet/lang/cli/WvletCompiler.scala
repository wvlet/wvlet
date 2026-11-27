package wvlet.lang.cli

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
import wvlet.lang.runner.QueryExecutorConfig
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.activity.ItemStores
import wvlet.lang.runner.connector.ConnectorCatalogs
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.lang.runner.engine.WvletEngineSession
import wvlet.uni.log.LogSupport

/**
  * Compiles and runs a query with the JVM connectors (`ConnectorProvider`), which also read table
  * schemas of the profile's engines. The npm and Native `wvlet` use `WvletCliCompiler` instead.
  *
  * @param profile
  *   the effective profile, usually `compilerOption.resolveProfile(default)`
  */
class WvletCompiler(
    opts: WvletCliGlobalOption,
    compilerOption: WvletCliCompileOption,
    workEnv: WorkEnv,
    dbConnectorProvider: ConnectorProvider,
    profile: Profile
) extends LogSupport
    with AutoCloseable:

  private def currentProfile: Profile = profile

  // Closing the provider closes every cached connector, including the default engine, the
  // in-process engine for local file reads, and the scratch DuckDB connector for exports
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
      dbType = dbType,
      failOnTypeErrors = compilerOption.strict
    )

    val compiler =
      if parseOnly then
        Compiler.parseOnly(options)
      else
        Compiler(options)
    // Set catalog from connector. The in-process wvlet engine needs no catalog setting: its
    // registered tables form the `wvlet` catalog. Without the engine's connector module (e.g.
    // DuckDB in the lite CLI), compile still generates the engine's SQL, with no catalog
    val defaultEngine = currentProfile.defaultEngine
    WvletEngineSession
      .defaultCatalogName(defaultEngine)
      .filter { _ =>
        val available = dbConnectorProvider.isAvailable(defaultEngine.`type`)
        if !available then
          debug(
            s"The ${defaultEngine.`type`} connector is not installed. Compiling without its catalog"
          )
        available
      }
      .foreach { catalog =>
        val schema = defaultEngine.schema.getOrElse("main")
        compiler.setDefaultCatalog(
          ConnectorCatalogs.catalogOf(
            dbConnectorProvider.getConnector(defaultEngine),
            defaultEngine,
            catalog,
            schema
          )
        )
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

  /** Run the query and print the result as a box table */
  def run(): Unit = println(execute().toPrettyBox())

  /** Run the query and return its result */
  def execute(): QueryResult =
    val compiler = createCompiler()
    val workEnv  = compiler.compilerOptions.workEnv
    Control.withResource(ItemStores.forWorkEnv(workEnv)) { itemStore =>
      Control.withResource(
        QueryExecutor(
          dbConnectorProvider,
          currentProfile,
          workEnv,
          QueryExecutorConfig().withItemStore(itemStore)
        )
      ) { executor =>
        val inputUnit     = getInputUnit(forSQL = false)
        val compileResult = compile(inputUnit)
        // Evaluate `test` and `debug` statements, as the npm and Native `wvlet run` do
        val runContext = compileResult.context.withDebugRun(true)
        given Context  = runContext

        executor.executeSelectedStatement(
          inputUnit,
          QuerySelection.All,
          linePosition = inputUnit.resolvedPlan.sourceLocation.position,
          runContext
        )
      }
    }

end WvletCompiler
