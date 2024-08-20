package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.catalog.Catalog
import com.treasuredata.flow.lang.compiler.Compiler.presetLibraryPaths
import com.treasuredata.flow.lang.compiler.analyzer.{
  RemoveUnusedQueries,
  SymbolLabeler,
  TypeResolver
}
import com.treasuredata.flow.lang.compiler.parser.{FlowParser, ParserPhase}
import com.treasuredata.flow.lang.compiler.transform.Incrementalize
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import wvlet.log.LogSupport

object Compiler:

  def default(sourcePath: String): Compiler =
    new Compiler(CompilerOptions(sourceFolders = List(sourcePath), workingFolder = sourcePath))

  /**
    * Phases for text-based analysis of the source code
    */
  def analysisPhases: List[Phase] = List(
    ParserPhase, // Parse *.flow files and create untyped plans
    SymbolLabeler, // Assign unique Symbol to each LogicalPlan and Expression nodes, a and assign a lazy DataType
    RemoveUnusedQueries(), // Exclude unused compilation units (e.g., out of scope queries) from the following phases
    TypeResolver // Assign a concrete DataType to each LogicalPlan and Expression nodes
  )

  /**
    * Phases for transforming the logical plan trees
    */
  def transformPhases: List[Phase] = List(
    Incrementalize // Create an incrementalized plan for a subscription
  )

  /**
    * Generate SQL, Scala, or other code from the logical plan
    * @return
    */
  def codeGenPhases: List[Phase] = List()

  def allPhases: List[List[Phase]] = List(analysisPhases, transformPhases, codeGenPhases)

  def presetLibraryPaths: List[String] = List("flow-lang/src/main/resources/flow-stdlib")

end Compiler

case class CompilerOptions(
    phases: List[List[Phase]] = Compiler.allPhases,
    sourceFolders: List[String] = List("."),
    workingFolder: String = ".",
    // Context database catalog
    catalog: Option[String] = None,
    // context database schema
    schema: Option[String] = None
)

class Compiler(compilerOptions: CompilerOptions) extends LogSupport:

  private lazy val globalContext         = newGlobalContext
  private lazy val localCompilationUnits = listCompilationUnits(compilerOptions.sourceFolders)

  def setDefaultCatalog(catalog: Catalog): Unit = globalContext.defaultCatalog = catalog
  def setDefaultSchema(schema: String): Unit    = globalContext.defaultSchema = schema

  private def newGlobalContext: GlobalContext =
    val global      = GlobalContext(compilerOptions)
    val rootContext = global.getContextOf(unit = CompilationUnit.empty, scope = Scope.newScope(0))
    // Need to initialize the global context before running the analysis phases
    global.init(using rootContext)
    global

  private def listCompilationUnits(sourceFolders: List[String]): List[CompilationUnit] =
    val sourcePaths = Compiler.presetLibraryPaths ++ sourceFolders
    val units = sourcePaths.flatMap { path =>
      CompilationUnit.fromPath(path, isPreset = presetLibraryPaths.contains(path))
    }
    units

  /**
    * @param sourceFolder
    *   A folder containing src and data folders
    * @return
    */
  def compile(): CompileResult = compileSingle(None)

  def compileSingle(contextFile: Option[String]): CompileResult =
    val contextUnit: Option[CompilationUnit] = contextFile
      .flatMap(f => localCompilationUnits.find(_.sourceFile.fileName == f))

    compileInternal(localCompilationUnits, contextUnit = contextUnit)

  def compileSingle(contextUnit: CompilationUnit): CompileResult =
    val units: List[CompilationUnit] = localCompilationUnits :+ contextUnit
    compileInternal(units, contextUnit = Some(contextUnit))

  def compileInternal(
      units: List[CompilationUnit],
      contextUnit: Option[CompilationUnit]
  ): CompileResult =
    globalContext.setContextUnit(contextUnit)
    val rootContext  = globalContext.getRootContext
    var refinedUnits = units
    for
      phaseGroup <- compilerOptions.phases
      phase      <- phaseGroup
    do
      debug(s"Running phase ${phase.name}")
      refinedUnits = phase.runOn(refinedUnits, rootContext)

    CompileResult(refinedUnits, this, rootContext)

end Compiler

case class CompileResult(units: List[CompilationUnit], compiler: Compiler, context: Context):
  def typedPlans: List[LogicalPlan] = units.map(_.resolvedPlan).filter(_.nonEmpty)

  /**
    * Extract compilation results for a specific file name
    * @param fileName
    * @return
    */
  def inFile(fileName: String): Option[CompilationUnit] =
    units.filter(_.sourceFile.fileName == fileName).headOption
