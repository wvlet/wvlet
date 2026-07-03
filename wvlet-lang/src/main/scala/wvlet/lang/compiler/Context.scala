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
package wvlet.lang.compiler

import wvlet.lang.api.SourceLocation
import wvlet.lang.api.Span
import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Catalog
import wvlet.lang.catalog.InMemoryCatalog
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.compiler.typer.TyperError
import wvlet.lang.compiler.typer.TyperState
import wvlet.lang.model.RelationType
import wvlet.lang.model.Type
import wvlet.lang.model.expr.NameExpr
import wvlet.lang.model.plan.Import
import wvlet.uni.log.LogSupport

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

/**
  * GlobalContext is a shared context for all compilation units in the same compilation run.
  *
  * This context maintains the mapping from the source file to the corresponding unique Context.
  *
  * @param sourceFolders
  * @param workingFolder
  */
case class GlobalContext(compilerOptions: CompilerOptions):
  // Used for specifying a context file to lookup queries, types, and models
  private var contextUnit: Option[CompilationUnit] = None
  private var symbolCount                          = 0
  private var rootContext: Context                 = null

  // Loaded data files, etc.
  private val files = new ConcurrentHashMap[NameExpr, VirtualFile]().asScala

  // Loaded contexts for source .wv files
  private val contextTable = new ConcurrentHashMap[SourceFile, Context]().asScala

  // Names already checked for duplicate top-level definitions across compilation units,
  // and the confirmed duplicates (name -> defining file names)
  private val duplicateCheckedNames    = new ConcurrentHashMap[Name, Boolean]().asScala
  private val duplicateDefinitionQueue =
    new java.util.concurrent.ConcurrentLinkedQueue[(Name, List[String])]()

  def duplicateDefinitions: List[(Name, List[String])] = duplicateDefinitionQueue.asScala.toList

  /**
    * Record and return whether the given name still needs a duplicate-definition check. The check
    * runs at most once per name to keep symbol lookup fast
    */
  def needsDuplicateCheck(name: Name): Boolean =
    duplicateCheckedNames.putIfAbsent(name, true).isEmpty

  def addDuplicateDefinition(name: Name, fileNames: List[String]): Unit = duplicateDefinitionQueue
    .add(name -> fileNames)

  // Contexts sorted by source file name, cached until a new compilation unit is loaded, so
  // that global symbol lookup stays fast while scanning units in a deterministic order
  private val sortedContextCache = AtomicReference[(Int, List[Context])]((0, Nil))

  def getAllContextsSorted: List[Context] =
    val cached = sortedContextCache.get()
    if cached._1 == contextTable.size then
      cached._2
    else
      // Key the cache by the size of the snapshot that was actually sorted, so a concurrent
      // insertion between reading the size and copying the values cannot leave the cache
      // permanently inconsistent
      val snapshot = contextTable.values.toList
      val sorted   = snapshot.sortBy(_.compilationUnit.sourceFile.fileName)
      sortedContextCache.set((snapshot.size, sorted))
      sorted

  // Globally available definitions (Name and Symbols)
  var defs: GlobalDefinitions = scala.compiletime.uninitialized

  var defaultCatalog: Catalog = loadCatalog(compilerOptions)
  var defaultSchema: String   = compilerOptions.schema.getOrElse("main")

  var workEnv: WorkEnv = compilerOptions.workEnv

  def init(using rootContext: Context): Unit =
    this.rootContext = rootContext
    defs = GlobalDefinitions(using rootContext)

  def getRootContext: Context                             = rootContext
  def getContextUnit: Option[CompilationUnit]             = contextUnit
  def setContextUnit(unit: CompilationUnit): Unit         = contextUnit = Some(unit)
  def setContextUnit(unit: Option[CompilationUnit]): Unit = contextUnit = unit

  def newSymbolId: Int =
    symbolCount += 1
    symbolCount

  def getFile(name: NameExpr): VirtualFile = files.getOrElseUpdate(name, LocalFile(name.fullName))

  private def loadCatalog(compilerOptions: CompilerOptions): Catalog = InMemoryCatalog(
    catalogName = compilerOptions.catalog.getOrElse("memory"),
    functions = Nil
  )

  /**
    * Get the context corresponding to the specific source file in the CompilationUnit
    * @param unit
    * @param scope
    * @return
    */
  def getContextOf(unit: CompilationUnit, scope: Scope = Scope.NoScope): Context = contextTable
    .getOrElseUpdate(unit.sourceFile, Context(global = this, scope = scope, compilationUnit = unit))

  def getAllContexts: List[Context]                 = contextTable.values.toList
  def getAllCompilationUnits: List[CompilationUnit] = getAllContexts
    .map(_.compilationUnit)
    .filter(!_.isEmpty)

end GlobalContext

/**
  * Context conveys the current state of the compilation, including defined types, table
  * definitions, and the current compilation unit.
  *
  * Context and Scope are mutable, and the compiler will update them as it processes the source
  * code.
  */
case class Context(
    // The global context, which will be shared by all Context objects in the same compilation run
    global: GlobalContext,
    outer: Context = Context.NoContext,
    // The owner of the current context. If it is NoSymbol, this is a global context.
    // Usually this is a package symbol
    owner: Symbol = Symbol.NoSymbol,
    scope: Scope = Scope.NoScope,
    compilationUnit: CompilationUnit = CompilationUnit.empty,
    importDefs: List[Import] = Nil,
    // If true, evaluate test expressions
    isDebugRun: Boolean = false,
    queryProgressMonitor: QueryProgressMonitor = QueryProgressMonitor.noOp,
    // Typer-specific state (following Scala 3 pattern)
    typerState: TyperState = TyperState.empty
) extends LogSupport:
  def isGlobalContext: Boolean = compilationUnit.isPreset || owner.isNoSymbol

  /**
    * Iterate this context and its enclosing (outer) contexts, innermost first
    */
  def outersIterator: Iterator[Context] = Iterator
    .iterate(this)(_.outer)
    .takeWhile(_ ne Context.NoContext)

  def isContextCompilationUnit: Boolean =
    !compilationUnit.isEmpty &&
      global
        .getContextUnit
        .exists { ctxUnit =>
          ctxUnit eq compilationUnit
        }

  def sourceLocationAt(span: Span): SourceLocation = compilationUnit
    .sourceFile
    .sourceLocationAt(span.start)

  // Get the context catalog
  // TODO support multiple catalogs
  def catalog: Catalog = global.defaultCatalog
  def dbType: DBType   = catalog.dbType

  def defaultSchema: String = global.defaultSchema

  def workEnv: WorkEnv = global.workEnv

  def withDebugRun(isDebug: Boolean): Context = this.copy(isDebugRun = isDebug)
  def withQueryProgressMonitor(monitor: QueryProgressMonitor): Context = this.copy(
    queryProgressMonitor = monitor
  )

  // Typer-specific methods (following Scala 3 pattern)

  /**
    * Set a new typer state
    */
  def withTyperState(ts: TyperState): Context = copy(typerState = ts)

  /**
    * Set the input relation type for typing expressions
    */
  def withInputType(tpe: RelationType): Context = copy(typerState = typerState.withInputType(tpe))

  /**
    * Set the input type from a generic Type
    */
  def withInputType(tpe: Type): Context = copy(typerState = typerState.withInputType(tpe))

  /**
    * Get the current input relation type
    */
  def inputType: RelationType = typerState.inputType

  /**
    * Add a typing error
    */
  def addTyperError(err: TyperError): Unit = typerState.addError(err)

  /**
    * Check if there are any typing errors
    */
  def hasTyperErrors: Boolean = typerState.hasErrors

  /**
    * Get typing errors in order they were added
    */
  def typerErrors: List[TyperError] = typerState.errorsInOrder

  /**
    * Create a new context with an additional import
    * @param i
    * @return
    */
  def withImport(i: Import): Context = this.copy(
    outer = this,
    scope = scope,
    importDefs = i :: importDefs
  )

  def withCompilationUnit[U](newCompileUnit: CompilationUnit): Context = global
    .getContextOf(newCompileUnit)
    // Propagate debug run flag
    .withDebugRun(isDebugRun)
    // Propagate the same query progress monitor
    .withQueryProgressMonitor(queryProgressMonitor)

  def enter(sym: Symbol): Unit = scope.enter(sym)(using this)

  def newContext(owner: Symbol): Context = Context(
    global = global,
    outer = this,
    owner = owner,
    scope = scope.newChildScope,
    compilationUnit = compilationUnit,
    importDefs = Nil,
    isDebugRun = isDebugRun,
    queryProgressMonitor = queryProgressMonitor,
    typerState = typerState
  )

  def findTermSymbolByName(name: String): Option[Symbol] = findSymbolByName(Name.termName(name))

  def findSymbolByName(name: Name): Option[Symbol] =
    // Search the current scope first
    var foundSymbol: Option[Symbol] = scope.lookupSymbol(name)

    // Search the imported symbols
    if foundSymbol.isEmpty then
      importDefs.collectFirst {
        case i: Import if i.importRef.leafName == name.name =>
          for
            ctx <- global.getAllContexts
            if foundSymbol.isEmpty
          do
            ctx
              .compilationUnit
              .knownSymbols
              .collectFirst {
                case s: Symbol if s.name == name =>
                  foundSymbol = Some(s)
              }
      }

    if foundSymbol.isEmpty then
      // Search global symbols. Contexts are scanned in source file name order so that a name
      // defined in multiple files resolves deterministically (#93)
      for
        ctx <- global.getAllContextsSorted
        if foundSymbol.isEmpty && ctx.isGlobalContext
      do
        foundSymbol = ctx.compilationUnit.knownSymbols.find(_.name == name)
      // A top-level name defined in multiple files still shadows the later definitions
      // (#93). Warn once per name
      foundSymbol.foreach { sym =>
        if global.needsDuplicateCheck(name) then
          val definingFiles =
            (
              for
                ctx <- global.getAllContextsSorted
                if ctx.isGlobalContext
                s <- ctx.compilationUnit.knownSymbols
                if s.name == name
              yield ctx.compilationUnit.sourceFile.fileName
            ).distinct
          if definingFiles.size > 1 then
            global.addDuplicateDefinition(name, definingFiles)
            warn(
              s"Duplicate top-level definition of '${name}' in ${definingFiles.mkString(
                  ", "
                )}; the definition in ${definingFiles.head} is used"
            )
      }

    if isContextCompilationUnit then
      trace(s"Looked up ${name} in ${compilationUnit.sourceFile.fileName} => ${foundSymbol}")
    foundSymbol
  end findSymbolByName

  def findDataFile(path: String): Option[String] = global
    .compilerOptions
    .sourceFolders
    .map(folder => dataFilePath(path))
    .find(file => SourceIO.existsFile(file))

  def findCompilationUnit(path: String): Option[CompilationUnit] = global
    .getAllCompilationUnits
    .find(_.sourceFile.fileName == path)

  def dataFilePath(relativePath: String): String =
    if relativePath.startsWith("s3://") || relativePath.startsWith("https://") then
      relativePath
    else
      s"${global.compilerOptions.workEnv.path}/${relativePath}"

  /**
    * Return a resolved file path or URL
    * @param path
    * @return
    */
  def getDataFile(path: String): String =
    findDataFile(path) match
      case None =>
        throw StatusCode.FILE_NOT_FOUND.newException(s"${path} is not found")
      case Some(f) =>
        f

end Context

object Context:
  val NoContext: Context = Context(null)

  def testGlobalContext(path: String): GlobalContext =
    val global = GlobalContext(
      CompilerOptions(sourceFolders = List(path), workEnv = WorkEnv(path = path))
    )
    val rootContext = global.getContextOf(unit = CompilationUnit.empty, scope = Scope.newScope(0))
    // Need to initialize the global context before running the analysis phases
    global.init(using rootContext)
    global
