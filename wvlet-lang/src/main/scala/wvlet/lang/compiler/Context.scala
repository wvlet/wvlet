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

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.{Catalog, InMemoryCatalog}
import wvlet.lang.model.expr.{NameExpr, UnquotedIdentifier}
import wvlet.lang.model.plan.Import
import wvlet.log.LogSupport

import java.util.concurrent.ConcurrentHashMap
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

  // Globally available definitions (Name and Symbols)
  var defs: GlobalDefinitions = _

  var defaultCatalog: Catalog = InMemoryCatalog(catalogName = "memory", functions = Nil)
  var defaultSchema: String   = compilerOptions.schema.getOrElse("main")

  var workEnv: WorkEnv = compilerOptions.workEnv

  def init(using rootContext: Context): Unit =
    this.rootContext = rootContext
    defs = GlobalDefinitions(using rootContext)

  def getRootContext: Context                             = rootContext
  def getContextUnit: Option[CompilationUnit]             = contextUnit
  def setContextUnit(unit: Option[CompilationUnit]): Unit = contextUnit = unit

  def newSymbolId: Int =
    symbolCount += 1
    symbolCount

  def getFile(name: NameExpr): VirtualFile = files.getOrElseUpdate(name, LocalFile(name.fullName))

  /**
    * Get the context corresponding to the specific source file in the CompilationUnit
    * @param unit
    * @param scope
    * @return
    */
  def getContextOf(unit: CompilationUnit, scope: Scope = Scope.NoScope): Context = contextTable
    .getOrElseUpdate(unit.sourceFile, Context(global = this, scope = scope, compilationUnit = unit))

  def getAllContexts: List[Context] = contextTable.values.toList
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
    isDebugRun: Boolean = false
) extends LogSupport:
  def isGlobalContext: Boolean = compilationUnit.isPreset || owner.isNoSymbol

  def isContextCompilationUnit: Boolean =
    !compilationUnit.isEmpty &&
      global
        .getContextUnit
        .exists { ctxUnit =>
          ctxUnit eq compilationUnit
        }

  // Get the context catalog
  // TODO support multiple catalogs
  def catalog: Catalog = global.defaultCatalog
  def dbType: DBType   = catalog.dbType

  def defaultSchema: String = global.defaultSchema

  def workEnv: WorkEnv = global.workEnv

  def withDebugRun(isDebug: Boolean): Context = this.copy(isDebugRun = isDebug)

  /**
    * Create a new context with an additional import
    * @param i
    * @return
    */
  def withImport(i: Import): Context = this
    .copy(outer = this, scope = scope, importDefs = i :: importDefs)

  def withCompilationUnit[U](newCompileUnit: CompilationUnit): Context = global
    .getContextOf(newCompileUnit)
    // Propagate debug run flag
    .withDebugRun(isDebugRun)

  def enter(sym: Symbol): Unit = scope.enter(sym)(using this)

  def newContext(owner: Symbol): Context = Context(
    global = global,
    outer = this,
    owner = owner,
    scope = scope.newChildScope,
    compilationUnit = compilationUnit,
    importDefs = Nil,
    isDebugRun = isDebugRun
  )

  def findTermSymbolByName(name: String): Option[Symbol] =
    val n = TermName(name)
    scope
      // Search local symbols
      .lookupSymbol(n)
      .orElse {
        // Search global symbols
        var foundSymbol: Option[Symbol] = None
        for
          ctx <- global.getAllContexts.filter(_.isGlobalContext)
          if foundSymbol.isEmpty
        do
          foundSymbol = ctx.compilationUnit.knownSymbols.find(_.name.name == name)

        foundSymbol
      }

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

  def getDataFile(path: String): String =
    findDataFile(path) match
      case None =>
        throw StatusCode.FILE_NOT_FOUND.newException(s"${path} is not found")
      case Some(f) =>
        f

end Context

object Context:
  val NoContext: Context = Context(null)
