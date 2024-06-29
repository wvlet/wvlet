package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.catalog.Catalog
import com.treasuredata.flow.lang.model.expr.{Name, UnquotedIdentifier}
import com.treasuredata.flow.lang.model.plan.Import
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
case class GlobalContext(sourceFolders: List[String] = List.empty, workingFolder: String):
  private var symbolCount = 0

  // Loaded data files, etc.
  private val files = new ConcurrentHashMap[Name, VirtualFile]().asScala

  // Loaded contexts for source .flow files
  private val contextTable = new ConcurrentHashMap[SourceFile, Context]().asScala

  def newSymbolId: Int =
    symbolCount += 1
    symbolCount

  def getFile(name: Name): VirtualFile = files
    .getOrElseUpdate(name, LocalFile(name.leafName, name.fullName))

  def getContextOf(unit: CompilationUnit): Context = contextTable
    .getOrElseUpdate(unit.sourceFile, Context(global = this, compilationUnit = unit))

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
    owner: Symbol = Symbol.NoSymbol,
    scope: Scope = Scope.NoScope,
    compilationUnit: CompilationUnit = CompilationUnit.empty
) extends LogSupport:

  private var importDefs = List.empty[Import]

  // Get the context catalog
  // TODO support multiple catalogs
  def catalog: Catalog = ???

  def addImport(i: Import): Context =
    importDefs = i :: importDefs
    this

  def withCompilationUnit[U](newCompileUnit: CompilationUnit): Context = global
    .getContextOf(newCompileUnit)

  def findDataFile(path: String): Option[String] = global
    .sourceFolders
    .map(folder => dataFilePath(path))
    .find(file => new java.io.File(file).exists())

  def dataFilePath(relativePath: String): String = s"${global.workingFolder}/data/${relativePath}"

  def getDataFile(path: String): String =
    findDataFile(path) match
      case None =>
        throw StatusCode.FILE_NOT_FOUND.newException(s"${path} is not found")
      case Some(f) =>
        f

end Context

object Context:
  val NoContext: Context = Context(null)
