package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.catalog.Catalog
import com.treasuredata.flow.lang.model.expr.UnquotedIdentifier
import com.treasuredata.flow.lang.model.plan.ImportDef
import wvlet.log.LogSupport

/**
  * Context conveys the current state of the compilation, including defined types, table
  * definitions, and the current compilation unit.
  *
  * Context and Scope are mutable, and the compiler will update them as it processes the source
  * code.
  */
case class Context(
    outer: Context = Context.NoContext,
    owner: Symbol = Symbol.NoSymbol,
    scope: Scope = Scope.NoScope,
    compilationUnit: CompilationUnit = CompilationUnit.empty,
    sourceFolders: List[String] = List.empty,
    workingFolder: String
) extends LogSupport:

  private var symbolCount = 0
  def newSymbolId: Int =
    symbolCount += 1
    symbolCount

  private var importDefs = List.empty[ImportDef]

  // Get the context catalog
  // TODO support multiple catalogs
  def catalog: Catalog = ???

  def addImport(i: ImportDef): Context =
    importDefs = i :: importDefs
    this

  def withCompilationUnit[U](newCompileUnit: CompilationUnit): Context = this
    .copy(outer = this, compilationUnit = newCompileUnit)

  def findDataFile(path: String): Option[String] = sourceFolders
    .map(folder => dataFilePath(path))
    .find(file => new java.io.File(file).exists())

  def dataFilePath(relativePath: String): String = s"${workingFolder}/data/${relativePath}"

  def getDataFile(path: String): String =
    findDataFile(path) match
      case None =>
        throw StatusCode.FILE_NOT_FOUND.newException(s"${path} is not found")
      case Some(f) =>
        f

end Context

object Context:
  val NoContext: Context = Context(workingFolder = ".")
