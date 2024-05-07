package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.catalog.Catalog
import wvlet.log.LogSupport

/**
  * Context conveys the current state of the compilation, including defined types, table definitions, and the current
  * compilation unit.
  *
  * Context and Scope are mutable, and the compiler will update them as it processes the source code.
  */
case class Context(
    sourceFolders: List[String] = List.empty
) extends LogSupport:
  private var _compileUnit: CompilationUnit = CompilationUnit.empty
  private var _scope: Scope                 = Scope()

  def scope: Scope                 = _scope
  def compileUnit: CompilationUnit = _compileUnit

  // Get the context catalog
  // TODO support multiple catalogs
  def catalog: Catalog = ???

  def withCompilationUnit[U](newCompileUnit: CompilationUnit)(block: Context => U): U =
    val prev = _compileUnit
    try
      _compileUnit = newCompileUnit
      block(this)
    finally _compileUnit = prev

  def findDataFile(path: String): Option[String] =
    sourceFolders
      .map(folder => s"${folder}/data/${path}")
      .find(file => new java.io.File(file).exists())

  def getDataFile(path: String): String =
    findDataFile(path) match
      case None =>
        throw StatusCode.FILE_NOT_FOUND.newException(s"${path} is not found")
      case Some(f) => f
