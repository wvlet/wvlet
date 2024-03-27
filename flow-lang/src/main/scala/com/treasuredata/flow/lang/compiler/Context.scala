package com.treasuredata.flow.lang.compiler

import wvlet.log.LogSupport

/**
  * Context conveys the current state of the compilation, including defined types, table definitions, and the current
  * compilation unit.
  *
  * Context and Scope are mutable, and the compiler will update them as it processes the source code.
  */
case class Context() extends LogSupport:
  private var _compileUnit: CompilationUnit = CompilationUnit.empty
  private var _scope: Scope                 = Scope()

  def scope: Scope                 = _scope
  def compileUnit: CompilationUnit = _compileUnit

  def withCompilationUnit[U](newCompileUnit: CompilationUnit)(block: Context => U): U =
    val prev = _compileUnit
    try
      _compileUnit = newCompileUnit
      block(this)
    finally _compileUnit = prev
