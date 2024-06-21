package com.treasuredata.flow.lang.compiler

import wvlet.log.LogSupport

trait Phase(
    // The name of the phase
    val name: String
) extends LogSupport:
  def runOn(units: List[CompilationUnit], context: Context): List[CompilationUnit] =
    debug(s"Running phase ${name}")
    val buf = List.newBuilder[CompilationUnit]
    for unit <- units do
      val newContext = context.withCompilationUnit(unit)
      buf += run(unit, newContext)
    buf.result()

  def run(unit: CompilationUnit, context: Context): CompilationUnit
