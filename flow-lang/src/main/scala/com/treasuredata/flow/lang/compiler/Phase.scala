package com.treasuredata.flow.lang.compiler

import wvlet.log.{LogSupport, Logger}

abstract class Phase(
    // The name of the phase
    val name: String
) extends LogSupport:

  def runOn(units: List[CompilationUnit], context: Context): List[CompilationUnit] =
    val buf = List.newBuilder[CompilationUnit]
    for unit <- units do
      trace(s"Running phase ${name} on ${unit.sourceFile.file}")
      val sourceContext = context.withCompilationUnit(unit)
      buf += run(unit, sourceContext)
    buf.result()

  def run(unit: CompilationUnit, context: Context): CompilationUnit
