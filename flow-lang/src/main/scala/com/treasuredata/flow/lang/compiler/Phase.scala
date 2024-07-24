package com.treasuredata.flow.lang.compiler

import wvlet.log.{LogSupport, Logger}

abstract class Phase(
    // The name of the phase
    val name: String
) extends LogSupport:

  protected def init(units: List[CompilationUnit], context: Context): Unit = {}

  protected def refineUnits(units: List[CompilationUnit]): List[CompilationUnit] = units

  def runOn(units: List[CompilationUnit], context: Context): List[CompilationUnit] =
    init(units, context)
    val buf = List.newBuilder[CompilationUnit]
    for unit <- units do
      trace(s"Running phase ${name} on ${unit.sourceFile.file}")
      val sourceContext = context.withCompilationUnit(unit)
      buf += run(unit, sourceContext)
    refineUnits(buf.result())

  def run(unit: CompilationUnit, context: Context): CompilationUnit
