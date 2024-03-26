package com.treasuredata.flow.lang.compiler

trait Phase(
    // The name of the phase
    val name: String
):
  def runOn(units: List[CompilationUnit], context: Context): List[CompilationUnit] =
    val buf = List.newBuilder[CompilationUnit]
    for unit <- units do
      context.withCompilationUnit(unit) { ctx =>
        buf += run(unit, ctx)
      }
    buf.result()

  def run(unit: CompilationUnit, context: Context): CompilationUnit
