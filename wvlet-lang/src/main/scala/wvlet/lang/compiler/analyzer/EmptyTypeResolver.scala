package wvlet.lang.compiler.analyzer

import wvlet.lang.compiler.{CompilationUnit, Context, Phase}

/**
  * A dummy type resolver for skipping TypeResolver phase
  */
object EmptyTypeResolver extends Phase("empty-type-resolver"):
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    unit.resolvedPlan = unit.unresolvedPlan
    unit
