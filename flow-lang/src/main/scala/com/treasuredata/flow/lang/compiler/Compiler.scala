package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.compiler.analyzer.{Resolver, TypeScan}
import com.treasuredata.flow.lang.compiler.parser.FlowParser
import com.treasuredata.flow.lang.compiler.transform.Transform
import com.treasuredata.flow.lang.model.plan.LogicalPlan

object Compiler:

  def firstPhases: List[Phase] = List(
    FlowParser,
    TypeScan,
    Resolver
  )

  def transformPhases: List[Phase] = List(
    Transform
  )

  def allPhases: List[List[Phase]] = List(
    firstPhases,
    transformPhases
  )

class Compiler(phases: List[List[Phase]] = Compiler.allPhases):
  def compile(sourceFolder: String): CompileResult =
    compile(List(sourceFolder))

  def compile(sourceFolders: List[String]): CompileResult =
    var units: List[CompilationUnit] = sourceFolders.flatMap { folder =>
      CompilationUnit.fromPath(folder)
    }
    var ctx = Context()
    for
      phaseGroup <- phases
      phase      <- phaseGroup
    do units = phase.runOn(units, ctx)

    CompileResult(units, this, ctx)

case class CompileResult(
    units: List[CompilationUnit],
    compiler: Compiler,
    ctx: Context
):
  def typedPlans: List[LogicalPlan] =
    units.flatMap(_.typedPlan.logicalPlans)

trait Phase(
    // The name of the phase
    val name: String
):
  def runOn(units: List[CompilationUnit], context: Context): List[CompilationUnit] =
    val buf = List.newBuilder[CompilationUnit]
    for unit <- units do
      context.setCompilationUnit(unit)
      buf += run(unit, context)

    buf.result()

  def run(unit: CompilationUnit, context: Context): CompilationUnit
