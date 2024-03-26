package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.compiler.analyzer.{Resolver, ScanTypes}
import com.treasuredata.flow.lang.compiler.parser.FlowParser
import com.treasuredata.flow.lang.compiler.transform.Transform
import com.treasuredata.flow.lang.model.plan.LogicalPlan

object Compiler:

  /**
    * Phases for text-based analysis of the source code
    */
  def analysisPhases: List[Phase] = List(
    FlowParser,
    ScanTypes,
    Resolver
  )

  /**
    * Phases for transforming the logical plan trees
    */
  def transformPhases: List[Phase] = List(
    Transform
  )

  def allPhases: List[List[Phase]] = List(
    analysisPhases,
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
