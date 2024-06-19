package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.compiler.analyzer.{
  PostTypeScan,
  PreTypeScan,
  TypeResolver,
  TypeScanner
}
import com.treasuredata.flow.lang.compiler.parser.{FlowParser, ParserPhase}
import com.treasuredata.flow.lang.compiler.transform.Incrementalize
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import wvlet.log.LogSupport

object Compiler:
  def default: Compiler = Compiler(phases = allPhases)

  /**
    * Phases for text-based analysis of the source code
    */
  def analysisPhases: List[Phase] = List(
    ParserPhase, // Parse *.flow files and create untyped plans
    PreTypeScan, // Collect all schema and types in the source paths
    PostTypeScan, // Post-process to resolve unresolved types, which cannot be found in the first type scan
    TypeResolver // Resolve concrete types for each LogicalPlan node
  )

  /**
    * Phases for transforming the logical plan trees
    */
  def transformPhases: List[Phase] = List(
    Incrementalize // Create an incrementalized plan for a subscription
  )

  def allPhases: List[List[Phase]] = List(analysisPhases, transformPhases)

class Compiler(phases: List[List[Phase]] = Compiler.allPhases) extends LogSupport:
  def compile(sourceFolder: String): CompileResult = compile(List(sourceFolder))

  def compile(sourceFolders: List[String]): CompileResult =
    var units: List[CompilationUnit] = sourceFolders.flatMap { folder =>
      val srcPath = s"${folder}/src"
      CompilationUnit.fromPath(srcPath)
    }
    val ctx = Context(sourceFolders = sourceFolders)
    for
      phaseGroup <- phases
      phase      <- phaseGroup
    do
      units = phase.runOn(units, ctx)

    CompileResult(units, this, ctx)

case class CompileResult(units: List[CompilationUnit], compiler: Compiler, context: Context):
  def typedPlans: List[LogicalPlan] = units.map(_.resolvedPlan).filter(_.nonEmpty)

  /**
    * Extract compilation results for a specific file name
    * @param fileName
    * @return
    */
  def inFile(fileName: String): Option[CompilationUnit] =
    units.filter(_.sourceFile.fileName == fileName).headOption
