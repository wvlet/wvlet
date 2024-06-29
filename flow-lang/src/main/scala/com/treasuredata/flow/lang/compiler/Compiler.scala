package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.compiler.analyzer.{
  PostTypeScan,
  PreTypeScan,
  SymbolLabeler,
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
    SymbolLabeler, // Assign unique Symbol to each LogicalPlan and Expression nodes, a and assign a lazy DataType
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

  def presetLibraryPaths: List[String] = List("flow-lang/src/main/resources/flow-stdlib")

class Compiler(phases: List[List[Phase]] = Compiler.allPhases) extends LogSupport:
  /**
    * @param sourceFolder
    *   A folder containing src and data folders
    * @return
    */
  def compile(sourceFolder: String): CompileResult = compile(List(sourceFolder), sourceFolder)

  def compile(sourceFolders: List[String], contextFolder: String): CompileResult =
    val sourcePaths = Compiler.presetLibraryPaths ++ sourceFolders
    var units: List[CompilationUnit] = sourcePaths.flatMap { path =>
      val srcPath = s"${path}/src"
      CompilationUnit.fromPath(srcPath)
    }

    val global      = GlobalContext(sourceFolders = sourceFolders, workingFolder = contextFolder)
    val rootContext = global.getContextOf(CompilationUnit.empty)
    for
      phaseGroup <- phases
      phase      <- phaseGroup
    do
      units = phase.runOn(units, rootContext)

    CompileResult(units, this, rootContext)

end Compiler

case class CompileResult(units: List[CompilationUnit], compiler: Compiler, context: Context):
  def typedPlans: List[LogicalPlan] = units.map(_.resolvedPlan).filter(_.nonEmpty)

  /**
    * Extract compilation results for a specific file name
    * @param fileName
    * @return
    */
  def inFile(fileName: String): Option[CompilationUnit] =
    units.filter(_.sourceFile.fileName == fileName).headOption
