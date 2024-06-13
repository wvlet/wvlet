package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.{CompilationUnit, Context, Phase, SourceFile}
import com.treasuredata.flow.lang.model.plan.{LogicalPlan, PackageDef}
import wvlet.log.LogSupport

/**
  * Parse *.flow files and create untyped plans
  */
object ParserPhase extends Phase("parser") with LogSupport:

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    unit.unresolvedPlan = parse(unit)
    unit

  def parseSourceFolder(path: String): Seq[LogicalPlan] =
    CompilationUnit.fromPath(path).map(parse)

  def parse(compileUnit: CompilationUnit): LogicalPlan =
    debug(s"Parsing ${compileUnit.sourceFile}")

    val p    = FlowParser(compileUnit)
    val plan = p.parse()
    plan
