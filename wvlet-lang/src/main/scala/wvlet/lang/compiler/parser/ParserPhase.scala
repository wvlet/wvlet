package wvlet.lang.compiler.parser

import wvlet.lang.compiler.{CompilationUnit, Context, Phase, SourceFile}
import wvlet.lang.model.plan.{LogicalPlan, PackageDef}
import wvlet.log.LogSupport

/**
  * Parse *.wv files and create untyped plans
  */
object ParserPhase extends Phase("parser") with LogSupport:

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    unit.unresolvedPlan = parse(unit)
    unit

  def parseSourceFolder(path: String): Seq[LogicalPlan] = CompilationUnit.fromPath(path).map(parse)

  def parse(compileUnit: CompilationUnit): LogicalPlan =
    debug(s"Parsing ${compileUnit.sourceFile}")

    val p    = WvletParser(compileUnit)
    val plan = p.parse()
    plan
