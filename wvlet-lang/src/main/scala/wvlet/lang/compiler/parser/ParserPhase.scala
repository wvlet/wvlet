/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.compiler.parser

import wvlet.lang.compiler.{CompilationUnit, Context, Phase, SourceFile}
import wvlet.lang.model.expr.MarkdownPlan
import wvlet.lang.model.plan.{LogicalPlan, PackageDef}
import wvlet.log.LogSupport

/**
  * Parse *.wv files and create untyped plans
  */
object ParserPhase extends Phase("parser") with LogSupport:

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    unit.unresolvedPlan = parse(unit, context)
    unit

  def parse(compileUnit: CompilationUnit, ctx: Context): LogicalPlan =
    debug(s"Parsing ${compileUnit.sourceFile}")

    val plan = parseOnly(compileUnit, ctx.isContextCompilationUnit)
    debug(
      s"[parsed tree for ${compileUnit.sourceFile}:\n${plan.pp}\n${compileUnit
          .sourceFile
          .getContent}"
    )
    plan

  /**
    * Just parse the given CompilationUnit and return an unresolved LogicalPlan
    * @param compilationUnit
    * @param isContextUnit
    * @return
    */
  def parseOnly(compilationUnit: CompilationUnit, isContextUnit: Boolean = true): LogicalPlan =
    val plan =
      if compilationUnit.sourceFile.isSQL then
        val p = SqlParser(unit = compilationUnit, isContextUnit = isContextUnit)
        p.parse()
      else if compilationUnit.sourceFile.isMarkdown then
        val p   = MarkdownParser(unit = compilationUnit)
        val doc = p.parse()
        // Wrap MarkdownDocument (Expression) in MarkdownPlan (LogicalPlan)
        MarkdownPlan(doc, doc.span)
      else
        val p = WvletParser(unit = compilationUnit, isContextUnit = isContextUnit)
        p.parse()

    plan

end ParserPhase
