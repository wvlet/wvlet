package wvlet.lang.compiler.codegen

import wvlet.lang.compiler.codegen.CodeFormatter.Doc
import wvlet.lang.model.plan.LogicalPlan

/**
 * A common interface for query printers
 */
trait QueryPrinter(protected val formatter: CodeFormatter):

  /**
   * Generate an intermediate Doc representation of the given logical plan
   * @param plan
   * @return
   */
  def render(plan: LogicalPlan): Doc

  /**
   * Generate a formatted code from the given logical plan
   * @param l
   */
  def print(plan: LogicalPlan): String =
    val doc = render(plan)
    formatter.format(doc)
