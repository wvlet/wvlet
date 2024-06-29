package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.compiler.{CompilationUnit, Context, Phase}
import com.treasuredata.flow.lang.model.plan.{Import, LogicalPlan, PackageDef}

/**
  * Assign unique Symbol to each LogicalPlan and Expression nodes, a and assign a lazy DataType
  */
object SymbolLabeler extends Phase("symbol-labeler"):
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    label(unit.unresolvedPlan, context)
    unit

  private def label(plan: LogicalPlan, context: Context): Unit =
    def iter(tree: LogicalPlan): Context =
      tree match
        case p: PackageDef =>
          p.statements
            .foreach { stmt =>
              debug(s"labeling ${stmt.pp}")
            }
          context
        case d: Import =>
          context.global.newSymbolId
          context
        case _ =>
          context
    iter(plan)
