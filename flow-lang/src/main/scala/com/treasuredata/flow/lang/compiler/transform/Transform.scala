package com.treasuredata.flow.lang.compiler.transform

import com.treasuredata.flow.lang.compiler.{CompilationUnit, Context, Phase, RewriteRule}
import com.treasuredata.flow.lang.model.plan.{FlowPlan, LogicalPlan}

/**
  * Transform contains a rule set for optimizing flow trees.
  */
object Transform extends Phase("transform"):
  val rules = Seq[RewriteRule](
    Incrementalize
  )

  def apply(plan: LogicalPlan, context: Context): LogicalPlan =
    rules.foldLeft(plan) { (p, rule) =>
      rule.transform(p, context)
    }

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    // TODO
    unit
