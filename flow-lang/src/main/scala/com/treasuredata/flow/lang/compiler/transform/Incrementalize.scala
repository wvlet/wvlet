package com.treasuredata.flow.lang.compiler.transform

import com.treasuredata.flow.lang.compiler.{CompilationUnit, Context, Phase, RewriteRule}
import com.treasuredata.flow.lang.model.plan.*

/**
  * Generate incremental query plans corresponding for Subscription nodes
  */
object Incrementalize extends Phase("incrementalize"):
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    unit.resolvedPlan.transformUp { case s: Subscribe =>
      info(s)
      s
    }
    unit
