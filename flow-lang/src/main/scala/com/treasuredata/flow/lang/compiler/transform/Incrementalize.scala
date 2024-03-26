package com.treasuredata.flow.lang.compiler.transform

import com.treasuredata.flow.lang.compiler.{Context, RewriteRule}
import RewriteRule.PlanRewriter
import com.treasuredata.flow.lang.model.plan.*

object Incrementalize extends RewriteRule:
  override def apply(context: Context): PlanRewriter = { case s: Subscribe =>
    debug(s.watermarkColumn)
    debug(s.windowSize)
    s.child match
      case r: RelScan =>
        info(r)

    s
  }
