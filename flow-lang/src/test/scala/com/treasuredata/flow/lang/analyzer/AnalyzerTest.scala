package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.model.plan.{Query, Subscribe}
import wvlet.airspec.AirSpec

class AnalyzerTest extends AirSpec:
  test("analyze behavior plan") {
    val result = Analyzer.analyzeSourceFolder("examples/cdp_behavior/src/behavior")
    result.plans.flatMap { plan =>
      plan.logicalPlans.collect { case p =>
        debug(p.pp)
      }
    }
  }

  test("analyze customer plan") {
    val result = Analyzer.analyzeSourceFolder("examples/cdp_behavior/src/customer")
    result.plans.flatMap { plan =>
      plan.logicalPlans.collect { case p =>
        debug(p.pp)
      }
    }
  }

  test("analyze behavior subscription") {
    val result = Analyzer.analyzeSourceFolder("examples/cdp_behavior/src/behavior")
    result.plans.foreach { plan =>
      plan.logicalPlans
        .collect {
          case q: Query =>
            debug(q.pp)
          case s: Subscribe =>
            debug(s.pp)
        }
    }
  }
