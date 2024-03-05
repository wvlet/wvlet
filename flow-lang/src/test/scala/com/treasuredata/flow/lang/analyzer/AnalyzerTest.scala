package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.model.plan.Query
import wvlet.airspec.AirSpec

class AnalyzerTest extends AirSpec:
  test("analyze behavior plan") {
    val plan = Analyzer.analyzeSourceFolder("examples/cdp_behavior/src/behavior")
    plan.flatMap { plan =>
      plan.logicalPlans.collect { case p =>
        debug(p.pp)
      }
    }
  }

  test("analyze customer plan") {
    val plan = Analyzer.analyzeSourceFolder("examples/cdp_behavior/src/customer")
    plan.flatMap { plan =>
      plan.logicalPlans.collect { case p =>
        debug(p.pp)
      }
    }
  }
