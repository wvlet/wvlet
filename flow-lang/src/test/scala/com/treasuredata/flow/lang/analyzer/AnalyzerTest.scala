package com.treasuredata.flow.lang.analyzer

import wvlet.airspec.AirSpec

class AnalyzerTest extends AirSpec:
  test("analyze plan") {
    val plan = Analyzer.analyzeSourceFolder("examples/cdp_behavior/src")
    debug(plan)
  }
