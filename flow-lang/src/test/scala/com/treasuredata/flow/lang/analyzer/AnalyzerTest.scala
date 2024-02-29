package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.CompileUnit
import wvlet.airspec.AirSpec
import wvlet.log.io.IOUtil

class AnalyzerTest extends AirSpec:
  test("analyze plan") {
    val src  = CompileUnit("examples/cdp_behavior/src/behavior.flow")
    val plan = Analyzer.analyze(src)
    trace(plan)
  }
