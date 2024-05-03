package com.treasuredata.flow.lang.compiler.runner

import com.treasuredata.flow.lang.compiler.Compiler
import wvlet.airspec.AirSpec

class DuckDBExecutorTest extends AirSpec:

  test("q1.flow") {
    DuckDBExecutor.execute("examples/basic", "q1.flow")
  }

  test("q2.flow") {
    DuckDBExecutor.execute("examples/basic", "q2.flow")
  }
