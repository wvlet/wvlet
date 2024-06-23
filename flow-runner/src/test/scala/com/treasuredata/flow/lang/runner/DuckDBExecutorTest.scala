package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.catalog.Catalog
import com.treasuredata.flow.lang.catalog.InMemoryCatalog
import wvlet.airspec.AirSpec

class DuckDBExecutorTest extends AirSpec:

  pendingUntil("Stabilizing the new parser")

  test("q1.flow") {
    DuckDBExecutor.execute("examples/basic", "q1.flow")
  }

  test("q2.flow") {
    DuckDBExecutor.execute("examples/basic", "q2.flow")
  }

  test("weblog") {
    DuckDBExecutor.execute("examples/cdp_behavior", "behavior.flow")
  }
