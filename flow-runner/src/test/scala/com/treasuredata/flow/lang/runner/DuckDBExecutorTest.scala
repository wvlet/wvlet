package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.catalog.Catalog
import com.treasuredata.flow.lang.catalog.InMemoryCatalog
import com.treasuredata.flow.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.airspec.AirSpec

class DuckDBExecutorTest extends AirSpec:

  pendingUntil("Stabilizing the new parser")

  private val duckDB = QueryExecutor(DuckDBConnector(prepareTPCH = false))

  override def afterAll: Unit = duckDB.close()

  test("q1.flow") {
    duckDB.execute("examples/basic", "q1.flow")
  }

  test("q2.flow") {
    duckDB.execute("examples/basic", "q2.flow")
  }

  test("weblog") {
    duckDB.execute("examples/cdp_behavior", "behavior.flow")
  }
