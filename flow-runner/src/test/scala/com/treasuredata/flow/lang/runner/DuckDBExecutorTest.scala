package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.catalog.Catalog
import com.treasuredata.flow.lang.catalog.InMemoryCatalog
import com.treasuredata.flow.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.airspec.AirSpec

class DuckDBExecutorTest extends AirSpec:

  private val duckDB = QueryExecutor(DuckDBConnector(prepareTPCH = false))

  override def afterAll: Unit = duckDB.close()

  test("weblog") {
    pendingUntil("Stabilizing the new parser")
    duckDB.executeSingleSpec("examples/cdp_behavior", "behavior.flow")
  }

end DuckDBExecutorTest
