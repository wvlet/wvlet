package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.catalog.Catalog
import com.treasuredata.flow.lang.catalog.InMemoryCatalog
import com.treasuredata.flow.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.airspec.AirSpec

class DuckDBExecutorTest extends AirSpec:

  private val duckDB = QueryExecutor(DuckDBConnector(prepareTPCH = false))

  override def afterAll: Unit = duckDB.close()

  test("q1.flow") {
    duckDB.execute("spec/basic", "q1.flow")
  }

  test("q2.flow") {
    duckDB.execute("spec/basic", "q2.flow")
  }

  test("table function") {
    val result = duckDB.execute("spec/duckdb", "from_table_function.flow")
    debug(result)
  }

  test("raw sql") {
    val result = duckDB.execute("spec/duckdb", "raw_sql.flow")
    debug(result)
  }

  test("weblog") {
    pendingUntil("Stabilizing the new parser")
    duckDB.execute("examples/cdp_behavior", "behavior.flow")
  }
