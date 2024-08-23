package wvlet.lang.runner

import wvlet.lang.catalog.Catalog
import wvlet.lang.catalog.InMemoryCatalog
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.airspec.AirSpec

class DuckDBExecutorTest extends AirSpec:

  private val duckDB = QueryExecutor(DuckDBConnector(prepareTPCH = false))

  override def afterAll: Unit = duckDB.close()

  test("weblog") {
    pendingUntil("Stabilizing the new parser")
    duckDB.executeSingleSpec("examples/cdp_behavior", "behavior.wv")
  }

end DuckDBExecutorTest
