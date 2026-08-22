package wvlet.lang.runner.connector

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.compiler.analyzer.duckdb.DuckDB
import wvlet.lang.compiler.analyzer.trino.TrinoSqlConnector
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.uni.test.UniTest

/**
  * Cross-platform tests for [[SqlConnectorProvider]] — the ConnectorConfig → SqlConnector registry
  * shared by the JVM, Node.js, and Native runners. DuckDB-backed tests auto-skip where libduckdb
  * isn't loadable (`DuckDB.canExecute` gates them); Trino tests only construct connectors and never
  * contact a server.
  */
class SqlConnectorProviderTest extends UniTest:

  private given QueryProgressMonitor = QueryProgressMonitor.noOp

  private def duckdbConfig(name: String = "duckdb") = ConnectorConfig(
    name = name,
    `type` = "duckdb"
  )

  private def skipIfNoDuckDB(): Unit =
    if !DuckDB.canExecute then
      ignore("DuckDB execution is not available on this platform")

  test("cache connectors by config value equality") {
    skipIfNoDuckDB()
    val provider = SqlConnectorProvider()
    try
      val c1 = provider.getConnector(duckdbConfig())
      val c2 = provider.getConnector(duckdbConfig())
      c1 shouldBeTheSameInstanceAs c2
      val c3 = provider.getConnector(duckdbConfig(name = "second"))
      (c3 eq c1) shouldBe false
    finally
      provider.close()
  }

  test("duckdb connector preserves session state across executes") {
    skipIfNoDuckDB()
    val provider = SqlConnectorProvider()
    try
      val connector = provider.getConnector(duckdbConfig())
      connector.execute("create table t(id integer)")
      connector.execute("insert into t values (1), (2)")
      val r = connector.execute("select count(*) as n from t")
      r.rows.head.values.head shouldBe Some("2")
    finally
      provider.close()
  }

  test("default connector follows the profile's default engine") {
    skipIfNoDuckDB()
    import wvlet.lang.catalog.Profile
    val provider = SqlConnectorProvider(Profile.defaultDuckDBProfile)
    try
      val c = provider.defaultConnector
      c shouldBeTheSameInstanceAs provider.getConnector(Profile.defaultDuckDBProfile.defaultEngine)
    finally
      provider.close()
  }

  test("close closes cached connectors") {
    skipIfNoDuckDB()
    val provider  = SqlConnectorProvider()
    val connector = provider.getConnector(duckdbConfig())
    connector.execute("select 1")
    provider.close()
    intercept[IllegalStateException] {
      connector.execute("select 1")
    }
  }

  test("build a Trino connector from host config") {
    val provider = SqlConnectorProvider()
    try
      val c = provider.getConnector(
        ConnectorConfig(name = "td", `type` = "trino", host = Some("localhost"))
      )
      c.isInstanceOf[TrinoSqlConnector] shouldBe true
    finally
      provider.close()
  }

  test("reject a Trino config without host") {
    val provider = SqlConnectorProvider()
    try
      val err = intercept[Exception] {
        provider.getConnector(ConnectorConfig(name = "td", `type` = "trino"))
      }
      err.getMessage shouldContain "host"
    finally
      provider.close()
  }

  test("reject unsupported connector types with guidance") {
    val provider = SqlConnectorProvider()
    try
      val err = intercept[Exception] {
        provider.getConnector(ConnectorConfig(name = "sf", `type` = "snowflake"))
      }
      err.getMessage shouldContain "snowflake"
      err.getMessage shouldContain "duckdb, trino"
    finally
      provider.close()
  }

end SqlConnectorProviderTest
