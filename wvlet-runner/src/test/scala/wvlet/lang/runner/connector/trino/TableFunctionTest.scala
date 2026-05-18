package wvlet.lang.runner.connector.trino

import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.test.WvletDITest

class TableFunctionTest extends WvletDITest:
  if inCI then
    skip(s"This is a demo function for integrating Trino with DuckDB")

  initDesign { d =>
    d.bindInstance[TestTrinoServer](TestTrinoServer().withCustomMemoryPlugin)
      .bindProvider { (server: TestTrinoServer) =>
        TrinoConfig(
          catalog = "memory",
          schema = "main",
          hostAndPort = server.address,
          useSSL = false,
          user = Some("test"),
          password = Some("")
        )
      }
  }

  private given QueryProgressMonitor = QueryProgressMonitor.noOp

  test("Run table functions") {
    test("hello table function") {
      val trino = dep[TrinoConnector].asSqlConnector
      val r     = trino.execute("select * from TABLE(wvlet.hello('wvlet'))")
      r.rowCount shouldBe 1
      r.rows.head.values.head shouldBe Some("Hello wvlet!")
    }

    test("hello duckdb table function") {
      val trino = dep[TrinoConnector].asSqlConnector
      val r     = trino.execute(s"""
           |-- Projection in Trino
           |select c_custkey, c_nationkey, c_phone
           |-- Read a parquet file with DuckDB
           |from TABLE(
           |  duckdb.sql(
           |    'SELECT *
           |     FROM values(1, 15, ''25-989-741-2988'') as t(c_custkey, c_nationkey, c_phone)
           |     where c_custkey = 1'
           |  )
           |)
           |""".stripMargin)
      r.rowCount shouldBe 1
      r.columns.map(_.name.name) shouldBe List("c_custkey", "c_nationkey", "c_phone")
      r.rows.head.values shouldBe List(Some("1"), Some("15"), Some("25-989-741-2988"))
    }
  }

end TableFunctionTest
