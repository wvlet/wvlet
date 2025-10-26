package wvlet.lang.runner.connector.trino

import wvlet.airframe.codec.JDBCCodec.ResultSetCodec
import wvlet.airspec.AirSpec

class TableFunctionTest extends AirSpec:
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

  test("Run table functions") { (trino: TrinoConnector) =>
    test("hello table function") {
      trino.runQuery("select * from TABLE(wvlet.hello('wvlet'))") { rs =>
        rs.next() shouldBe true
        val name = rs.getString(1)
        debug(name)
        name shouldBe "Hello wvlet!"
        rs.next() shouldBe false
      }
    }

    test("hello duckdb table function") {
      trino.runQuery(s"""
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
           |""".stripMargin) { rs =>
        val json = ResultSetCodec(rs).toJson
        debug(json)
        json shouldBe """[{"c_custkey":1,"c_nationkey":15,"c_phone":"25-989-741-2988"}]"""
      }
    }
  }

end TableFunctionTest
