package wvlet.lang.runner.connector.trino

import wvlet.airspec.AirSpec

class TableFunctionTest extends AirSpec:
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

  test("Create an in-memory schema and table") { (trino: TrinoConnector) =>
    trino.runQuery("select * from TABLE(wvlet.hello('wvlet'))") { rs =>
      rs.next() shouldBe true
      val name = rs.getString(1)
      debug(name)
      name shouldBe "hello"
      rs.next() shouldBe false
    }
  }
