package com.treasuredata.flow.lang.connector.trino

import wvlet.airspec.AirSpec

class TrinoContextTest extends AirSpec:

  initDesign {
    _.bind[TestTrinoServer]
      .toInstance(new TestTrinoServer())
      .bind[TrinoConfig].toProvider { (server: TestTrinoServer) =>
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

  test("Create an in-memory schema and table"): (trino: TrinoContext) =>
    trino.createSchema("memory", "main")
    trino.getSchema("memory", "main") shouldBe defined

    trino.withConnection: conn =>
      conn.createStatement().execute("create table a(id bigint)")

    trino.getTable("memory", "main", "a") shouldBe defined

    test("drop table"):
      trino.dropTable("memory", "main", "a")
      trino.getTable("memory", "main", "a") shouldBe empty

    test("drop schema"):
      trino.dropSchema("memory", "main")
