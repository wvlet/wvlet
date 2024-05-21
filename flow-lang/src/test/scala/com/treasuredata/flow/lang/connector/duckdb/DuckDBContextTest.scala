package com.treasuredata.flow.lang.connector.duckdb

import wvlet.airspec.AirSpec

class DuckDBContextTest extends AirSpec:

  test("Create an in-memory schema and table"): (duckdb: DuckDBContext) =>
    duckdb.withConnection: conn =>
      val ret = conn.createStatement().execute("create table a(id bigint)")

    duckdb.getTable("memory", "main", "a") shouldBe defined

    test("drop table"):
      duckdb.dropTable("memory", "main", "a")
      duckdb.getTable("memory", "main", "a") shouldBe empty

  test("Create an in-memory schema"): (duckdb: DuckDBContext) =>
    test("drop schema"):
      duckdb.dropSchema("memory", "b")
      duckdb.getSchema("memory", "b") shouldBe empty
