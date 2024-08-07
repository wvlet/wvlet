package com.treasuredata.flow.lang.runner.connector.duckdb

import com.treasuredata.flow.lang.compiler.Name
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.NamedType
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

  test("Read SchemaType"): (duckdb: DuckDBContext) =>
    duckdb.withConnection: conn =>
      conn.createStatement().execute("create table a(c1 bigint, c2 varchar, c3 integer[])")

    val schemas = duckdb.getTableDefs("memory", "main")
    schemas.head.fields shouldBe
      List[NamedType](
        NamedType(Name.termName("c1"), DataType.LongType),
        NamedType(Name.termName("c2"), DataType.StringType),
        NamedType(Name.termName("c3"), DataType.ArrayType(DataType.IntType))
      )
