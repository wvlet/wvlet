/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.runner.connector.duckdb

import wvlet.airframe.control.Control.withResource
import wvlet.lang.compiler.Name
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.airspec.AirSpec

class DuckDBConnectorTest extends AirSpec:
  initDesign:
    _.bindSingleton[DuckDBConnector]

  test("Create an in-memory schema and table"): (duckdb: DuckDBConnector) =>
    duckdb.withConnection: conn =>
      val ret = conn.createStatement().execute("create table a(id bigint)")

    duckdb.getTableDef("memory", "main", "a") shouldBe defined

    test("drop table"):
      duckdb.dropTable("memory", "main", "a")
      duckdb.getTableDef("memory", "main", "a") shouldBe empty

  test("Create an in-memory schema"): (duckdb: DuckDBConnector) =>
    test("drop schema"):
      duckdb.dropSchema("memory", "b")
      duckdb.getSchema("memory", "b") shouldBe empty

  test("Read SchemaType"): (duckdb: DuckDBConnector) =>
    duckdb.withConnection: conn =>
      withResource(conn.createStatement()): stmt =>
        stmt.execute("create table a(c1 bigint, c2 varchar, c3 integer[])")

    val schemas = duckdb.listTableDefs("memory", "main")
    schemas.head.schemaType.fields shouldBe
      List[NamedType](
        NamedType(Name.termName("c1"), DataType.LongType),
        NamedType(Name.termName("c2"), DataType.StringType),
        NamedType(Name.termName("c3"), DataType.ArrayType(DataType.IntType))
      )

  test("read catalog"): (duckdb: DuckDBConnector) =>
    val catalog = duckdb.getCatalog("memory", "main")
    catalog.catalogName shouldBe "memory"
    catalog.listSchemaNames shouldContain "main"
    catalog.listTableNames("main")

  test("read functions"): (duckdb: DuckDBConnector) =>
    val functions = duckdb.listFunctions("memory")
    functions shouldNotBe empty

end DuckDBConnectorTest
