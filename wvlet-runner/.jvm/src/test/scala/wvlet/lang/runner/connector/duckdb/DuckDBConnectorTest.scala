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

import wvlet.lang.connector.duckdb.DuckDBConnector

import wvlet.lang.compiler.Name
import wvlet.lang.compiler.connector.QueryState
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.test.WvletDITest
import wvlet.uni.control.Control.withResource

class DuckDBConnectorTest extends WvletDITest:
  initDesign:
    _.bindSingleton[DuckDBConnector]

  test("Create an in-memory schema and table"):
    val duckdb = dep[DuckDBConnector]
    duckdb.withConnection: conn =>
      conn.createStatement().execute("create table a(id bigint)")

    duckdb.getTableDef("memory", "main", "a") shouldBe defined

    // drop the table so subsequent tests in this spec see a clean slate
    duckdb.dropTable("memory", "main", "a")
    duckdb.getTableDef("memory", "main", "a") shouldBe empty

  test("Create an in-memory schema and drop it"):
    val duckdb = dep[DuckDBConnector]
    duckdb.dropSchema("memory", "b")
    duckdb.getSchema("memory", "b") shouldBe empty

  test("Read SchemaType"):
    val duckdb = dep[DuckDBConnector]
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

  test("read catalog"):
    val duckdb  = dep[DuckDBConnector]
    val catalog = duckdb.getCatalog("memory", "main")
    catalog.catalogName shouldBe "memory"
    catalog.listSchemaNames shouldContain "main"
    catalog.listTableNames("main")

  test("read functions"):
    val duckdb    = dep[DuckDBConnector]
    val functions = duckdb.listFunctions("memory")
    functions shouldNotBe empty

  test("should preserve in-memory tables across asSqlConnector submits"):
    given QueryProgressMonitor = QueryProgressMonitor.noOp
    val duckdb                 = dep[DuckDBConnector]
    val sc                     = duckdb.asSqlConnector

    sc.execute("create table sql_connector_state(id bigint)")
    sc.execute("insert into sql_connector_state values (1), (2), (3)")
    val r = sc.execute("select count(*) as cnt from sql_connector_state")
    r.rowCount shouldBe 1
    r.rows.head.values.head shouldBe Some("3")

    // The view shares the connector's long-lived connection, so the table is also visible
    // through the JDBC-side metadata methods
    duckdb.getTableDef("memory", "main", "sql_connector_state") shouldBe defined
    duckdb.dropTable("memory", "main", "sql_connector_state")

  test("should keep the typed JDBC path as the default query route"):
    // The stateful view is opt-in: exposing it via the `sqlConnector` capability would switch
    // QueryExecutor to varchar-coerced cross-platform rows and lose typed results
    val duckdb = dep[DuckDBConnector]
    duckdb.sqlConnector shouldBe empty

  test("should return an already-finished handle from asSqlConnector.submit"):
    given QueryProgressMonitor = QueryProgressMonitor.noOp
    val duckdb                 = dep[DuckDBConnector]
    val h                      = duckdb.asSqlConnector.submit("select 1 as one")
    h.state shouldBe QueryState.Finished
    h.queryId shouldBe None
    // cancel is a no-op after completion; await stays idempotent
    h.cancel()
    h.await().rows.head.values.head shouldBe Some("1")
    h.close()

end DuckDBConnectorTest
