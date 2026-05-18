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
package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.compiler.connector.QueryState
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.uni.test.UniTest

/**
  * Exercise [[DuckDBSqlConnector]] over the `SqlConnector` trait surface — `submit` → `await`,
  * `state`, `cancel`, the default `execute` path. Runs on every platform that supports
  * `DuckDB.execute` (auto-skips when libduckdb isn't loadable).
  */
class DuckDBSqlConnectorTest extends UniTest:

  private given QueryProgressMonitor = QueryProgressMonitor.noOp

  private def skipIfUnavailable(): Unit =
    if !DuckDB.canExecute then
      ignore("DuckDB.execute not available on this platform (libduckdb missing?)")

  test("submit returns an already-Finished handle with the query result") {
    skipIfUnavailable()
    val conn = DuckDBSqlConnector()
    try
      val h = conn.submit("select 1 as one, 'duck' as src")
      try
        h.state shouldBe QueryState.Finished
        h.queryId shouldBe None
        val r = h.await()
        r.columnCount shouldBe 2
        r.rowCount shouldBe 1
        r.columns.map(_.name.name) shouldBe List("one", "src")
        r.rows.head.values shouldBe List(Some("1"), Some("duck"))
      finally
        h.close()
    finally
      conn.close()
  }

  test("await is idempotent — second call returns the same QueryResult instance") {
    skipIfUnavailable()
    val conn = DuckDBSqlConnector()
    try
      val h     = conn.submit("select 42 as answer")
      val first = h.await()
      h.await() shouldBe first
    finally
      conn.close()
  }

  test("execute (default trait method) returns the materialized result directly") {
    skipIfUnavailable()
    val r = DuckDBSqlConnector().execute("select 'hi' as greeting")
    r.rowCount shouldBe 1
    r.rows.head.values.head shouldBe Some("hi")
  }

  test("cancel on a synchronous DuckDB query is a safe no-op") {
    skipIfUnavailable()
    val conn = DuckDBSqlConnector()
    try
      val h = conn.submit("select 1")
      // Already Finished by the time submit returns; cancel must not throw.
      h.cancel()
      h.state shouldBe QueryState.Finished
    finally
      conn.close()
  }

end DuckDBSqlConnectorTest
