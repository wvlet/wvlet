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
package wvlet.lang.compiler.connector

import wvlet.lang.compiler.query.QueryProgressMonitor

/**
  * Cross-platform interface for SQL backends (Trino, DuckDB, Snowflake, …). Unlike the JDBC-shaped
  * `DBConnector` in `wvlet-runner`, this trait does not depend on `java.sql.*` and therefore
  * compiles on JVM, Node.js (Scala.js), and Native.
  *
  * The connector returns a [[QueryHandle]] for each submitted query so callers can poll state, read
  * stats, and cancel mid-flight without blocking on a `Statement.cancel()` thread. Backends that
  * cannot offer true cancellation (e.g. a strictly synchronous JDBC driver) still implement
  * `cancel()` best-effort; the contract is "stop the work if possible, never throw".
  */
trait SqlConnector extends AutoCloseable:

  /**
    * Submit `sql` to the backend, blocking only until the server has accepted the query and
    * returned an initial response (so `QueryHandle.queryId` is populated). The caller drives the
    * rest via `await()` / `cancel()`.
    */
  def submit(sql: String)(using QueryProgressMonitor): QueryHandle

  /**
    * Convenience: submit, await terminal state, return the materialized result. Equivalent to
    * `submit(sql).use { _.await() }` — kept on the trait so most callers don't have to manage the
    * handle lifecycle.
    */
  def execute(sql: String)(using QueryProgressMonitor): QueryResult =
    val h = submit(sql)
    try h.await()
    finally h.close()

end SqlConnector
