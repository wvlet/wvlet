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

import wvlet.lang.compiler.connector.QueryResult

/**
  * A persistent DuckDB session: every [[execute]] runs on the SAME database connection, so
  * in-memory tables, temp views, and other session state survive across calls — unlike
  * [[DuckDB.execute]], which opens a fresh in-memory database per invocation.
  *
  * Obtain one via [[DuckDB.newSession]] (in-memory by default, file-backed when a path is given).
  * Sessions are single-threaded, must be closed by the caller, and throw `IllegalStateException` on
  * use after close. Each platform backs the session with its long-lived native handle: a JDBC
  * `DuckDBConnection` on JVM, koffi FFI handles on Node.js, and `duckdb_database` /
  * `duckdb_connection` C-API handles on Scala Native.
  */
trait DuckDBSession extends AutoCloseable:
  /** Run `sql` on this session's connection and materialize the result as strings. */
  def execute(sql: String): QueryResult

  /** Close the underlying connection (and database handle where applicable). Idempotent. */
  override def close(): Unit
