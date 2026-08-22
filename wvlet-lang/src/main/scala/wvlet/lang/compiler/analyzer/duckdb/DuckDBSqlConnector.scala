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

import wvlet.lang.compiler.connector.QueryHandle
import wvlet.lang.compiler.connector.QueryResult
import wvlet.lang.compiler.connector.QueryState
import wvlet.lang.compiler.connector.QueryStats
import wvlet.lang.compiler.connector.SqlConnector
import wvlet.lang.compiler.query.QueryProgressMonitor

/**
  * Cross-platform `SqlConnector` impl for DuckDB. Wraps the synchronous [[DuckDB.execute]] so the
  * SqlConnector trait has a DuckDB impl alongside
  * [[wvlet.lang.compiler.analyzer.trino.TrinoSqlConnector]].
  *
  * DuckDB runs in-process and synchronously — there's no async submission, no server-side query id,
  * and no cancel mid-flight. `submit` therefore runs the query inline and returns an already-
  * finished [[QueryHandle]]; `state` is always `Finished`, `cancel` is a no-op. Callers that want
  * streaming progress should pass a [[QueryProgressMonitor]] — the underlying chunk-reader backends
  * (JDBC on JVM, libduckdb on Native, koffi on JS) don't surface per-batch stats today, but the
  * monitor is reserved for the same role when they do.
  *
  * Without a session, each `submit` invokes `DuckDB.execute(sql)`, which opens a fresh in-memory
  * database per call — in-memory tables don't persist across calls; that mode is the right tool for
  * one-shot CLI queries. Pass a [[DuckDBSession]] (from [[DuckDB.newSession]]) to run every
  * statement on the same connection so temp tables, models, and multi-statement scripts work; the
  * connector then owns the session and closes it in `close()`. The runner's JVM `DuckDBConnector`
  * exposes an equivalent stateful `asSqlConnector` view over its long-lived JDBC connection.
  */
class DuckDBSqlConnector(session: Option[DuckDBSession] = None) extends SqlConnector:

  override def submit(sql: String)(using QueryProgressMonitor): QueryHandle =
    val result =
      session match
        case Some(s) =>
          s.execute(sql)
        case None =>
          DuckDB.execute(sql)
    DuckDBSqlConnector.completedHandle(result)

  override def close(): Unit = session.foreach(_.close())

end DuckDBSqlConnector

object DuckDBSqlConnector:

  /** A connector over a fresh persistent session (in-memory, or file-backed when `path` is set). */
  def withNewSession(path: Option[String] = None): DuckDBSqlConnector = DuckDBSqlConnector(
    Some(DuckDB.newSession(path))
  )

  /**
    * Build a [[QueryHandle]] that wraps an already-materialized [[QueryResult]]. Used for backends
    * (DuckDB today) whose `submit` is synchronous so callers still see the trait's submit/await/
    * cancel surface. Also reused by the runner's stateful `DuckDBConnector.asSqlConnector` view.
    */
  private[lang] def completedHandle(result: QueryResult): QueryHandle =
    new QueryHandle:
      override def queryId: Option[String] = None
      override def state: QueryState       = QueryState.Finished
      override def stats: QueryStats       = QueryStats(QueryState.Finished)
      override def await(): QueryResult    = result
      override def cancel(): Unit          = ()
      override def close(): Unit           = ()

end DuckDBSqlConnector
