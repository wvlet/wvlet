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

import wvlet.lang.compiler.query.QueryMetric

/**
  * In-flight query handle returned by [[SqlConnector.submit]]. Lets the caller poll progress,
  * cancel from any thread, and finally `await()` the materialized result.
  *
  * Implementations must guarantee:
  *   - `await()` is idempotent: a second call returns the same `QueryResult`.
  *   - `cancel()` is idempotent and safe to call from a different thread than `await()`.
  *   - `close()` cancels the query if it has not reached a terminal state, then releases any
  *     underlying transport resources.
  */
trait QueryHandle extends AutoCloseable:
  def queryId: Option[String]
  def state: QueryState
  def stats: QueryStats
  def await(): QueryResult
  def cancel(): Unit

/**
  * Normalized lifecycle states across SQL backends. Per-backend mapping functions live next to the
  * connector implementation (e.g. `QueryState.fromTrino`).
  */
enum QueryState:
  case Queued,
    Planning,
    Starting,
    Running,
    Blocked,
    Finishing,
    Finished,
    Failed,
    Canceled

  /** Terminal states stop the pagination/polling loop. */
  def isTerminal: Boolean =
    this match
      case Finished | Failed | Canceled =>
        true
      case _ =>
        false

end QueryState

object QueryState:

  /**
    * Map a Trino client-protocol state string (also used verbatim by `io.trino.jdbc.QueryStats`) to
    * a wvlet [[QueryState]]. Unknown values fall back to `Running` so a future Trino addition
    * doesn't crash the consumer — terminal states are an explicit, closed list.
    */
  def fromTrino(state: String): QueryState =
    state.toUpperCase match
      case "QUEUED" =>
        Queued
      case "PLANNING" =>
        Planning
      case "STARTING" =>
        Starting
      case "RUNNING" =>
        Running
      case "BLOCKED" =>
        Blocked
      case "FINISHING" =>
        Finishing
      case "FINISHED" =>
        Finished
      case "FAILED" =>
        Failed
      case "CANCELED" =>
        Canceled
      case _ =>
        Running

end QueryState

/**
  * Snapshot of backend-reported query progress. All numeric fields are `Option` because not every
  * backend reports every metric (e.g. DuckDB doesn't have splits; Snowflake's REST API returns
  * different counters). Consumers should treat absence as "not available", not zero.
  */
case class QueryStats(
    state: QueryState,
    rowsProcessed: Option[Long] = None,
    bytesProcessed: Option[Long] = None,
    elapsedMs: Option[Long] = None,
    splitsCompleted: Option[Int] = None,
    splitsTotal: Option[Int] = None
) extends QueryMetric

object QueryStats:
  val empty: QueryStats = QueryStats(QueryState.Queued)
