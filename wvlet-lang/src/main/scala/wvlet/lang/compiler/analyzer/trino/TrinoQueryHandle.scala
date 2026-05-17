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
package wvlet.lang.compiler.analyzer.trino

import wvlet.lang.compiler.Name
import wvlet.lang.compiler.connector.QueryHandle
import wvlet.lang.compiler.connector.QueryResult
import wvlet.lang.compiler.connector.QueryResultRow
import wvlet.lang.compiler.connector.QueryState
import wvlet.lang.compiler.connector.QueryStats
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.model.DataType.NamedType
import wvlet.uni.http.Http
import wvlet.uni.http.HttpMethod
import wvlet.uni.http.HttpSyncClient
import wvlet.uni.http.Request
import wvlet.uni.json.JSON.JSONArray
import wvlet.uni.json.JSON.JSONLong
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.log.LogSupport

/**
  * Cross-platform `QueryHandle` backed by uni's `HttpSyncClient` and the Trino client protocol.
  *
  * Lifecycle:
  *   - `Trino.submit` calls `consume(firstResponse)` before returning the handle, so `queryId` and
  *     the initial `state`/`stats`/columns are populated by the time the caller sees the handle.
  *   - `await()` runs the `nextUri` pagination loop, accumulating rows and pushing per-page
  *     [[QueryStats]] through the [[QueryProgressMonitor]]. Idempotent — caches the materialized
  *     result for subsequent calls.
  *   - `cancel()` is safe from any thread. Sets a cooperative flag (so an in-flight loop iteration
  *     exits at the next boundary) and fires `DELETE` on the last `nextUri`. The DELETE is
  *     best-effort: a non-2xx response is logged, never thrown, so the consumer thread isn't
  *     surprised by a cancellation that the server rejected.
  *   - `close()` cancels if not yet terminal, then closes the `HttpSyncClient`. Idempotent.
  */
class TrinoQueryHandle(
    client: HttpSyncClient,
    config: TrinoConfig,
    progressMonitor: QueryProgressMonitor
) extends QueryHandle
    with LogSupport:

  @volatile
  private var _queryId: Option[String] = None

  @volatile
  private var _stats: QueryStats = QueryStats(QueryState.Queued)

  @volatile
  private var lastNextUri: Option[String] = None

  @volatile
  private var cancelRequested: Boolean = false

  @volatile
  private var closed: Boolean = false

  @volatile
  private var cachedResult: Option[QueryResult] = None

  private var columnsSnapshot: Option[Seq[NamedType]] = None
  private val rowsBuilder                             = List.newBuilder[QueryResultRow]

  override def queryId: Option[String] = _queryId
  override def state: QueryState       = _stats.state
  override def stats: QueryStats       = _stats

  /**
    * Drive the pagination loop until the query reaches a terminal state or `nextUri` is absent.
    * Subsequent calls return the cached result without re-running the loop.
    */
  override def await(): QueryResult = synchronized {
    cachedResult match
      case Some(r) =>
        r
      case None =>
        while lastNextUri.isDefined && !cancelRequested && !state.isTerminal do
          val uri  = lastNextUri.get
          val req  = Trino.withTrinoHeaders(Request(method = HttpMethod.GET, uri = uri), config)
          val resp = Trino.sendOrThrow(client, req)
          val json = Trino.parseBody(resp)
          Trino.checkError(json)
          consume(json)
        if cancelRequested && !state.isTerminal then
          _stats = _stats.copy(state = QueryState.Canceled)
        val result = QueryResult(columnsSnapshot.getOrElse(Seq.empty), rowsBuilder.result())
        cachedResult = Some(result)
        result
  }

  /**
    * Ask Trino to abort the query. Sets a cooperative flag the `await()` loop checks at the next
    * iteration boundary, then issues `DELETE` on the last known `nextUri` so the server stops
    * working immediately. Idempotent and safe from any thread; failures from the DELETE are logged
    * but never thrown — callers expect cancel to be non-fatal.
    *
    * Uses a freshly-constructed `HttpSyncClient` for the DELETE rather than the handle's `client`,
    * because uni's `HttpSyncClient` may not be safe under concurrent `send` calls on every
    * platform (libcurl's `curl_easy` is single-threaded; Node's worker_threads channel is
    * single-threaded; only Apache HttpClient is fully concurrent). One extra short-lived client
    * costs little and makes thread-safety platform-independent.
    */
  override def cancel(): Unit =
    if !cancelRequested && !state.isTerminal then
      cancelRequested = true
      lastNextUri.foreach { uri =>
        val cancelClient = Http.client.withBaseUri(config.baseUri).newSyncClient
        try
          val req  = Trino.withTrinoHeaders(Request(method = HttpMethod.DELETE, uri = uri), config)
          val resp = cancelClient.send(req)
          if !resp.status.isSuccessful then
            warn(
              s"Trino cancel for query ${_queryId.getOrElse("?")} returned ${resp
                  .status
                  .code} ${resp.status.reason}"
            )
        catch
          case e: Throwable =>
            warn(s"Trino cancel for query ${_queryId.getOrElse("?")} failed: ${e.getMessage}")
        finally cancelClient.close()
      }

  override def close(): Unit =
    if !closed then
      closed = true
      try
        if !state.isTerminal then
          cancel()
      finally client.close()

  /**
    * Internal: fold one Trino response page into the handle's accumulator. Used both by
    * `Trino.submit` (first page) and `await()` (subsequent pages).
    */
  private[trino] def consume(json: JSONObject): Unit =
    extractQueryId(json)
    collectColumns(json)
    collectRows(json)
    val newStats = parseStats(json)
    _stats = newStats
    lastNextUri = nextUri(json)
    progressMonitor.reportProgress(newStats)

  private def extractQueryId(json: JSONObject): Unit =
    if _queryId.isEmpty then
      json
        .get("id")
        .collect { case s: JSONString =>
          s.v
        }
        .foreach(id => _queryId = Some(id))

  private def collectColumns(json: JSONObject): Unit =
    if columnsSnapshot.isEmpty then
      json
        .get("columns")
        .foreach {
          case arr: JSONArray =>
            val cols =
              arr
                .v
                .collect { case obj: JSONObject =>
                  NamedType(
                    Name.termName(Trino.stringField(obj, "name")),
                    Trino.parseTrinoType(Trino.stringField(obj, "type", "any"))
                  )
                }
                .toList
            columnsSnapshot = Some(cols)
          case _ =>
        }

  private def collectRows(json: JSONObject): Unit = json
    .get("data")
    .foreach {
      case arr: JSONArray =>
        arr
          .v
          .foreach {
            case row: JSONArray =>
              rowsBuilder += QueryResultRow(row.v.map(Trino.stringifyCell).toList)
            case _ =>
          }
      case _ =>
    }

  private def nextUri(json: JSONObject): Option[String] = json
    .get("nextUri")
    .collect { case s: JSONString =>
      s.v
    }

  /**
    * Parse Trino's `stats` block into wvlet's [[QueryStats]]. Trino reports both `processedRows` /
    * `processedBytes` (lifetime totals) and `state`. Missing or non-numeric fields fall back to
    * `None` — every numeric is optional on the wvlet side.
    */
  private def parseStats(json: JSONObject): QueryStats =
    val statsObj: Option[JSONObject] = json
      .get("stats")
      .collect { case obj: JSONObject =>
        obj
      }
    statsObj match
      case None =>
        _stats
      case Some(obj) =>
        QueryStats(
          state = obj
            .get("state")
            .collect { case s: JSONString =>
              QueryState.fromTrino(s.v)
            }
            .getOrElse(_stats.state),
          rowsProcessed = longField(obj, "processedRows"),
          bytesProcessed = longField(obj, "processedBytes"),
          elapsedMs = longField(obj, "elapsedTimeMillis"),
          splitsCompleted = longField(obj, "completedSplits").map(_.toInt),
          splitsTotal = longField(obj, "totalSplits").map(_.toInt)
        )

  private def longField(obj: JSONObject, name: String): Option[Long] = obj
    .get(name)
    .collect { case n: JSONLong =>
      n.v
    }

end TrinoQueryHandle
