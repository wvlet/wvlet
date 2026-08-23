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
package wvlet.lang.runner.connector

import wvlet.lang.api.StatusCode
import wvlet.lang.api.v1.frontend.FrontendApi.QueryCancelRequest
import wvlet.lang.api.v1.frontend.FrontendApi.QueryInfoRequest
import wvlet.lang.api.v1.frontend.FrontendRPC
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QuerySelection
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.lang.compiler.Name
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.runner.ErrorResult
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.TableRows
import wvlet.lang.runner.compat.Sleep
import wvlet.uni.http.Http
import wvlet.uni.log.LogSupport
import wvlet.uni.util.ULID

import scala.collection.immutable.ListMap
import scala.util.Try

/**
  * Remote execution against a wvlet server: submits the ORIGINAL wvlet query text over the server's
  * RPC API, polls until a terminal state, and adapts the structured result into the runner's
  * [[QueryResult]] shape. The server compiles and runs the query with its own profile, catalogs,
  * and credentials — thin clients (wvc, the Node CLI) need no local engine at all.
  *
  * One server-side session is held per client instance (a fresh ULID session id), so `use`
  * statements and temp tables persist across [[runQuery]] calls. Built purely on uni's sync HTTP
  * client, so it runs unchanged on JVM, Node.js, and Native.
  */
class WvletServerClient(
    baseUri: String,
    // Server-side profile to run under; None uses the server's default profile
    profile: Option[String] = None,
    maxRows: Int = WvletServerClient.DefaultMaxRows,
    pollIntervalMillis: Long = 100
) extends AutoCloseable
    with LogSupport:

  private val rpc = FrontendRPC.newRPCSyncClient(
    Http
      .client
      .withBaseUri(baseUri)
      // The Native (libcurl) channel does not transparently decompress gzip responses the way
      // the JVM client does; ask the server for identity encoding so all platforms decode alike
      .withRequestFilter(req => req.setHeader("Accept-Encoding", "identity"))
      .newSyncClient
  )

  // One server-side session per client so engine/schema switches survive across statements
  private val sessionId = ULID.newULIDString

  override def close(): Unit = rpc.close()

  /** Submit a wvlet query, wait for completion, and adapt the result. */
  def runQuery(wvletQuery: String): QueryResult = toQueryResult(awaitCompletion(submit(wvletQuery)))

  /** Submit a wvlet query and return its server-side query id without waiting. */
  def submit(wvletQuery: String): ULID =
    rpc
      .FrontendApi
      .submitQuery(
        QueryRequest(
          query = wvletQuery,
          querySelection = QuerySelection.All,
          profile = profile,
          maxRows = Some(maxRows),
          sessionId = Some(sessionId)
        )
      )
      .queryId

  /** Poll the server until the query reaches a terminal state. */
  def awaitCompletion(queryId: ULID): QueryInfo =
    var info = rpc.FrontendApi.getQueryInfo(QueryInfoRequest(queryId, pageToken = "0"))
    while !info.status.isFinished do
      Sleep.sleepMillis(pollIntervalMillis)
      info = rpc.FrontendApi.getQueryInfo(QueryInfoRequest(queryId, pageToken = info.pageToken))
    info

  /** Best-effort server-side cancellation. */
  def cancel(queryId: ULID): QueryInfo = rpc.FrontendApi.cancelQuery(QueryCancelRequest(queryId))

  private def toQueryResult(info: QueryInfo): QueryResult =
    info.status match
      case QueryStatus.FINISHED =>
        info
          .result
          .map { r =>
            val fields =
              r.schema
                .map { c =>
                  NamedType(
                    Name.termName(c.name),
                    Try(DataType.parse(c.typeName)).getOrElse(DataType.AnyType)
                  )
                }
                .toList
            val names = fields.map(_.name.name)
            val rows  = r.rows.map(row => ListMap.from(names.zip(row))).toList
            TableRows(
              SchemaType(None, Name.NoTypeName, fields),
              rows,
              r.actualTotalRows.getOrElse(rows.size)
            )
          }
          .getOrElse(QueryResult.empty)
      case QueryStatus.CANCELED =>
        ErrorResult(
          StatusCode
            .QUERY_EXECUTION_FAILURE
            .newException(s"Query ${info.queryId} was canceled on the server")
        )
      case _ =>
        val error = info
          .errors
          .headOption
          .map(e => e.statusCode.newException(info.errors.map(_.message).mkString("; ")))
          .getOrElse(
            StatusCode
              .QUERY_EXECUTION_FAILURE
              .newException(s"Query ${info.queryId} failed with status ${info.status}")
          )
        ErrorResult(error)

end WvletServerClient

object WvletServerClient:
  /** Default bound for structured rows fetched from the server */
  val DefaultMaxRows: Int = 10000
