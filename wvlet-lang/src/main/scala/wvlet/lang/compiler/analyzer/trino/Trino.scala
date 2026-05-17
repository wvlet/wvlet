package wvlet.lang.compiler.analyzer.trino

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.connector.QueryHandle
import wvlet.lang.compiler.connector.QueryResult
import wvlet.lang.compiler.connector.SqlConnector
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.model.DataType
import wvlet.uni.http.Http
import wvlet.uni.http.HttpMethod
import wvlet.uni.http.HttpSyncClient
import wvlet.uni.http.Request
import wvlet.uni.http.Response
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONBoolean
import wvlet.uni.json.JSON.JSONDouble
import wvlet.uni.json.JSON.JSONLong
import wvlet.uni.json.JSON.JSONNull
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.json.JSON.JSONValue
import wvlet.uni.log.LogSupport

/**
  * Cross-platform Trino client built on uni's `HttpSyncClient`.
  *
  * Drives the Trino [Client Protocol](https://trino.io/docs/current/develop/client-protocol.html):
  *
  *   1. `POST /v1/statement` with the SQL text and `X-Trino-{User,Catalog,Schema,Source}` headers.
  *   2. Read `nextUri` from each response and `GET` it until either `nextUri` is absent (query is
  *      done) or `state` reaches a terminal value (`FINISHED`, `FAILED`, `CANCELED`).
  *   3. Decode the streaming `columns` + `data` payload into a [[QueryResult]] whose rows are the
  *      same `Seq[Option[String]]` shape DuckDB returns, so downstream printers don't need to care
  *      where the rows came from.
  *
  * No JDBC, no platform-specific drivers — pure HTTP, which means the same code runs unchanged on
  * JVM, Node.js (via uni's worker_threads sync HTTP channel), and Scala Native (via the libcurl
  * channel).
  *
  * `submit` returns a [[TrinoQueryHandle]] so callers can poll progress, read live `stats`, and
  * `cancel()` from another thread. `execute` is the synchronous facade for callers that just want
  * the materialized result.
  */
object Trino extends LogSupport:

  /**
    * Submit `sql` to the Trino coordinator described by `config`. Blocks only until the first
    * response (so `queryId` is populated), then returns a handle the caller drives via `await()` or
    * `cancel()`.
    */
  def submit(sql: String, config: TrinoConfig)(using
      progressMonitor: QueryProgressMonitor = QueryProgressMonitor.noOp
  ): TrinoQueryHandle =
    val client = Http.client.withBaseUri(config.baseUri).newSyncClient
    try
      val resp = sendOrThrow(client, withTrinoHeaders(startRequest(sql), config))
      val json = parseBody(resp)
      checkError(json)
      val handle = TrinoQueryHandle(client, config, progressMonitor)
      handle.consume(json)
      handle
    catch
      case e: Throwable =>
        client.close()
        throw e
  end submit

  /**
    * Run `sql` against the Trino coordinator described by `config` and return the materialized
    * result. Blocks until the query reaches a terminal state.
    *
    * Failures from the Trino server (errors in the `error` block, non-2xx responses, empty or
    * non-JSON bodies) are surfaced as `StatusCode.QUERY_EXECUTION_FAILURE` exceptions whose message
    * contains Trino's `errorName`/`message` payload when available.
    */
  def execute(sql: String, config: TrinoConfig)(using
      progressMonitor: QueryProgressMonitor = QueryProgressMonitor.noOp
  ): QueryResult =
    val h = submit(sql, config)
    try h.await()
    finally h.close()

  private[trino] def startRequest(sql: String): Request = Request(
    method = HttpMethod.POST,
    uri = "/v1/statement"
  ).withTextContent(sql)

  /**
    * Re-apply `X-Trino-*` headers on every request. Some gateways (e.g. Treasure Data's Presto
    * front-end) inspect the user header on every request to route to the right backend; omitting it
    * on follow-up GETs / DELETEs fails with `PERMISSION_DENIED`.
    */
  private[trino] def withTrinoHeaders(req: Request, config: TrinoConfig): Request =
    val base = req.setHeader("X-Trino-User", config.user).setHeader("X-Trino-Source", config.source)
    val withCatalog = config.catalog.foldLeft(base)((r, c) => r.setHeader("X-Trino-Catalog", c))
    config.schema.foldLeft(withCatalog)((r, s) => r.setHeader("X-Trino-Schema", s))

  /**
    * Send `req` and surface non-2xx responses as exceptions. `HttpSyncClient` already throws on
    * those codes; the explicit check is belt-and-braces in case retry policy changes upstream.
    */
  private[trino] def sendOrThrow(client: HttpSyncClient, req: Request): Response =
    val resp = client.send(req)
    if !resp.status.isSuccessful then
      throw StatusCode
        .QUERY_EXECUTION_FAILURE
        .newException(
          s"Trino request to ${req.uri} failed: ${resp.status.code} ${resp.status.reason}"
        )
    resp

  private[trino] def parseBody(resp: Response): JSONObject = resp
    .contentAsString
    .map { body =>
      JSON.parse(body) match
        case obj: JSONObject =>
          obj
        case other =>
          throw StatusCode
            .QUERY_EXECUTION_FAILURE
            .newException(s"Unexpected Trino response (not a JSON object): ${other}")
    }
    .getOrElse(
      throw StatusCode.QUERY_EXECUTION_FAILURE.newException("Empty body in Trino response")
    )

  private[trino] def stringField(json: JSONObject, name: String, default: String = "?"): String =
    json
      .get(name)
      .collect { case s: JSONString =>
        s.v
      }
      .getOrElse(default)

  private[trino] def checkError(json: JSONObject): Unit = json
    .get("error")
    .foreach {
      case err: JSONObject =>
        throw StatusCode
          .QUERY_EXECUTION_FAILURE
          .newException(
            s"Trino query failed (${stringField(err, "errorName")}): ${stringField(err, "message")}"
          )
      case _ =>
    }

  /**
    * Coerce a single cell to the same `Option[String]` shape DuckDB returns. Trino sends scalars as
    * JSON primitives and arrays/objects as JSON values; we render the latter via `toJSON` so the
    * CLI printer doesn't crash on structured columns.
    */
  private[trino] def stringifyCell(v: JSONValue): Option[String] =
    v match
      case _: JSONNull =>
        None
      case b: JSONBoolean =>
        Some(b.v.toString)
      case n: JSONLong =>
        Some(n.v.toString)
      case d: JSONDouble =>
        Some(d.v.toString)
      case s: JSONString =>
        Some(s.v)
      case other =>
        Some(other.toJSON)

  /**
    * Map a Trino type string (`bigint`, `varchar(100)`, `timestamp(3) with time zone`, …) to a
    * wvlet `DataType`. We strip trailing parameters before parsing because wvlet's `DataType.parse`
    * doesn't accept the full Trino type grammar — close-enough for column metadata used by the CLI
    * printer. Anything we still can't map (compound types like `array(bigint)`, vendor extensions,
    * future Trino additions) falls back to `any` so column display never breaks rendering.
    */
  private[trino] def parseTrinoType(name: String): DataType =
    val base = name.takeWhile(c => c != '(' && c != ' ').toLowerCase
    try
      DataType.parse(base)
    catch
      case _: Exception =>
        DataType.AnyType

end Trino

/**
  * `SqlConnector` implementation that talks Trino over uni's `HttpSyncClient`. Each `submit` builds
  * its own short-lived `HttpSyncClient` (owned by the returned [[TrinoQueryHandle]]) so the
  * connector instance itself holds no transport state.
  */
class TrinoSqlConnector(val config: TrinoConfig) extends SqlConnector:
  override def submit(sql: String)(using progressMonitor: QueryProgressMonitor): QueryHandle = Trino
    .submit(sql, config)

  override def close(): Unit = ()

end TrinoSqlConnector
