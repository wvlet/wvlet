package wvlet.lang.compiler.analyzer.trino

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.analyzer.duckdb.QueryResult
import wvlet.lang.compiler.analyzer.duckdb.QueryResultRow
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.uni.http.Http
import wvlet.uni.http.HttpMethod
import wvlet.uni.http.HttpSyncClient
import wvlet.uni.http.Request
import wvlet.uni.http.Response
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONArray
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
  *   2. Read `nextUri` from the response and `GET` it until either `nextUri` is absent (query is
  *      done) or `state` reaches a terminal value (`FINISHED`, `FAILED`, `CANCELED`).
  *   3. Decode the streaming `columns` + `data` payload into a [[QueryResult]] whose rows are the
  *      same `Seq[Option[String]]` shape DuckDB returns, so downstream printers don't need to care
  *      where the rows came from.
  *
  * No JDBC, no platform-specific drivers — pure HTTP, which means the same code runs unchanged on
  * JVM, Node.js (via uni's worker_threads sync HTTP channel), and Scala Native (via the libcurl
  * channel).
  */
object Trino extends TrinoCompat with LogSupport:
  // Idempotent — ensures uni's `Http.client` resolves a real channel factory before any request.
  installHttpFactory()

  /**
    * Run `sql` against the Trino coordinator described by `config` and return the materialized
    * result. Blocks until the query reaches a terminal state.
    *
    * Failures from the Trino server (errors in the `error` block, non-2xx responses, empty or
    * non-JSON bodies) are surfaced as `StatusCode.QUERY_EXECUTION_FAILURE` exceptions whose message
    * contains Trino's `errorName`/`message` payload when available.
    */
  def execute(sql: String, config: TrinoConfig): QueryResult =
    val client = Http.client.withBaseUri(config.baseUri).newSyncClient
    try
      val state = ResultState()
      var resp  = sendOrThrow(client, startRequest(sql, config))
      var done  = false
      while !done do
        val json = parseBody(resp)
        checkError(json)
        collectColumns(json, state)
        collectRows(json, state)
        nextUri(json) match
          case Some(uri) =>
            resp = sendOrThrow(client, Request(method = HttpMethod.GET, uri = uri))
          case None =>
            done = true
      QueryResult(state.columns.getOrElse(Seq.empty[NamedType]), state.rows.result())
    finally
      client.close()
  end execute

  private def startRequest(sql: String, config: TrinoConfig): Request =
    val base = Request(method = HttpMethod.POST, uri = "/v1/statement")
      .withTextContent(sql)
      .setHeader("X-Trino-User", config.user)
      .setHeader("X-Trino-Source", config.source)
    val withCatalog = config.catalog.foldLeft(base)((r, c) => r.setHeader("X-Trino-Catalog", c))
    config.schema.foldLeft(withCatalog)((r, s) => r.setHeader("X-Trino-Schema", s))

  /**
    * Send `req` and surface non-2xx responses as exceptions. `HttpSyncClient` already throws on
    * those codes; the explicit check is belt-and-braces in case retry policy changes upstream.
    */
  private def sendOrThrow(client: HttpSyncClient, req: Request): Response =
    val resp = client.send(req)
    if !resp.status.isSuccessful then
      throw StatusCode
        .QUERY_EXECUTION_FAILURE
        .newException(
          s"Trino request to ${req.uri} failed: ${resp.status.code} ${resp.status.reason}"
        )
    resp

  private def parseBody(resp: Response): JSONObject = resp
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

  private def stringField(json: JSONObject, name: String, default: String = "?"): String = json
    .get(name)
    .collect { case s: JSONString =>
      s.v
    }
    .getOrElse(default)

  private def checkError(json: JSONObject): Unit = json
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

  private def collectColumns(json: JSONObject, state: ResultState): Unit =
    if state.columns.isEmpty then
      json
        .get("columns")
        .foreach {
          case arr: JSONArray =>
            val cols =
              arr
                .v
                .collect { case obj: JSONObject =>
                  NamedType(
                    Name.termName(stringField(obj, "name")),
                    parseTrinoType(stringField(obj, "type", "any"))
                  )
                }
                .toList
            state.columns = Some(cols)
          case _ =>
        }

  private def collectRows(json: JSONObject, state: ResultState): Unit = json
    .get("data")
    .foreach {
      case arr: JSONArray =>
        arr
          .v
          .foreach {
            case row: JSONArray =>
              state.rows += QueryResultRow(row.v.map(stringifyCell).toList)
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
    * Coerce a single cell to the same `Option[String]` shape DuckDB returns. Trino sends scalars as
    * JSON primitives and arrays/objects as JSON values; we render the latter via `toJSON` so the
    * CLI printer doesn't crash on structured columns.
    */
  private def stringifyCell(v: JSONValue): Option[String] =
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
  private def parseTrinoType(name: String): DataType =
    val base = name.takeWhile(c => c != '(' && c != ' ').toLowerCase
    try
      DataType.parse(base)
    catch
      case _: Exception =>
        DataType.AnyType

  private final class ResultState:
    var columns: Option[Seq[NamedType]]                                              = None
    val rows: scala.collection.mutable.Builder[QueryResultRow, List[QueryResultRow]] =
      List.newBuilder

end Trino
