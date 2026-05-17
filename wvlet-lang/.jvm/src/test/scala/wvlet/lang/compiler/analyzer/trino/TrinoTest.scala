package wvlet.lang.compiler.analyzer.trino

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import wvlet.lang.compiler.connector.QueryState
import wvlet.lang.compiler.connector.QueryStats
import wvlet.lang.compiler.query.QueryMetric
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.uni.test.UniTest

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

/**
  * Drives the [[Trino]] client against an in-process HTTP server that speaks just enough of the
  * Trino client protocol to validate the request shape, pagination loop, columns/rows decoding, and
  * error handling. The same client code runs unchanged on Node.js (uni worker_threads sync HTTP)
  * and Native (libcurl); uni's own channel test suites cover those transports.
  */
class TrinoTest extends UniTest:

  test("execute single-page query, decoding columns + rows + nulls") {
    val responses = Seq("""{
        |  "id": "q1",
        |  "columns": [
        |    {"name": "n", "type": "bigint"},
        |    {"name": "label", "type": "varchar"}
        |  ],
        |  "data": [[1, "a"], [2, null]],
        |  "stats": {"state": "FINISHED"}
        |}""".stripMargin)
    withFakeTrino(responses) { (port, calls) =>
      val cfg = TrinoConfig(host = "localhost", port = port, user = "alice", catalog = Some("c"))
      val r   = Trino.execute("select 1", cfg)
      r.columns.map(_.name.name) shouldBe List("n", "label")
      r.rows.map(_.values) shouldBe List(List(Some("1"), Some("a")), List(Some("2"), None))
      val first = calls().head
      first.method shouldBe "POST"
      first.path shouldBe "/v1/statement"
      first.headers.get("x-trino-user") shouldBe Some("alice")
      first.headers.get("x-trino-catalog") shouldBe Some("c")
      first.body shouldBe "select 1"
    }
  }

  test("execute paginates through nextUri until results are exhausted") {
    val page1 =
      """{
      |  "id": "q2",
      |  "columns": [{"name": "x", "type": "integer"}],
      |  "data": [[1], [2]],
      |  "nextUri": "__SERVER__/v1/statement/q2/2",
      |  "stats": {"state": "RUNNING"}
      |}""".stripMargin
    val page2 =
      """{
      |  "id": "q2",
      |  "data": [[3]],
      |  "nextUri": "__SERVER__/v1/statement/q2/3",
      |  "stats": {"state": "RUNNING"}
      |}""".stripMargin
    val page3 =
      """{
      |  "id": "q2",
      |  "data": [[4]],
      |  "stats": {"state": "FINISHED"}
      |}""".stripMargin
    withFakeTrino(Seq(page1, page2, page3)) { (port, calls) =>
      val cfg = TrinoConfig(host = "localhost", port = port, user = "u", catalog = Some("c"))
      val r   = Trino.execute("select x from t", cfg)
      r.rows.map(_.values.head) shouldBe List(Some("1"), Some("2"), Some("3"), Some("4"))
      val recorded = calls()
      recorded.map(_.method) shouldBe List("POST", "GET", "GET")
      recorded.map(_.path) shouldBe
        List("/v1/statement", "/v1/statement/q2/2", "/v1/statement/q2/3")
      // Every request — initial POST AND each follow-up nextUri GET — must carry the user/catalog
      // headers. Some gateways (e.g. Treasure Data's Presto front-end) inspect them on every
      // request; omitting them on follow-up GETs fails with PERMISSION_DENIED.
      recorded.foreach { r =>
        r.headers.get("x-trino-user") shouldBe Some("u")
        r.headers.get("x-trino-catalog") shouldBe Some("c")
      }
    }
  }

  test("execute falls back to any for compound / unknown column types") {
    val body =
      """{
      |  "id": "q4",
      |  "columns": [
      |    {"name": "ids", "type": "array(bigint)"},
      |    {"name": "tags", "type": "map(varchar,varchar)"}
      |  ],
      |  "data": [[[1, 2], {"a": "b"}]],
      |  "stats": {"state": "FINISHED"}
      |}""".stripMargin
    withFakeTrino(Seq(body)) { (port, _: () => Vector[Recorded]) =>
      val cfg = TrinoConfig(host = "localhost", port = port, user = "u")
      val r   = Trino.execute("select 1", cfg)
      r.columns.map(_.name.name) shouldBe List("ids", "tags")
      // Compound types map to AnyType rather than throwing; rows are still rendered as JSON.
      r.columnCount shouldBe 2
      r.rowCount shouldBe 1
      val row = r.rows.head.values
      row(0) shouldBe Some("[1,2]")
      row(1) shouldBe Some("{\"a\":\"b\"}")
    }
  }

  test("execute surfaces Trino errors from the error block") {
    val errBody =
      """{
      |  "id": "q3",
      |  "error": {"errorName": "SYNTAX_ERROR", "message": "mismatched input"},
      |  "stats": {"state": "FAILED"}
      |}""".stripMargin
    withFakeTrino(Seq(errBody)) { (port, _: () => Vector[Recorded]) =>
      val cfg = TrinoConfig(host = "localhost", port = port, user = "u")
      val ex  = intercept[Exception] {
        Trino.execute("garbage", cfg)
      }
      ex.getMessage shouldContain "SYNTAX_ERROR"
      ex.getMessage shouldContain "mismatched input"
    }
  }

  test("submit populates queryId, state, and stats before await") {
    val body =
      """{
      |  "id": "q5",
      |  "columns": [{"name": "n", "type": "bigint"}],
      |  "data": [[1]],
      |  "stats": {"state": "RUNNING", "processedRows": 7, "elapsedTimeMillis": 42}
      |}""".stripMargin
    withFakeTrino(Seq(body)) { (port, _: () => Vector[Recorded]) =>
      val cfg    = TrinoConfig(host = "localhost", port = port, user = "u")
      val handle = Trino.submit("select 1", cfg)
      try
        handle.queryId shouldBe Some("q5")
        handle.state shouldBe QueryState.Running
        handle.stats.rowsProcessed shouldBe Some(7L)
        handle.stats.elapsedMs shouldBe Some(42L)
      finally
        handle.close()
    }
  }

  test("cancel issues DELETE on the last nextUri and transitions state to Canceled") {
    val pendingPage =
      """{
      |  "id": "q6",
      |  "columns": [{"name": "x", "type": "integer"}],
      |  "data": [[1]],
      |  "nextUri": "__SERVER__/v1/statement/q6/2",
      |  "stats": {"state": "RUNNING"}
      |}""".stripMargin
    withFakeTrino(Seq(pendingPage)) { (port, calls) =>
      val cfg    = TrinoConfig(host = "localhost", port = port, user = "u", catalog = Some("c"))
      val handle = Trino.submit("select 1", cfg)
      try
        handle.state shouldBe QueryState.Running
        handle.cancel()
        // State must flip to Canceled the moment cancel() returns, even before await is called —
        // otherwise a caller polling handle.state from another thread would see stale "Running".
        handle.state shouldBe QueryState.Canceled
        // After cancel, await returns the rows collected so far without re-entering the loop.
        val r = handle.await()
        r.rows.map(_.values.head) shouldBe List(Some("1"))
        handle.state shouldBe QueryState.Canceled
        val recorded = calls()
        recorded.map(_.method) shouldBe List("POST", "DELETE")
        recorded(1).path shouldBe "/v1/statement/q6/2"
        // DELETE must carry the same X-Trino-* headers as GET/POST — some gateways route on them.
        recorded(1).headers.get("x-trino-user") shouldBe Some("u")
        recorded(1).headers.get("x-trino-catalog") shouldBe Some("c")
      finally
        handle.close()
    }
  }

  test("await keeps paginating while nextUri is present even if stats.state is FINISHED") {
    // Trino can report `state: FINISHED` while still serving a `nextUri` for buffered rows or the
    // final error page. The await loop must follow `nextUri`, not `state` — otherwise we truncate
    // the result. This is a regression guard for the protocol-vs-stats distinction.
    val page1 =
      """{
      |  "id": "q8",
      |  "columns": [{"name": "x", "type": "integer"}],
      |  "data": [[1]],
      |  "nextUri": "__SERVER__/v1/statement/q8/2",
      |  "stats": {"state": "FINISHED"}
      |}""".stripMargin
    val page2 =
      """{
      |  "id": "q8",
      |  "data": [[2], [3]],
      |  "stats": {"state": "FINISHED"}
      |}""".stripMargin
    withFakeTrino(Seq(page1, page2)) { (port, _: () => Vector[Recorded]) =>
      val cfg = TrinoConfig(host = "localhost", port = port, user = "u")
      val r   = Trino.execute("select 1", cfg)
      r.rows.map(_.values.head) shouldBe List(Some("1"), Some("2"), Some("3"))
    }
  }

  test("progress monitor receives per-page QueryStats with state mapped to wvlet's enum") {
    val page1 =
      """{
      |  "id": "q7",
      |  "columns": [{"name": "x", "type": "integer"}],
      |  "data": [[1]],
      |  "nextUri": "__SERVER__/v1/statement/q7/2",
      |  "stats": {"state": "RUNNING", "processedRows": 1, "completedSplits": 1, "totalSplits": 4}
      |}""".stripMargin
    val page2 =
      """{
      |  "id": "q7",
      |  "data": [[2]],
      |  "stats": {"state": "FINISHED", "processedRows": 2, "completedSplits": 4, "totalSplits": 4}
      |}""".stripMargin
    val observed               = scala.collection.mutable.ArrayBuffer.empty[QueryStats]
    given QueryProgressMonitor =
      new QueryProgressMonitor:
        override def reportProgress(metric: QueryMetric): Unit =
          metric match
            case s: QueryStats =>
              observed += s
            case _ =>
    withFakeTrino(Seq(page1, page2)) { (port, _: () => Vector[Recorded]) =>
      val cfg = TrinoConfig(host = "localhost", port = port, user = "u")
      Trino.execute("select 1", cfg)
      observed.map(_.state).toList shouldBe List(QueryState.Running, QueryState.Finished)
      observed.last.rowsProcessed shouldBe Some(2L)
      observed.last.splitsCompleted shouldBe Some(4)
      observed.last.splitsTotal shouldBe Some(4)
    }
  }

  private case class Recorded(
      method: String,
      path: String,
      headers: Map[String, String],
      body: String
  )

  private def withFakeTrino(pages: Seq[String])(body: (Int, () => Vector[Recorded]) => Unit): Unit =
    val recorded = scala.collection.mutable.ArrayBuffer.empty[Recorded]
    val server   = HttpServer.create(InetSocketAddress(0), 0)
    var index    = 0
    server.createContext(
      "/",
      new HttpHandler:
        def handle(ex: HttpExchange): Unit =
          try
            val method = ex.getRequestMethod
            val path   = ex.getRequestURI.getPath
            // JVM HttpServer normalises header name casing (capitalizes first letter, lowercases
            // the rest), so we re-key by lower-case for stable assertions across JDK versions.
            val headers =
              ex.getRequestHeaders
                .entrySet()
                .stream()
                .map[(String, String)](e => e.getKey.toLowerCase -> e.getValue.get(0))
                .toArray(n => new Array[(String, String)](n))
                .toMap
            val bodyBytes = ex.getRequestBody.readAllBytes()
            recorded +=
              Recorded(method, path, headers, new String(bodyBytes, StandardCharsets.UTF_8))
            // DELETE is a client-driven abort; the server acknowledges with an empty 200 and the
            // test's request log records it, but we don't consume a scripted page (the cancel can
            // arrive between any two GETs).
            if method == "DELETE" then
              ex.sendResponseHeaders(200, -1L)
            else
              val raw = pages(index.min(pages.size - 1))
              index += 1
              val payload = raw.replace(
                "__SERVER__",
                s"http://localhost:${server.getAddress.getPort}"
              )
              val bytes = payload.getBytes(StandardCharsets.UTF_8)
              ex.getResponseHeaders.add("Content-Type", "application/json")
              ex.sendResponseHeaders(200, bytes.length.toLong)
              val os = ex.getResponseBody
              try os.write(bytes)
              finally os.close()
          finally
            ex.close()
    )
    server.start()
    try body(server.getAddress.getPort, () => recorded.toVector)
    finally server.stop(0)
  end withFakeTrino

end TrinoTest
