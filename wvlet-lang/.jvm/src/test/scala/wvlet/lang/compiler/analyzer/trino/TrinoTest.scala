package wvlet.lang.compiler.analyzer.trino

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
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
      val cfg = TrinoConfig(host = "localhost", port = port, user = "u")
      val r   = Trino.execute("select x from t", cfg)
      r.rows.map(_.values.head) shouldBe List(Some("1"), Some("2"), Some("3"), Some("4"))
      val recorded = calls()
      recorded.map(_.method) shouldBe List("POST", "GET", "GET")
      recorded.map(_.path) shouldBe
        List("/v1/statement", "/v1/statement/q2/2", "/v1/statement/q2/3")
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
