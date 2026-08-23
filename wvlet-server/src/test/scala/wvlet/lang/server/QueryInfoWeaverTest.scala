package wvlet.lang.server

import wvlet.uni.test.UniTest
import wvlet.uni.weaver.Weaver
import wvlet.lang.api.StatusCode
import wvlet.lang.api.v1.query.{QueryResult as ApiQueryResult, Column, QueryInfo, QueryStatus}
import wvlet.uni.util.ULID
import java.time.Instant

/**
  * Guards the Weaver round-trip of the structured query result: `QueryInfo.result.rows` is
  * `Seq[Seq[Any]]`, which must survive JSON encode/decode (via uni's AnyWeaver) for RPC clients to
  * receive rows (#1963 phase 4)
  */
class QueryInfoWeaverTest extends UniTest:
  test("weave ApiQueryResult with Any rows") {
    val r = ApiQueryResult(
      schema = Seq(Column("x", "int"), Column("s", "string")),
      rows = Seq(Seq(1, "hello"), Seq(2, null)),
      actualTotalRows = Some(2)
    )
    val codec = Weaver.of[ApiQueryResult]
    val back  = codec.fromJson(codec.toJson(r))
    back.rows.size shouldBe 2
    back.rows.head.map(v => Option(v).map(_.toString).orNull) shouldBe List("1", "hello")
    back.rows(1)(1) shouldBe null
  }

  test("weave QueryInfo with a structured result") {
    val q = QueryInfo(
      queryId = ULID.newULID,
      pageToken = "2",
      status = QueryStatus.FINISHED,
      statusCode = StatusCode.OK,
      createdAt = Instant.now(),
      result = Some(
        ApiQueryResult(
          schema = Seq(Column("x", "int")),
          rows = Seq(Seq(42)),
          actualTotalRows = Some(1)
        )
      )
    )
    val codec = Weaver.of[QueryInfo]
    val back  = codec.fromJson(codec.toJson(q))
    back.result.isDefined shouldBe true
    back.result.get.rows.head.head.toString shouldBe "42"
  }

end QueryInfoWeaverTest
