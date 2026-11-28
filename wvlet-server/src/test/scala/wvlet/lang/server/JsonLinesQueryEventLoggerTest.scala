package wvlet.lang.server

import wvlet.lang.api.StatusCode
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.uni.test.UniTest
import wvlet.uni.weaver.Weaver

import java.nio.file.Files
import java.time.Instant
import scala.jdk.CollectionConverters.*

class JsonLinesQueryEventLoggerTest extends UniTest:

  test("append one JSON line per event to the day's file") {
    val folder = Files.createTempDirectory("query-events").toFile
    val logger = JsonLinesQueryEventLogger(folder.getPath)
    val now    = Instant.now()
    val events = Seq(QueryEventType.CREATED, QueryEventType.COMPLETED).map { t =>
      QueryEvent(
        eventType = t,
        queryId = "20261127_155437_00042_x7k2p",
        requestId = "r1",
        sessionId = Some("s1"),
        profile = None,
        status = QueryStatus.FINISHED,
        statusCode = StatusCode.OK,
        createdAt = now,
        resultRows = Some(3)
      )
    }
    try events.foreach(logger.onEvent)
    finally logger.close()

    val lines = Files.readAllLines(logger.fileOf(Instant.now()).toPath).asScala.toList
    lines.size shouldBe 2
    val weaver = Weaver.of[QueryEvent]
    lines.map(l => weaver.fromJson(l)) shouldBe events
  }

end JsonLinesQueryEventLoggerTest
