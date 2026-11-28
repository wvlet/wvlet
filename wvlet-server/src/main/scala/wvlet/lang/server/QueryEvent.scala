package wvlet.lang.server

import wvlet.lang.api.StatusCode
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.uni.log.LogSupport
import wvlet.uni.weaver.Weaver

import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import scala.util.control.NonFatal

/** The lifecycle point a [[QueryEvent]] reports */
enum QueryEventType:
  case CREATED
  case STARTED
  case COMPLETED

/**
  * A query lifecycle event. Every query gets one CREATED and one COMPLETED event; STARTED is
  * skipped for a query cancelled while queued.
  *
  * @param query
  *   The query text, on CREATED only
  * @param queuedMillis
  *   Time from creation to start (or to cancellation of a queued query)
  * @param runMillis
  *   Time from start to completion
  * @param resultRows
  *   Rows of the final tabular result, on a FINISHED query that returns one
  */
case class QueryEvent(
    eventType: QueryEventType,
    queryId: String,
    requestId: String,
    sessionId: Option[String],
    profile: Option[String],
    status: QueryStatus,
    statusCode: StatusCode,
    createdAt: Instant,
    startedAt: Option[Instant] = None,
    completedAt: Option[Instant] = None,
    queuedMillis: Option[Long] = None,
    runMillis: Option[Long] = None,
    resultRows: Option[Int] = None,
    errorMessage: Option[String] = None,
    query: Option[String] = None
)

/**
  * Receives query lifecycle events from [[QueryService]]. Calls come from request and worker
  * threads, so implementations must be thread-safe. A listener failure is logged and never fails
  * the query.
  */
trait QueryEventListener extends AutoCloseable:
  def onEvent(event: QueryEvent): Unit
  override def close(): Unit = ()

object QueryEventListener:
  /** Drops every event */
  val noOp: QueryEventListener = (_: QueryEvent) => ()

/**
  * Appends each event as one JSON line to `<folder>/query-events-yyyy-MM-dd.jsonl`, starting a new
  * file per UTC day.
  */
class JsonLinesQueryEventLogger(folder: String) extends QueryEventListener with LogSupport:
  private val weaver  = Weaver.of[QueryEvent]
  private val dayFmt  = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC)
  private var day     = ""
  private var current = Option.empty[BufferedWriter]
  private var closed  = false

  /** The file today's events go to */
  def fileOf(at: Instant): File = File(folder, s"query-events-${dayFmt.format(at)}.jsonl")

  override def onEvent(event: QueryEvent): Unit =
    debug(
      s"Query ${event.queryId} ${event.eventType}: ${event.status}" +
        event.errorMessage.map(m => s" (${m})").getOrElse("")
    )
    val line = weaver.toJson(event)
    synchronized {
      // Events of queries still finishing at shutdown are dropped, not written to a reopened file
      if !closed then
        val w = writerFor(Instant.now())
        w.write(line)
        w.newLine()
        w.flush()
    }

  private def writerFor(now: Instant): BufferedWriter =
    val today = dayFmt.format(now)
    current match
      case Some(w) if today == day =>
        w
      case _ =>
        current.foreach(closeQuietly)
        val f = fileOf(now)
        f.getParentFile.mkdirs()
        val w = BufferedWriter(
          OutputStreamWriter(FileOutputStream(f, true), StandardCharsets.UTF_8)
        )
        day = today
        current = Some(w)
        w

  private def closeQuietly(w: BufferedWriter): Unit =
    try
      w.close()
    catch
      case NonFatal(e) =>
        warn(s"Failed to close the query event log: ${e.getMessage}")

  override def close(): Unit = synchronized {
    closed = true
    current.foreach(closeQuietly)
    current = None
  }

end JsonLinesQueryEventLogger
