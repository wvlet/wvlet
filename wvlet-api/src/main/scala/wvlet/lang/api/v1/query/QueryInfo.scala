package wvlet.lang.api.v1.query

import wvlet.airframe.ulid.ULID
import wvlet.lang.api.{SourceLocation, StatusCode}

import java.time.Instant

case class QueryInfo(
    queryId: ULID,
    // For pagination
    pageToken: String,
    status: QueryStatus,
    createdAt: Instant = Instant.now(),
    startedAt: Option[Instant] = None,
    completedAt: Option[Instant] = None,
    error: Option[QueryError] = None,
    // Partial query result
    result: Option[QueryResult] = None,
    // demo purpose only
    preview: Option[String] = None,
    limit: Option[Int] = None
)

case class QueryResult(schema: Seq[Column], rows: Seq[Seq[Any]])

case class Column(name: String, typeName: String)
case class QueryError(
    // More detailed errors at different locations, if exists
    errorReports: Seq[ErrorReport]
):
  require(errorReports.nonEmpty, "errorReports must not be empty")
  def message: String        = errorReports.head.message
  def statusCode: StatusCode = errorReports.head.statusCode

case class ErrorReport(
    statusCode: StatusCode,
    message: String,
    sourceLocation: SourceLocation,
    error: Option[Throwable]
)
