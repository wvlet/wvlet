package wvlet.lang.api.v1.query

import wvlet.airframe.ulid.ULID
import wvlet.lang.api.{SourceLocation, StatusCode}

import java.time.Instant

case class QueryInfo(
    queryId: ULID,
    // For pagination
    pageToken: String,
    status: QueryStatus,
    statusCode: StatusCode,
    queryFragment: Option[String] = None,
    sql: Option[String] = None,
    createdAt: Instant,
    startedAt: Option[Instant] = None,
    completedAt: Option[Instant] = None,
    errors: List[QueryError] = Nil,
    // Partial query result
    result: Option[QueryResult] = None,
    // demo purpose only
    preview: Option[String] = None,
    limit: Option[Int] = None
)

case class QueryResult(
    schema: Seq[Column],
    rows: Seq[Seq[Any]],
    actualTotalRows: Option[Int] = None
):
  def totalRows: Int       = actualTotalRows.getOrElse(rows.size)
  def isTruncated: Boolean = actualTotalRows.exists(_ >= rows.size)

case class Column(name: String, typeName: String)

case class QueryError(
    statusCode: StatusCode,
    message: String,
    sourceLocation: SourceLocation,
    stackTrace: Option[Throwable]
)
