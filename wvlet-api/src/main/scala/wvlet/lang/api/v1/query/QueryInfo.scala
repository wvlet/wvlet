package wvlet.lang.api.v1.query

import wvlet.airframe.ulid.ULID

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
case class QueryError(errorCode: String, message: String, error: Option[Throwable])
