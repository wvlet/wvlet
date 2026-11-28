/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.api.v1.query

import java.time.Instant

/**
  * The memory a query's in-process engine statements hold.
  *
  * @param tracked
  *   Whether any statement counted its memory: false for queries routed to DuckDB or Trino, and
  *   with `memory_accounting` off, where the byte counts are 0 because nothing was counted
  * @param reservedBytes
  *   Bytes held now (0 once the query finishes)
  * @param peakBytes
  *   The largest number of bytes one of the query's engine statements held
  */
case class QueryMemory(tracked: Boolean = false, reservedBytes: Long = 0L, peakBytes: Long = 0L)

/**
  * One query of the server's query list.
  *
  * @param queryHead
  *   The first characters of the query text (see [[QuerySummary.HeadLength]])
  * @param queuedMillis
  *   Time spent waiting for a slot: up to now for a queued query
  * @param runMillis
  *   Time since start: up to now for a running query
  * @param resultRows
  *   Rows of the final tabular result of a finished query
  * @param errorMessage
  *   The first error of a failed query
  */
case class QuerySummary(
    queryId: String,
    status: QueryStatus,
    sessionId: Option[String] = None,
    profile: Option[String] = None,
    queryHead: String = "",
    createdAt: Instant,
    startedAt: Option[Instant] = None,
    completedAt: Option[Instant] = None,
    queuedMillis: Long = 0L,
    runMillis: Option[Long] = None,
    resultRows: Option[Int] = None,
    errorMessage: Option[String] = None,
    memory: QueryMemory = QueryMemory()
)

object QuerySummary:
  /** The characters of the query text a summary carries */
  val HeadLength: Int = 200

/**
  * Which queries [[wvlet.lang.api.v1.frontend.FrontendApi.listQueries]] returns. Empty filters
  * match every query.
  *
  * @param statuses
  *   Match any of these statuses
  * @param createdAfter
  *   Match queries created at or after this time
  * @param createdBefore
  *   Match queries created before this time
  * @param text
  *   Match queries whose text contains this string, ignoring case
  * @param pageToken
  *   The `nextPageToken` of the previous page, to continue after it
  * @param limit
  *   The page size, at most [[QueryListRequest.MaxLimit]]
  */
case class QueryListRequest(
    statuses: Seq[QueryStatus] = Nil,
    sessionId: Option[String] = None,
    profile: Option[String] = None,
    createdAfter: Option[Instant] = None,
    createdBefore: Option[Instant] = None,
    text: Option[String] = None,
    pageToken: Option[String] = None,
    limit: Int = QueryListRequest.DefaultLimit
)

object QueryListRequest:
  val DefaultLimit: Int = 100
  val MaxLimit: Int     = 1000

/** How many queries have one value of a facet, such as a status or a session id */
case class FacetCount(value: String, count: Int)

/**
  * Query counts by status, session, and profile, for filter chips. Each facet counts the queries
  * matching every filter except its own dimension, so its chips show what selecting them would
  * give.
  *
  * @param sessions
  *   The sessions with the most queries, at most [[QueryFacets.MaxValues]]
  * @param profiles
  *   The profiles with the most queries, at most [[QueryFacets.MaxValues]]
  */
case class QueryFacets(
    statuses: Seq[FacetCount] = Nil,
    sessions: Seq[FacetCount] = Nil,
    profiles: Seq[FacetCount] = Nil
)

object QueryFacets:
  val MaxValues: Int = 8

/**
  * A page of the query list, newest first.
  *
  * @param totalMatched
  *   Queries matching the filter across all pages
  * @param nextPageToken
  *   Pass as `pageToken` to read the next (older) page; None on the last page
  * @param facets
  *   Counts by status, session, and profile
  */
case class QueryList(
    queries: Seq[QuerySummary],
    totalMatched: Int,
    nextPageToken: Option[String] = None,
    facets: QueryFacets = QueryFacets()
)

/** Which query [[wvlet.lang.api.v1.frontend.FrontendApi.getQueryDetail]] reads */
case class QueryDetailRequest(queryId: String)

/**
  * Everything the server keeps about one query, except its result rows (read them with
  * `getQueryInfo`).
  *
  * @param query
  *   The full query text
  * @param sql
  *   SQL statements the query sent to DuckDB, Trino, or other SQL engines, in order
  * @param enginePlans
  *   The physical plan of each statement the in-process engine ran, in order, drawn source-first
  *   (`Operator.explain`)
  */
case class QueryDetail(
    summary: QuerySummary,
    requestId: String,
    query: String,
    sql: Seq[String] = Nil,
    enginePlans: Seq[String] = Nil,
    errors: List[QueryError] = Nil
)
