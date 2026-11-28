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
package wvlet.lang.ui.server

import wvlet.lang.api.v1.query.QueryList
import wvlet.lang.api.v1.query.QueryListRequest
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.lang.api.v1.query.QuerySummary
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx
import wvlet.uni.ui.Button
import wvlet.uni.ui.ButtonVariant
import wvlet.uni.ui.Column
import wvlet.uni.ui.DataTable
import wvlet.uni.ui.Input
import wvlet.uni.ui.Select

/**
  * Every query the server keeps, newest first, refreshed every second: filter by state, text, and
  * session, and open a query by its row
  */
class QueryListPage(conn: ServerConnection) extends Polling:
  import Widgets.*

  private val statusFilter = Rx.variable(QueryListPage.AllStates)
  private val textFilter   = Rx.variable("")
  private val sessionFilter = Rx.variable("")
  private val limit         = Rx.variable(QueryListRequest.DefaultLimit)
  private val paused        = Rx.variable(false)
  private val result        = Rx.variable(Option.empty[QueryList])
  private val error         = Rx.variable(Option.empty[String])

  override protected def refresh(): Unit =
    if !paused.get then
      load()

  private def request: QueryListRequest = QueryListRequest(
    statuses = QueryListPage.statusesOf(statusFilter.get),
    sessionId = Some(sessionFilter.get.trim).filter(_.nonEmpty),
    text = Some(textFilter.get.trim).filter(_.nonEmpty),
    limit = limit.get
  )

  private def load(): Unit = conn
    .rpc
    .FrontendApi
    .listQueries(request)
    .map { l =>
      error  := None
      result := Some(l)
    }
    .recover { case e: Throwable =>
      error := Some(conn.errorMessage(e))
    }
    .run()

  private val columns: Seq[Column[QuerySummary]] = Seq(
    Column[QuerySummary](
      "id",
      "Query ID",
      cell = q => link(Page.Query(q.queryId), span(cls -> "wv-mono", q.queryId))
    ).sortBy(_.queryId).withWidth("230px"),
    Column[QuerySummary]("state", "State", cell = q => statusBadge(q.status))
      .sortBy(_.status.ordinal)
      .withWidth("96px"),
    Column[QuerySummary](
      "query",
      "Query",
      cell = q => span(cls -> "wv-query-head", titleAttr -> q.queryHead, q.queryHead)
    ).sortBy(_.queryHead),
    Column[QuerySummary]("session", "Session", cell = q => span(cls -> "wv-mono wv-muted wv-ellipsis", titleAttr -> q.sessionId.getOrElse(""), q.sessionId.getOrElse("-")))
      .sortBy(_.sessionId.getOrElse(""))
      .withWidth("150px"),
    Column[QuerySummary]("created", "Created", cell = q => span(titleAttr -> Fmt.dateTime(q.createdAt), Fmt.clock(q.createdAt)))
      .sortBy(_.createdAt.toEpochMilli)
      .withWidth("90px"),
    Column[QuerySummary]("queued", "Queued", cell = q => span(Fmt.duration(q.queuedMillis)))
      .sortBy(_.queuedMillis)
      .alignEnd
      .withWidth("86px"),
    Column[QuerySummary]("run", "Run", cell = q => span(q.runMillis.map(Fmt.duration).getOrElse("-")))
      .sortBy(_.runMillis.getOrElse(-1L))
      .alignEnd
      .withWidth("86px"),
    Column[QuerySummary]("rows", "Rows", cell = q => span(q.resultRows.map(n => Fmt.count(n)).getOrElse("-")))
      .sortBy(_.resultRows.getOrElse(-1))
      .alignEnd
      .withWidth("80px"),
    Column[QuerySummary]("memory", "Peak memory", cell = q => span(QueryListPage.memoryText(q)))
      .sortBy(_.memory.peakBytes)
      .alignEnd
      .withWidth("110px")
  )

  private def filters: RxElement = div(
    cls -> "wv-filters",
    Select(statusFilter, QueryListPage.stateOptions, identity[String])
      .withLabel("State")
      .onChange(_ => scala.scalajs.js.timers.setTimeout(0)(load())),
    div(
      cls -> "wv-filter-text",
      Input(textFilter).withPlaceholder("Search query text").withLabel("Search query text")
    ),
    div(
      cls -> "wv-filter-session",
      Input(sessionFilter).withPlaceholder("Session id").withLabel("Session id")
    ),
    paused.map { p =>
      Button(
        if p then
          "Resume"
        else
          "Pause"
      ).withVariant(ButtonVariant.Ghost)
        .withLabel(
          if p then
            "Resume live refresh"
          else
            "Pause live refresh"
        )
        .onClick(() => paused := !p)
    },
    paused.map(p =>
      span(
        cls -> (
          if p then
            "wv-live wv-live--off"
          else
            "wv-live"
        ),
        if p then
          "Paused"
        else
          "Live"
      )
    )
  )

  override def render: RxElement = div(
    cls -> "wv-page",
    div(cls -> "wv-page__head", h1("Queries")),
    filters,
    errorLine(error),
    result.map {
      case None =>
        div(cls -> "wv-empty", "Loading…")
      case Some(l) if l.queries.isEmpty =>
        div(cls -> "wv-empty", "No query matches.")
      case Some(l) =>
        div(
          div(
            cls -> "wv-muted wv-count",
            s"${l.queries.size} of ${Fmt.count(l.totalMatched)} queries"
          ),
          DataTable[QuerySummary, String](Rx.const(l.queries), key = _.queryId, columns = columns)
            .withLabel("Queries")
            .withRowLabel(q => s"${q.queryId} ${q.status}")
            .onSelect(q => Page.go(Page.Query(q.queryId))),
          if l.nextPageToken.nonEmpty && limit.get < QueryListRequest.MaxLimit then
            div(
              cls -> "wv-more",
              Button("Show more")
                .onClick { () =>
                  limit := (limit.get + QueryListRequest.DefaultLimit).min(QueryListRequest.MaxLimit)
                  load()
                }
            )
          else
            span()
        )
    }
  )

end QueryListPage

object QueryListPage:
  val AllStates: String = "All states"

  val stateOptions: Seq[String] =
    AllStates +: Seq(
      QueryStatus.RUNNING,
      QueryStatus.QUEUED,
      QueryStatus.FINISHED,
      QueryStatus.FAILED,
      QueryStatus.CANCELED
    ).map(_.toString.toLowerCase.capitalize)

  /** The statuses a state option selects; empty for all */
  def statusesOf(option: String): Seq[QueryStatus] =
    QueryStatus.values.find(_.toString.equalsIgnoreCase(option)).toSeq

  /** The peak memory of a query, or "-" when the server does not count it */
  def memoryText(q: QuerySummary): String =
    if !q.memory.tracked then
      "-"
    else if q.status.isFinished then
      Fmt.bytes(q.memory.peakBytes)
    else
      s"${Fmt.bytes(q.memory.reservedBytes)} (peak ${Fmt.bytes(q.memory.peakBytes)})"
