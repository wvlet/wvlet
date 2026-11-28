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

import wvlet.lang.api.v1.frontend.ClusterInfo
import wvlet.lang.api.v1.frontend.FrontendApi.QueryCancelRequest
import wvlet.lang.api.v1.query.QueryDetail
import wvlet.lang.api.v1.query.QueryDetailRequest
import wvlet.lang.api.v1.query.QueryList
import wvlet.lang.api.v1.query.QueryListRequest
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.lang.api.v1.query.QuerySummary
import wvlet.uni.rx.Rx
import wvlet.uni.rx.RxVar

/** How the console shows the query list */
enum ListView:
  case Rows
  case Timeline

/**
  * The state of the workload console: the server's health history, the filtered query list, the
  * selected and checked queries, and the inspector's detail. [[refresh]] reloads it once; the
  * console calls it every second while live.
  */
class ConsoleState(val conn: ServerConnection):
  val cluster: RxVar[Option[ClusterInfo]] = Rx.variable(None)
  val history: RxVar[Vector[ClusterInfo]] = Rx.variable(Vector.empty)
  val list: RxVar[Option[QueryList]]      = Rx.variable(None)
  val error: RxVar[Option[String]]        = Rx.variable(None)
  val statuses: RxVar[Set[QueryStatus]]   = Rx.variable(Set.empty)
  val session: RxVar[Option[String]]      = Rx.variable(None)
  val profile: RxVar[Option[String]]      = Rx.variable(None)
  val text: RxVar[String]                 = Rx.variable("")
  val paused: RxVar[Boolean]              = Rx.variable(false)
  val view: RxVar[ListView]               = Rx.variable(ListView.Rows)
  val selected: RxVar[Option[String]]     = Rx.variable(None)
  val checked: RxVar[Set[String]]         = Rx.variable(Set.empty)
  val detail: RxVar[Option[QueryDetail]]  = Rx.variable(None)
  val detailError: RxVar[Option[String]]  = Rx.variable(None)
  val canceling: RxVar[Set[String]]       = Rx.variable(Set.empty)
  val notice: RxVar[Option[String]]       = Rx.variable(None)
  // Numbers list requests, so a slow response never replaces a newer one
  private var listSeq: Long = 0L

  /** The queries shown now, newest first */
  def queries: Seq[QuerySummary] = list.get.map(_.queries).getOrElse(Nil)

  private def request: QueryListRequest = QueryListRequest(
    statuses = statuses.get.toSeq.sortBy(_.ordinal),
    sessionId = session.get,
    profile = profile.get,
    text = Some(text.get.trim).filter(_.nonEmpty),
    limit = ConsoleState.ListLimit
  )

  /** Reload the health, the list, and the selected query's detail */
  def refresh(): Unit =
    conn
      .rpc
      .FrontendApi
      .getClusterInfo
      .map { c =>
        error   := None
        cluster := Some(c)
        history := (history.get :+ c).takeRight(ConsoleState.HistorySize)
      }
      .recover { case e: Throwable =>
        error := Some(conn.errorMessage(e))
      }
      .run()
    reloadList()
    reloadDetail()

  /** Whether any filter narrows the list */
  def hasFilters: Boolean =
    statuses.get.nonEmpty || session.get.nonEmpty || profile.get.nonEmpty || text.get.trim.nonEmpty

  /** Reload the list now, e.g. after a filter changed */
  def reloadList(): Unit =
    listSeq += 1
    val seq = listSeq
    conn
      .rpc
      .FrontendApi
      .listQueries(request)
      .map { l =>
        if seq == listSeq then
          list := Some(l)
          // Forget checks of queries no longer in view
          val ids = l.queries.iterator.map(_.queryId).toSet
          if !checked.get.subsetOf(ids) then
            checked := checked.get.intersect(ids)
      }
      .recover { case e: Throwable =>
        error := Some(conn.errorMessage(e))
      }
      .run()

  private def reloadDetail(): Unit = selected
    .get
    .foreach { id =>
      // A finished query no longer changes
      val done = detail.get.exists(d => d.summary.queryId == id && d.summary.status.isFinished)
      if !done then
        conn
          .rpc
          .FrontendApi
          .getQueryDetail(QueryDetailRequest(id))
          .map { d =>
            if selected.get.contains(id) then
              detailError := None
              detail      := Some(d)
          }
          .recover { case e: Throwable =>
            if selected.get.contains(id) then
              detailError := Some(conn.errorMessage(e))
          }
          .run()
    }

  /** Open `id` in the inspector, or close the inspector with None */
  def select(id: Option[String]): Unit =
    if selected.get != id then
      selected    := id
      detail      := None
      detailError := None
      reloadDetail()
      Page.go(Page.Console(id))

  /** Move the selection `step` rows down (negative: up) in the shown order */
  def moveSelection(step: Int): Unit =
    val qs = queries
    if qs.nonEmpty then
      val at   = selected.get.map(id => qs.indexWhere(_.queryId == id)).getOrElse(-1)
      val next =
        if at < 0 then
          if step > 0 then
            0
          else
            qs.size - 1
        else
          (at + step).max(0).min(qs.size - 1)
      select(Some(qs(next).queryId))

  def toggleCheck(id: String): Unit =
    val c = checked.get
    checked := (
      if c.contains(id) then
        c - id
      else
        c + id
    )

  def toggleStatus(s: QueryStatus): Unit =
    val c = statuses.get
    statuses := (
      if c.contains(s) then
        c - s
      else
        c + s
    )
    reloadList()

  /** Show only queries in `s`, or every query when `s` is the only status shown already */
  def onlyStatus(s: QueryStatus): Unit =
    statuses := (
      if statuses.get == Set(s) then
        Set.empty
      else
        Set(s)
    )
    reloadList()

  def filterSession(s: Option[String]): Unit =
    session := s
    reloadList()

  def filterProfile(p: Option[String]): Unit =
    profile := p
    reloadList()

  def clearFilters(): Unit =
    statuses := Set.empty
    session  := None
    profile  := None
    text     := ""
    reloadList()

  /** Cancel the queries `ids`, then refresh once all the server's answers are in */
  def cancel(ids: Seq[String]): Unit =
    var pending      = ids.size
    def done(): Unit =
      pending -= 1
      if pending == 0 then
        refresh()
    ids.foreach(cancelOne(_, () => done()))

  private def cancelOne(id: String, done: () => Unit): Unit =
    canceling := canceling.get + id
    conn
      .rpc
      .FrontendApi
      .cancelQuery(QueryCancelRequest(id))
      .map { _ =>
        canceling := canceling.get - id
        checked   := checked.get - id
        if selected.get.contains(id) then
          detail := None
        done()
      }
      .recover { case e: Throwable =>
        canceling := canceling.get - id
        notice    := Some(s"Could not cancel ${id}: ${conn.errorMessage(e)}")
        done()
      }
      .run()

end ConsoleState

object ConsoleState:
  /** Samples of the health strip's lines: two minutes at one poll per second */
  val HistorySize: Int = 120

  /** Rows the console loads per poll; the search and chips narrow down the rest */
  val ListLimit: Int = 200
