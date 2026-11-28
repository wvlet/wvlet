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
import wvlet.lang.api.v1.query.QueryListRequest
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.lang.api.v1.query.QuerySummary
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx

import java.time.Instant

/**
  * The landing page: query counts against the limits, the engine memory pool, thread load, and the
  * heap, each with a line of its last two minutes, plus the running and queued queries
  */
class OverviewPage(conn: ServerConnection) extends Polling:
  import Widgets.*

  private val info    = Rx.variable(Option.empty[ClusterInfo])
  private val active  = Rx.variable(Seq.empty[QuerySummary])
  private val recent  = Rx.variable(Seq.empty[QuerySummary])
  private val error   = Rx.variable(Option.empty[String])
  private val history = Rx.variable(Vector.empty[ClusterInfo])

  override protected def refresh(): Unit =
    conn
      .rpc
      .FrontendApi
      .getClusterInfo
      .map { c =>
        error   := None
        info    := Some(c)
        history := (history.get :+ c).takeRight(OverviewPage.HistorySize)
      }
      .recover { case e: Throwable =>
        error := Some(conn.errorMessage(e))
      }
      .run()
    conn
      .rpc
      .FrontendApi
      .listQueries(
        QueryListRequest(statuses = Seq(QueryStatus.RUNNING, QueryStatus.QUEUED), limit = 20)
      )
      .map(l => active := l.queries)
      .recover { case _ => () }
      .run()
    conn
      .rpc
      .FrontendApi
      .listQueries(
        QueryListRequest(
          statuses = Seq(QueryStatus.FINISHED, QueryStatus.FAILED, QueryStatus.CANCELED),
          limit = 8
        )
      )
      .map(l => recent := l.queries)
      .recover { case _ => () }
      .run()

  private def card(title: String, value: DomNode, sub: DomNode, extra: DomNode*): RxElement = div(
    cls -> "wv-card",
    div(cls -> "wv-card__title", title),
    div(cls -> "wv-card__value", value),
    div(cls -> "wv-card__sub", sub),
    extra
  )

  private def series(f: ClusterInfo => Double): Seq[Double] = history.get.map(f)

  private def cards(c: ClusterInfo): RxElement =
    val q    = c.queries
    val pool = c.memoryPool
    val heap = c.heap
    val eng  = c.engineThreads
    div(
      cls -> "wv-cards",
      card(
        "Running queries",
        s"${q.running}",
        s"of ${q.maxRunning} slots",
        meter(Fmt.percent(q.running, q.maxRunning), "Running query slots used"),
        sparkline(series(_.queries.running.toDouble), q.maxRunning, "Running queries, last 2 minutes")
      ),
      card(
        "Queued queries",
        s"${q.queued}",
        s"limit ${Fmt.count(q.maxQueued)}",
        meter(Fmt.percent(q.queued, q.maxQueued), "Queue used"),
        sparkline(series(_.queries.queued.toDouble), label = "Queued queries, last 2 minutes")
      ),
      card(
        "Engine memory",
        Fmt.bytes(pool.reservedBytes),
        pool
          .capacityBytes
          .map(cap => s"of ${Fmt.bytes(cap)} pool · ${pool.openQueries} open")
          .getOrElse(s"no pool limit · ${pool.openQueries} open"),
        pool
          .capacityBytes
          .map(cap => meter(Fmt.percent(pool.reservedBytes, cap), "Memory pool used"))
          .getOrElse(span()),
        sparkline(
          series(_.memoryPool.reservedBytes.toDouble),
          pool.capacityBytes.map(_.toDouble).getOrElse(0.0),
          "Engine memory, last 2 minutes"
        )
      ),
      card(
        "Low-memory killer",
        s"${pool.killedQueries}",
        pool
          .lowMemoryKillerDelayMillis
          .map(d => s"queries killed · after ${Fmt.duration(d)} over the pool")
          .getOrElse("queries killed · killer off"),
        pool
          .queryLimitBytes
          .map(l => div(cls -> "wv-card__note", s"Per-query limit ${Fmt.bytes(l)}"))
          .getOrElse(div(cls -> "wv-card__note", "No per-query limit"))
      ),
      card(
        "Engine threads",
        s"${eng.busy} busy",
        s"of ${eng.threads} · ${Fmt.count(eng.waitingTasks)} tasks waiting",
        meter(Fmt.percent(eng.busy, eng.threads), "Engine threads busy"),
        sparkline(series(_.engineThreads.busy.toDouble), eng.threads, "Busy engine threads, last 2 minutes")
      ),
      card(
        "JVM heap",
        Fmt.bytes(heap.usedBytes),
        s"of ${Fmt.bytes(heap.maxBytes)} max · ${Fmt.bytes(heap.committedBytes)} committed",
        meter(Fmt.percent(heap.usedBytes, heap.maxBytes), "Heap used"),
        sparkline(series(_.heap.usedBytes.toDouble), heap.maxBytes.toDouble, "Heap used, last 2 minutes")
      )
    )

  end cards

  private def queryRows(qs: Seq[QuerySummary], empty: String): RxElement =
    if qs.isEmpty then
      div(cls -> "wv-empty", empty)
    else
      val now = Instant.now()
      table(
        cls -> "wv-table",
        thead(tr(th("Query ID"), th("State"), th("Query"), th("Time"), th("Memory"))),
        tbody(
          qs.map { q =>
            tr(
              td(link(Page.Query(q.queryId), span(cls -> "wv-mono", q.queryId))),
              td(statusBadge(q.status)),
              td(cls -> "wv-query-head", titleAttr -> q.queryHead, q.queryHead),
              td(
                cls -> "wv-num",
                q.runMillis
                  .map(Fmt.duration)
                  .getOrElse(s"queued ${Fmt.duration(q.queuedMillis)}")
              ),
              td(cls -> "wv-num", QueryListPage.memoryText(q))
            )
          }
        )
      )

  override def render: RxElement = div(
    cls -> "wv-page",
    errorLine(error),
    info.map {
      case None =>
        div(cls -> "wv-empty", "Connecting to the server…")
      case Some(c) =>
        div(
          div(
            cls -> "wv-page__head",
            h1("Overview"),
            span(
              cls -> "wv-muted",
              s"wvlet ${c.version} · server ${c.serverId} · up ${Fmt.duration(c.upTimeMillis)} · ${c.processors} CPUs"
            )
          ),
          cards(c)
        )
    },
    div(
      cls -> "wv-section",
      div(
        cls -> "wv-section__head",
        h2("Running and queued"),
        link(Page.Queries, "All queries →")
      ),
      active.map(qs => queryRows(qs, "No query is running."))
    ),
    div(
      cls -> "wv-section",
      div(cls -> "wv-section__head", h2("Recently finished")),
      recent.map(qs => queryRows(qs, "No query has finished yet."))
    )
  )

end OverviewPage

object OverviewPage:
  /** Samples the overview's lines keep: two minutes at one poll per second */
  val HistorySize: Int = 120
