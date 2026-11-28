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

import wvlet.lang.api.v1.frontend.FrontendApi.QueryCancelRequest
import wvlet.lang.api.v1.query.QueryDetail
import wvlet.lang.api.v1.query.QueryDetailRequest
import wvlet.lang.api.v1.query.QueryError
import wvlet.lang.api.v1.query.QuerySummary
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx
import wvlet.uni.ui.Button
import wvlet.uni.ui.ButtonVariant
import wvlet.uni.ui.Tab
import wvlet.uni.ui.TabList
import wvlet.uni.ui.TabPanel

import java.time.Instant

/**
  * One query: its state and timeline, memory, errors, text, engine plans, and generated SQL, with
  * Cancel while it is queued or running. Refreshes every second until the query finishes
  */
class QueryDetailPage(conn: ServerConnection, queryId: String) extends Polling:
  import Widgets.*

  private val detail     = Rx.variable(Option.empty[QueryDetail])
  private val error      = Rx.variable(Option.empty[String])
  private val canceling  = Rx.variable(false)
  private val tab        = Rx.variable("query")

  override protected def refresh(): Unit =
    if !detail.get.exists(_.summary.status.isFinished) then
      conn
        .rpc
        .FrontendApi
        .getQueryDetail(QueryDetailRequest(queryId))
        .map { d =>
          error  := None
          detail := Some(d)
        }
        .recover { case e: Throwable =>
          error := Some(conn.errorMessage(e))
        }
        .run()

  private def cancel(): Unit =
    canceling := true
    conn
      .rpc
      .FrontendApi
      .cancelQuery(QueryCancelRequest(queryId))
      .map { _ =>
        canceling := false
        detail    := None
        refresh()
      }
      .recover { case e: Throwable =>
        canceling := false
        error     := Some(conn.errorMessage(e))
      }
      .run()

  private def field(name: String, value: DomNode): RxElement = div(
    cls -> "wv-field",
    div(cls -> "wv-field__name", name),
    div(cls -> "wv-field__value", value)
  )

  private def timeOf(t: Option[Instant]): String = t.map(Fmt.dateTime).getOrElse("-")

  /** Queued and running time as two segments of one bar */
  private def timeline(s: QuerySummary): RxElement =
    val run   = s.runMillis.getOrElse(0L)
    val total = (s.queuedMillis + run).max(1L)
    val qPct  = (s.queuedMillis * 100.0 / total).max(if s.queuedMillis > 0 then 1.0 else 0.0)
    div(
      cls       -> "wv-timeline",
      role      -> "img",
      ariaLabel -> s"Queued ${Fmt.duration(s.queuedMillis)}, ran ${Fmt.duration(run)}",
      div(cls -> "wv-timeline__queued", styleAttr -> f"width: ${qPct}%.1f%%"),
      div(cls -> "wv-timeline__run", styleAttr -> f"width: ${100 - qPct}%.1f%%")
    )

  private def memory(s: QuerySummary): String =
    if !s.memory.tracked then
      "Not tracked"
    else if s.status.isFinished then
      s"Peak ${Fmt.bytes(s.memory.peakBytes)}"
    else
      s"${Fmt.bytes(s.memory.reservedBytes)} now · peak ${Fmt.bytes(s.memory.peakBytes)}"

  private def errorBox(e: QueryError): RxElement = div(
    cls  -> "wv-error-box",
    role -> "alert",
    div(cls -> "wv-error-box__code", e.statusCode.toString),
    pre(cls -> "wv-pre", e.message),
    Option(e.sourceLocation)
      .map(_.locationString)
      .filter(_.nonEmpty)
      .map(l => div(cls -> "wv-muted", s"at ${l}"))
      .getOrElse(span())
  )

  private def codeBlocks(items: Seq[String], empty: String): RxElement =
    if items.isEmpty then
      div(cls -> "wv-empty", empty)
    else
      div(
        items
          .zipWithIndex
          .map { (text, i) =>
            div(
              cls -> "wv-code",
              if items.size > 1 then
                div(cls -> "wv-code__label", s"Statement ${i + 1}")
              else
                span()
              ,
              pre(cls -> "wv-pre", text)
            )
          }
      )

  private def body(d: QueryDetail): RxElement =
    val s    = d.summary
    val tabs = TabList[String](
      tab,
      "Query parts",
      Seq(
        Tab("query", "Query"),
        Tab("plan", s"Engine plan (${d.enginePlans.size})"),
        Tab("sql", s"SQL (${d.sql.size})")
      )
    )
    div(
      div(
        cls -> "wv-page__head",
        link(Page.Queries, span(cls -> "wv-muted", "← Queries")),
        h1(cls -> "wv-mono", s.queryId),
        statusBadge(s.status),
        if !s.status.isFinished then
          canceling.map(c =>
            Button(
              if c then
                "Canceling…"
              else
                "Cancel query"
            ).withVariant(ButtonVariant.Danger).withDisabled(c).onClick(() => cancel())
          )
        else
          span()
      ),
      timeline(s),
      div(
        cls -> "wv-fields",
        field("Created", timeOf(Some(s.createdAt))),
        field("Started", timeOf(s.startedAt)),
        field("Completed", timeOf(s.completedAt)),
        field("Queued", Fmt.duration(s.queuedMillis)),
        field("Run time", s.runMillis.map(Fmt.duration).getOrElse("-")),
        field("Rows", s.resultRows.map(n => Fmt.count(n)).getOrElse("-")),
        field("Memory", memory(s)),
        field("Session", span(cls -> "wv-mono", s.sessionId.getOrElse("default"))),
        field("Profile", s.profile.getOrElse("server default")),
        field("Request id", span(cls -> "wv-mono", d.requestId))
      ),
      if d.errors.nonEmpty then
        div(cls -> "wv-section", d.errors.map(errorBox))
      else
        span()
      ,
      div(
        cls -> "wv-section",
        tabs,
        TabPanel[String](
          tabs,
          {
            case "plan" =>
              codeBlocks(d.enginePlans, "No statement ran on the wvlet engine.")
            case "sql" =>
              codeBlocks(d.sql, "No SQL was sent to another engine.")
            case _ =>
              codeBlocks(Seq(d.query), "")
          }
        )
      )
    )

  end body

  override def render: RxElement = div(
    cls -> "wv-page",
    errorLine(error),
    detail.map {
      case None =>
        div(cls -> "wv-empty", s"Loading ${queryId}…")
      case Some(d) =>
        body(d)
    }
  )

end QueryDetailPage
