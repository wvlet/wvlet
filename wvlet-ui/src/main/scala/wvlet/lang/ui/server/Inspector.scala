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

import org.scalajs.dom
import wvlet.lang.api.v1.query.QueryDetail
import wvlet.lang.api.v1.query.QueryError
import wvlet.lang.api.v1.query.QuerySummary
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx

import java.time.Instant

/**
  * The selected query beside the list: state and timing, memory, errors, and its text, engine
  * plans, and generated SQL, with Cancel and Open in editor. Follows the selection live
  */
class Inspector(state: ConsoleState, openInEditor: String => Unit) extends RxElement:
  private val tab = Rx.variable("query")

  private def fact(name: String, value: DomNode): RxElement = div(
    cls -> "wv-fact",
    div(cls -> "wv-fact__name", name),
    div(cls -> "wv-fact__value", value)
  )

  private def timeOf(t: Option[Instant]): String = t.map(Fmt.clock).getOrElse("-")

  private def memory(s: QuerySummary): String =
    if !s.memory.tracked then
      "Not tracked"
    else if s.status.isFinished then
      s"peak ${Fmt.bytes(s.memory.peakBytes)}"
    else
      s"${Fmt.bytes(s.memory.reservedBytes)} · peak ${Fmt.bytes(s.memory.peakBytes)}"

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

  private def code(items: Seq[String], empty: String, wrap: Boolean = true): RxElement =
    if items.isEmpty then
      div(cls -> "wv-empty wv-empty--small", empty)
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
              pre(
                cls -> (
                  if wrap then
                    "wv-pre"
                  else
                    "wv-pre wv-pre--nowrap"
                ),
                text
              )
            )
          }
      )

  private def tabs(d: QueryDetail): RxElement = tab.map { t =>
    val options = Seq(
      "query" -> "Query",
      "plan"  ->
        s"Engine plan${
            if d.enginePlans.nonEmpty then
              s" ${d.enginePlans.size}"
            else
              ""
          }",
      "sql" ->
        s"SQL${
            if d.sql.nonEmpty then
              s" ${d.sql.size}"
            else
              ""
          }"
    )
    div(
      div(
        cls       -> "wv-tabs",
        role      -> "tablist",
        ariaLabel -> "Query parts",
        options.map { (key, name) =>
          button(
            tpe              -> "button",
            role             -> "tab",
            cls              -> "wv-tabs__tab",
            aria("selected") -> (key == t).toString,
            onclick          -> { (_: dom.MouseEvent) =>
              tab := key
            },
            name
          )
        }
      ),
      div(
        role -> "tabpanel",
        t match
          case "plan" =>
            code(d.enginePlans, "No statement ran on the wvlet engine.", wrap = false)
          case "sql" =>
            code(d.sql, "No SQL was sent to another engine.")
          case _ =>
            code(Seq(d.query), "")
      )
    )
  }

  private def body(d: QueryDetail): RxElement =
    val s      = d.summary
    val status = s.status.toString.toLowerCase
    div(
      cls -> "wv-inspector__body",
      div(
        cls -> "wv-inspector__head",
        span(cls -> s"wv-dot wv-dot--${status}"),
        span(cls -> s"wv-status wv-status--${status}", status),
        span(cls -> "wv-mono wv-inspector__id", titleAttr -> s.queryId, s.queryId),
        div(cls  -> "wv-grow"),
        button(
          tpe       -> "button",
          cls       -> "wv-icon-button",
          titleAttr -> "Close ( Esc )",
          ariaLabel -> "Close the inspector",
          onclick   -> { (_: dom.MouseEvent) =>
            state.select(None)
          },
          "×"
        )
      ),
      div(
        cls -> "wv-inspector__actions",
        if !s.status.isFinished then
          state
            .canceling
            .map { c =>
              val busy = c.contains(s.queryId)
              button(
                tpe       -> "button",
                cls       -> "wv-button wv-button--danger",
                disabled  -> busy,
                titleAttr -> "Cancel ( c c )",
                onclick   -> { (_: dom.MouseEvent) =>
                  state.cancel(Seq(s.queryId))
                },
                if busy then
                  "Canceling…"
                else
                  "Cancel"
              )
            }
        else
          span()
        ,
        button(
          tpe       -> "button",
          cls       -> "wv-button",
          titleAttr -> "Open in editor ( Enter )",
          onclick   -> { (_: dom.MouseEvent) =>
            openInEditor(s.queryId)
          },
          "Open in editor"
        )
      ),
      div(
        cls -> "wv-facts",
        fact("Created", span(titleAttr -> Fmt.dateTime(s.createdAt), Fmt.clock(s.createdAt))),
        fact("Started", timeOf(s.startedAt)),
        fact("Completed", timeOf(s.completedAt)),
        fact("Queued", Fmt.duration(s.queuedMillis)),
        fact("Run", s.runMillis.map(Fmt.duration).getOrElse("-")),
        fact("Rows", s.resultRows.map(n => Fmt.count(n)).getOrElse("-")),
        fact("Memory", memory(s)),
        fact(
          "Session",
          span(cls -> "wv-mono", s.sessionId.getOrElse(ConsoleLayout.DefaultSession))
        ),
        fact("Profile", s.profile.getOrElse("server default"))
      ),
      if d.errors.nonEmpty then
        div(cls -> "wv-section", d.errors.map(errorBox))
      else
        span()
      ,
      tabs(d)
    )

  end body

  override def render: RxElement = Rx
    .join(state.selected, state.detail, state.detailError)
    .map {
      case (None, _, _) =>
        span()
      case (Some(id), Some(d), _) if d.summary.queryId == id =>
        aside(cls -> "wv-inspector", ariaLabel -> "Query inspector", body(d))
      case (Some(id), _, Some(err)) =>
        aside(
          cls       -> "wv-inspector",
          ariaLabel -> "Query inspector",
          div(
            cls -> "wv-inspector__body",
            div(
              cls -> "wv-inspector__head",
              span(cls -> "wv-mono", id),
              div(cls  -> "wv-grow"),
              button(
                tpe       -> "button",
                cls       -> "wv-icon-button",
                ariaLabel -> "Close the inspector",
                onclick   -> { (_: dom.MouseEvent) =>
                  state.select(None)
                },
                "×"
              )
            ),
            div(
              cls  -> "wv-error",
              role -> "alert",
              if err.contains("not found") || err.contains("NOT_FOUND") then
                "The server no longer keeps this query."
              else
                err
            )
          )
        )
      case (Some(id), _, _) =>
        aside(
          cls       -> "wv-inspector",
          ariaLabel -> "Query inspector",
          div(cls -> "wv-empty wv-empty--small", s"Loading ${id}…")
        )
    }

end Inspector
