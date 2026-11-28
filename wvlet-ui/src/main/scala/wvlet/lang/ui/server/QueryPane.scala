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
import wvlet.lang.api.v1.query.FacetCount
import wvlet.lang.api.v1.query.QueryList
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.lang.api.v1.query.QuerySummary
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx

import java.time.Instant

/**
  * The query list of the console: filter chips with counts, a search box, rows with inline time and
  * memory bars, or a timeline of the same queries by session. Clicking a row opens it in the
  * inspector without leaving the list
  *
  * @param openInEditor
  *   loads a query's text into the editor
  */
class QueryPane(state: ConsoleState, openInEditor: String => Unit) extends RxElement:
  import QueryPane.*

  private def chip(label: String, count: Int, on: Boolean, title: String)(
      toggle: () => Unit
  ): RxElement = button(
    cls -> (
      if on then
        "wv-chip wv-chip--on"
      else
        "wv-chip"
    ),
    tpe             -> "button",
    aria("pressed") -> on.toString,
    titleAttr       -> title,
    onclick         -> { (_: dom.MouseEvent) =>
      toggle()
    },
    span(cls -> "wv-chip__label", label),
    span(cls -> "wv-chip__count", Fmt.count(count))
  )

  private def statusChips(l: Option[QueryList], selected: Set[QueryStatus]): Seq[RxElement] =
    val counts = l.map(_.facets.statuses).getOrElse(Nil).map(f => f.value -> f.count).toMap
    ChipStatuses.map { s =>
      val n = counts.getOrElse(s.toString, 0)
      chip(statusLabel(s), n, selected.contains(s), s"Show ${statusLabel(s).toLowerCase} queries")(
        () => state.toggleStatus(s)
      )
    }

  private def valueChips(
      name: String,
      values: Seq[FacetCount],
      current: Option[String],
      set: Option[String] => Unit
  ): Seq[RxElement] =
    // Keep the chosen value visible even when no kept query has it any more
    val shown =
      current match
        case Some(c) if !values.exists(_.value == c) =>
          values :+ FacetCount(c, 0)
        case _ =>
          values
    if shown.size <= 1 && current.isEmpty then
      Nil
    else
      span(cls -> "wv-chips__name", name) +:
        shown.map { f =>
          val on = current.contains(f.value)
          chip(f.value, f.count, on, s"${name} ${f.value}") { () =>
            set(
              if on then
                None
              else
                Some(f.value)
            )
          }
        }

  private def searchBox: RxElement = div(
    cls -> "wv-search",
    input(
      id          -> SearchId,
      tpe         -> "search",
      placeholder -> "Search query text  ( / )",
      ariaLabel   -> "Search query text",
      spellcheck  -> "false",
      oninput     -> { (e: dom.Event) =>
        state.text := e.target.asInstanceOf[dom.html.Input].value
        state.reloadList()
      }
    )
  )

  private def toolbar: RxElement = div(
    cls -> "wv-toolbar",
    searchBox,
    Rx.join(state.list, state.statuses, state.session, state.profile)
      .map { (l, st, se, pr) =>
        val facets = l.map(_.facets)
        div(
          cls       -> "wv-chips",
          role      -> "group",
          ariaLabel -> "Filters",
          statusChips(l, st),
          valueChips("Session", facets.map(_.sessions).getOrElse(Nil), se, state.filterSession),
          valueChips("Profile", facets.map(_.profiles).getOrElse(Nil), pr, state.filterProfile),
          if st.nonEmpty || se.nonEmpty || pr.nonEmpty then
            button(
              cls     -> "wv-link-button",
              tpe     -> "button",
              onclick -> { (_: dom.MouseEvent) =>
                Option(dom.document.getElementById(SearchId)).foreach(
                  _.asInstanceOf[dom.html.Input].value = ""
                )
                state.clearFilters()
              },
              "Clear"
            )
          else
            span()
        )
      },
    div(cls -> "wv-grow"),
    state
      .view
      .map { v =>
        div(
          cls       -> "wv-segmented",
          role      -> "group",
          ariaLabel -> "View",
          ListView
            .values
            .toSeq
            .map { opt =>
              button(
                tpe             -> "button",
                cls             -> "wv-segmented__item",
                aria("pressed") -> (opt == v).toString,
                titleAttr       -> "Switch view ( v )",
                onclick         -> { (_: dom.MouseEvent) =>
                  state.view := opt
                },
                opt match
                  case ListView.Rows =>
                    "List"
                  case ListView.Timeline =>
                    "Timeline"
              )
            }
        )
      },
    state
      .paused
      .map { p =>
        button(
          tpe -> "button",
          cls -> (
            if p then
              "wv-live wv-live--off"
            else
              "wv-live"
          ),
          titleAttr -> (
            if p then
              "Resume live updates"
            else
              "Pause live updates"
          ),
          onclick -> { (_: dom.MouseEvent) =>
            state.paused := !p
          },
          if p then
            "Paused"
          else
            "Live"
        )
      }
  )

  private def bar(cls0: String, pct: Double): RxElement = span(
    cls       -> cls0,
    styleAttr -> f"width: ${pct.max(0).min(100)}%.1f%%"
  )

  private def actionButton(label: String, title: String, tone: String = "")(
      run: () => Unit
  ): RxElement = button(
    tpe -> "button",
    cls -> (
      if tone.isEmpty then
        "wv-icon-button"
      else
        s"wv-icon-button wv-icon-button--${tone}"
    ),
    titleAttr -> title,
    ariaLabel -> title,
    onclick   -> { (e: dom.MouseEvent) =>
      e.stopPropagation()
      run()
    },
    label
  )

  private def row(
      q: QuerySummary,
      now: Instant,
      time: TimeBar,
      mem: Option[Double],
      poolBytes: Option[Long],
      isSelected: Boolean,
      isChecked: Boolean,
      isCanceling: Boolean
  ): RxElement =
    val status = q.status.toString.toLowerCase
    val held   = ConsoleLayout.peakOrReserved(q)
    div(
      cls -> (
        if isSelected then
          "wv-row wv-row--selected"
        else
          "wv-row"
      ),
      role             -> "option",
      aria("selected") -> isSelected.toString,
      data("query-id") -> q.queryId,
      data("status")   -> status,
      onclick          -> { (_: dom.MouseEvent) =>
        state.select(Some(q.queryId))
      },
      label(
        cls     -> "wv-row__check",
        onclick -> { (e: dom.MouseEvent) =>
          e.stopPropagation()
        },
        input(
          tpe       -> "checkbox",
          checked   -> isChecked,
          ariaLabel -> s"Select ${q.queryId}",
          onchange  -> { (_: dom.Event) =>
            state.toggleCheck(q.queryId)
          }
        )
      ),
      span(cls -> s"wv-dot wv-dot--${status}", titleAttr -> status, ariaLabel -> status),
      div(
        cls -> "wv-row__main",
        div(cls -> "wv-row__head", titleAttr -> q.queryHead, q.queryHead),
        div(
          cls -> "wv-row__meta",
          span(cls       -> "wv-mono", q.queryId),
          span(cls       -> "wv-row__session", q.sessionId.getOrElse(ConsoleLayout.DefaultSession)),
          span(titleAttr -> Fmt.dateTime(q.createdAt), Fmt.ago(q.createdAt, now)),
          q.resultRows.map(n => span(s"${Fmt.count(n)} rows")).getOrElse(span()),
          q.errorMessage
            .map(m => span(cls -> "wv-row__error", titleAttr -> m, m.linesIterator.nextOption()))
            .getOrElse(span())
        )
      ),
      div(
        cls       -> "wv-row__time",
        titleAttr ->
          s"Queued ${Fmt.duration(q.queuedMillis)}, ran ${q
              .runMillis
              .map(Fmt.duration)
              .getOrElse("-")}",
        div(
          cls -> "wv-bar",
          bar("wv-bar__queued", time.queuedPct),
          bar(s"wv-bar__run wv-bar__run--${status}", time.runPct)
        ),
        span(cls -> "wv-row__num", Fmt.duration(ConsoleLayout.totalMillis(q)))
      ),
      div(
        cls       -> "wv-row__mem",
        titleAttr -> (
          if !q.memory.tracked then
            "Memory not tracked"
          else
            poolBytes
              .map(p => s"${Fmt.percent(held, p)}% of the engine memory pool")
              .getOrElse("No pool limit")
        ),
        mem match
          case Some(pct) =>
            Seq(
              div(cls  -> "wv-bar", bar("wv-bar__mem", pct)),
              span(cls -> "wv-row__num", Fmt.bytes(held))
            )
          case None =>
            span(cls -> "wv-row__num wv-muted", "-")
      ),
      div(
        cls -> "wv-row__actions",
        if !q.status.isFinished then
          actionButton(
            if isCanceling then
              "…"
            else
              "■"
            ,
            "Cancel query",
            "danger"
          )(() => state.cancel(Seq(q.queryId)))
        else
          span()
        ,
        actionButton("✎", "Open in editor")(() => openInEditor(q.queryId)),
        actionButton("⧉", "Copy query id")(() => copy(q.queryId))
      )
    )

  end row

  private def copy(text: String): Unit =
    dom.window.navigator.clipboard.writeText(text)
    state.notice := Some(s"Copied ${text}")

  private def rows(l: QueryList): RxElement = Rx
    .join(state.selected, state.checked, state.canceling, state.cluster)
    .map { (sel, chk, canc, c) =>
      val now   = Instant.now()
      val times = ConsoleLayout.timeBars(l.queries)
      val mems  = ConsoleLayout.memoryBars(l.queries)
      val pool  = c.flatMap(_.memoryPool.capacityBytes)
      div(
        cls       -> "wv-rows",
        role      -> "listbox",
        ariaLabel -> "Queries",
        l.queries
          .map { q =>
            row(
              q,
              now,
              times(q.queryId),
              mems.get(q.queryId),
              pool,
              sel.contains(q.queryId),
              chk.contains(q.queryId),
              canc.contains(q.queryId)
            )
          }
      )
    }

  private def timeline(l: QueryList): RxElement = state
    .selected
    .map { sel =>
      val now    = Instant.now()
      val layout = ConsoleLayout.timeline(l.queries, now)
      val live   = l.queries.exists(!_.status.isFinished)
      div(
        cls -> "wv-gantt",
        div(
          cls -> "wv-gantt__axis",
          span(cls -> "wv-gantt__lane-name", ""),
          div(
            cls -> "wv-gantt__ticks",
            span(Fmt.clock(layout.start)),
            span(Fmt.duration(layout.spanMillis)),
            span(
              if live then
                "now"
              else
                Fmt.clock(layout.end)
            )
          )
        ),
        layout
          .lanes
          .map { lane =>
            div(
              cls -> "wv-gantt__lane",
              span(cls -> "wv-gantt__lane-name wv-mono", titleAttr -> lane.session, lane.session),
              div(
                cls       -> "wv-gantt__track",
                styleAttr -> s"height: ${lane.rows * QueryPane.GanttRowPx + 8}px",
                lane
                  .bars
                  .map { b =>
                    val status = b.query.status.toString.toLowerCase
                    div(
                      cls -> (
                        if sel.contains(b.query.queryId) then
                          "wv-gantt__bar wv-gantt__bar--selected"
                        else
                          "wv-gantt__bar"
                      ),
                      styleAttr ->
                        f"left: ${b.startPct}%.2f%%; width: max(3px, ${b.queuedPct +
                            b.runPct}%.2f%%); top: ${b.row * QueryPane.GanttRowPx + 6}px",
                      titleAttr ->
                        s"${b.query.queryId} · ${status} · ${Fmt.duration(
                            ConsoleLayout.totalMillis(b.query)
                          )}\n${b.query.queryHead}",
                      onclick -> { (_: dom.MouseEvent) =>
                        state.select(Some(b.query.queryId))
                      },
                      span(
                        cls       -> "wv-gantt__queued",
                        styleAttr -> {
                          val total = (b.queuedPct + b.runPct).max(0.0001)
                          f"width: ${b.queuedPct * 100 / total}%.1f%%"
                        }
                      ),
                      span(cls -> s"wv-gantt__run wv-bar__run wv-bar__run--${status}")
                    )
                  },
                if live then
                  div(cls -> "wv-gantt__now")
                else
                  span()
              )
            )
          }
      )
    }

  private def bulkBar: RxElement = Rx
    .join(state.checked, state.list)
    .map { (chk, l) =>
      if chk.isEmpty then
        span()
      else
        val cancellable = l
          .map(_.queries)
          .getOrElse(Nil)
          .filter(q => chk.contains(q.queryId) && !q.status.isFinished)
        div(
          cls       -> "wv-bulk",
          role      -> "region",
          ariaLabel -> "Selected queries",
          span(s"${chk.size} selected"),
          button(
            tpe      -> "button",
            cls      -> "wv-link-button wv-link-button--danger",
            disabled -> cancellable.isEmpty,
            onclick  -> { (_: dom.MouseEvent) =>
              state.cancel(cancellable.map(_.queryId))
            },
            s"Cancel ${cancellable.size} running"
          ),
          button(
            tpe     -> "button",
            cls     -> "wv-link-button",
            onclick -> { (_: dom.MouseEvent) =>
              state.checked := Set.empty
            },
            "Clear"
          )
        )
    }

  override def render: RxElement = div(
    cls -> "wv-pane",
    toolbar,
    bulkBar,
    Rx.join(state.list, state.view)
      .map {
        case (None, _) =>
          div(cls -> "wv-empty", "Loading queries…")
        case (Some(l), _) if l.queries.isEmpty =>
          div(
            cls -> "wv-empty",
            if l.totalMatched == 0 && state.statuses.get.isEmpty && state.text.get.isEmpty then
              "No query yet. Write one in the editor below and press ⌘↵."
            else
              "No query matches the filters."
          )
        case (Some(l), v) =>
          div(
            cls -> "wv-pane__body",
            v match
              case ListView.Rows =>
                rows(l)
              case ListView.Timeline =>
                timeline(l)
            ,
            if l.totalMatched > l.queries.size then
              div(
                cls -> "wv-muted wv-count",
                s"Showing the newest ${l.queries.size} of ${Fmt.count(
                    l.totalMatched
                  )}; search to narrow down"
              )
            else
              span()
          )
      }
  )

end QueryPane

object QueryPane:
  /** The DOM id of the search box, focused by `/` */
  val SearchId: String = "wv-query-search"

  /** The statuses with a chip, in the order users scan them */
  val ChipStatuses: Seq[QueryStatus] = Seq(
    QueryStatus.RUNNING,
    QueryStatus.QUEUED,
    QueryStatus.FINISHED,
    QueryStatus.FAILED,
    QueryStatus.CANCELED
  )

  /** The height of one sub-row of a timeline lane */
  val GanttRowPx: Int = 18

  def statusLabel(s: QueryStatus): String = s.toString.toLowerCase.capitalize
