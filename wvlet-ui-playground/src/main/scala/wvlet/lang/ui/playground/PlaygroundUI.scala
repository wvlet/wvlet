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
package wvlet.lang.ui.playground

import org.scalajs.dom
import wvlet.lang.api.v1.query.QuerySelection
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.log.LogSupport
import wvlet.uni.rx.Cancelable
import wvlet.uni.rx.Rx
import wvlet.uni.ui.Button
import wvlet.uni.ui.ButtonSize
import wvlet.uni.ui.ButtonVariant
import wvlet.uni.ui.ColorScheme
import wvlet.uni.ui.Edge
import wvlet.uni.ui.FoldablePane
import wvlet.uni.ui.IconButton
import wvlet.uni.ui.Kbd
import wvlet.uni.ui.Select
import wvlet.uni.ui.Splitter
import wvlet.uni.ui.Tab
import wvlet.uni.ui.TabList
import wvlet.uni.ui.Toaster

import scala.scalajs.js

object PlaygroundUI extends LogSupport:
  def main(args: Array[String]): Unit =
    val state = PlaygroundState()
    ColorScheme.bind(state.theme)
    PlaygroundUI(state).renderTo("main")
    // Generate the tables and run the first query after the page has painted
    js.timers
      .setTimeout(30) {
        state.openFromHash()
      }

end PlaygroundUI

/**
  * The Wvlet playground: an example library on the left, the query editor in the middle with its
  * translation and plans beside it, and the result table below. Queries run in the browser on
  * Wvlet's columnar engine over generated TPC-H tables.
  */
class PlaygroundUI(state: PlaygroundState) extends RxElement:
  private val toasts = Toaster()

  private val editor: CodeEditor = CodeEditor(
    "pg-query-editor",
    state.text,
    language = state.mode.map(_.editorLanguage),
    theme = state.theme,
    onAction = {
      case "run-query" =>
        runAtCursor(QuerySelection.Single)
      case "run-subquery" =>
        runAtCursor(QuerySelection.Subquery)
      case "describe-query" =>
        runAtCursor(QuerySelection.Describe)
      case "run-production-query" =>
        runAtCursor(QuerySelection.Single)
      case _ =>
    }
  )

  private def runAtCursor(selection: QuerySelection): Unit = state.run(
    RunRequest(state.text.get, state.mode.get, editor.cursor, selection)
  )

  private var subscriptions: Seq[Cancelable]                 = Nil
  private var pendingRun: Option[js.timers.SetTimeoutHandle] = None

  override def onMount(node: Any): Unit =
    state.revealLine = line => editor.revealLine(line)
    subscriptions = Seq(
      narrowScreenDrawer(),
      // Run the statement being edited shortly after typing stops
      state
        .text
        .run { t =>
          if t != state.lastRequest.get.text && t.nonEmpty then
            pendingRun.foreach(js.timers.clearTimeout)
            pendingRun = Some(
              js.timers
                .setTimeout(400) {
                  pendingRun = None
                  if state.text.get == t then
                    state.run(RunRequest(t, state.mode.get, editor.cursor))
                }
            )
        },
      state
        .dialect
        .run(_ =>
          if state.lastRequest.get.text.nonEmpty then
            state.rerun()
        ),
      state
        .disabledOptimizations
        .run(_ =>
          if state.lastRequest.get.text.nonEmpty then
            state.rerun()
        )
    )

  end onMount

  /**
    * On a phone-width screen the sidebar is a drawer over the page, so keep it closed there: close
    * it at startup and whenever the window narrows, and restore its wide-screen state when the
    * window widens again.
    */
  private def narrowScreenDrawer(): Cancelable =
    val query    = dom.window.matchMedia(s"(max-width: ${PlaygroundState.narrowMaxWidth}px)")
    var wideOpen = state.sidebarOpen.get
    def apply(narrow: Boolean): Unit =
      if narrow then
        wideOpen = state.sidebarOpen.get
        state.sidebarOpen := false
      else
        state.sidebarOpen := wideOpen
    if query.matches then
      apply(true)
    val listener: js.Function1[dom.Event, Unit] = _ => apply(query.matches)
    query.addEventListener("change", listener)
    Cancelable(() => query.removeEventListener("change", listener))

  override def beforeUnmount: Unit =
    subscriptions.foreach(_.cancel)
    subscriptions = Nil

  private def headerBar: RxElement =
    val modeTabs = TabList(
      state.mode,
      label = "Query language",
      tabs = InputMode.values.toSeq.map(m => Tab(m, m.label))
    ).onChange { d =>
      // Convert the text instead of only switching the highlighting
      d.cancel()
      val next = d.value.asInstanceOf[InputMode]
      js.timers.setTimeout(0)(state.switchMode(next))
    }
    header(
      cls -> "pg-header",
      IconButton("Toggle sidebar", span(cls -> "pg-icon", "☰"))
        .withVariant(ButtonVariant.Ghost)
        .onClick(() => state.sidebarOpen.update(!_)),
      a(
        cls  -> "pg-brand",
        href -> "https://wvlet.org/wvlet/",
        img(src -> "./img/wvlet-icon.svg", alt -> "", cls -> "pg-brand__logo"),
        span("Wvlet"),
        span(cls -> "pg-brand__sub", "Playground")
      ),
      div(cls -> "pg-header__mode", modeTabs),
      Button("Describe")
        .withSize(ButtonSize.Small)
        .withLabel("Show the columns of the query at the cursor (Shift+Enter)")
        .onClick(() => runAtCursor(QuerySelection.Describe)),
      span(cls -> "pg-grow"),
      label(
        cls -> "pg-field",
        span("TPC-H"),
        Select(
          state.scaleFactor,
          options = PlaygroundEngine.scaleFactors,
          optionLabel = sf => s"SF ${sf}"
        ).withLabel("TPC-H scale factor")
          .onChange { _ =>
            state.status := "Generating TPC-H tables..."
            js.timers.setTimeout(30)(state.rerun())
          }
      ),
      ThemeSwitch(state.theme),
      Button("Share")
        .withSize(ButtonSize.Small)
        .withVariant(ButtonVariant.Ghost)
        .withLabel("Copy a link to this query")
        .onClick { () =>
          js.Dynamic.global.navigator.clipboard.writeText(state.shareUrl)
          toasts.success("Link copied")
        },
      a(
        cls    -> "pg-link",
        href   -> "https://wvlet.org/wvlet/docs/syntax/",
        target -> "_blank",
        "Docs"
      ),
      a(cls -> "pg-link", href -> "https://github.com/wvlet/wvlet", target -> "_blank", "GitHub")
    )

  end headerBar

  private def exampleInfo: RxElement = div(
    cls -> "pg-info",
    Rx.join(state.example, state.mode)
      .map {
        case (Some(e), _) =>
          div(
            cls -> "pg-info__body",
            span(cls -> "pg-info__category", Examples.categoryLabel(e.category)),
            span(cls -> "pg-info__title", e.title),
            when(e.description.nonEmpty, span(cls -> "pg-info__desc", e.description))
          )
        case (None, m) =>
          div(
            cls -> "pg-info__body",
            span(cls -> "pg-info__title", s"Your ${m.label} query"),
            span(
              cls -> "pg-info__desc",
              "Edits run automatically. Pick an example on the left to start from."
            )
          )
      }
  )

  private def resultHeader: RxElement = div(
    cls -> "pg-panel-header pg-result-header",
    span(cls -> "pg-panel-title", "Result"),
    state
      .output
      .map { out =>
        span(
          cls -> "pg-result-meta",
          when(out.statement.nonEmpty, span(cls -> "pg-result-stmt", out.statement)),
          out.result match
            case Some(r) if !out.hasError =>
              span(
                cls -> "pg-badge",
                if r.isTruncated then
                  f"${r.rows.size}%,d of ${r.totalRows}%,d rows"
                else
                  f"${r.totalRows}%,d rows"
              )
            case _ =>
              span()
          ,
          when(
            out.elapsedMillis > 0,
            span(cls -> "pg-badge pg-badge--muted", f"${out.elapsedMillis}%.1f ms")
          )
        )
      }
  )

  override def render: RxElement = div(
    cls -> "pg-app",
    headerBar,
    div(
      cls -> "pg-body",
      // Shown only on narrow screens, behind the sidebar drawer: a tap outside closes the drawer
      div(
        cls            -> "pg-backdrop",
        data("open")   -> state.sidebarOpen.map(_.toString),
        aria("hidden") -> "true",
        onclick        -> { (_: dom.MouseEvent) =>
          state.sidebarOpen := false
        }
      ),
      FoldablePane(
        state.sidebarOpen,
        width = state.sidebarWidth,
        edge = Edge.Right,
        content = Sidebar(state)
      ).withSplitter(
        Splitter(state.sidebarWidth, PlaygroundState.sidebarRange, Edge.Right, "Resize sidebar")
      ),
      div(
        cls -> "pg-main",
        div(
          cls -> "pg-top",
          div(cls -> "pg-editor-pane", exampleInfo, div(cls -> "pg-editor-wrap", editor)),
          FoldablePane(
            state.outputOpen,
            width = state.outputWidth,
            edge = Edge.Left,
            content = OutputPanel(state)
          ).withSplitter(
            Splitter(
              state.outputWidth,
              PlaygroundState.outputRange,
              Edge.Left,
              "Resize output panel"
            )
          )
        ),
        VerticalSplitter(state.resultHeight, PlaygroundState.resultRange, "Resize result panel"),
        div(
          cls       -> "pg-bottom",
          styleAttr -> state.resultHeight.map(h => s"height: ${h}px"),
          resultHeader,
          ResultPanel(state)
        )
      )
    ),
    footer(
      cls -> "pg-status",
      span(cls -> "pg-status__dot", data("error") -> state.output.map(_.hasError.toString)),
      state.status.map(s => span(s)),
      span(cls -> "pg-grow"),
      span(
        cls -> "pg-status__keys",
        Kbd("⌘↵"),
        " run ",
        Kbd("⇧↵"),
        " describe ",
        Kbd("⌃↵"),
        " subquery"
      ),
      state
        .scaleFactor
        .map(sf =>
          span(cls -> "pg-status__engine", s"Wvlet engine (Scala.js) · TPC-H SF ${sf} in memory")
        ),
      state.mode.map(m => span(cls -> "pg-status__mode", m.label))
    ),
    toasts
  )

end PlaygroundUI
