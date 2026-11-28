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
import wvlet.lang.compiler.lsp.StdLibIndex
import wvlet.lang.ui.component.monaco.MonacoEditor
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
    registerEditorServices(state)
    PlaygroundUI(state).renderTo("main")
    // Generate the tables and run the first query after the page has painted
    js.timers
      .setTimeout(30) {
        state.openFromHash()
      }
    // Links in text cells (#example=..., #function=...) open in place
    dom.window.addEventListener("hashchange", (_: dom.Event) => state.openFromHash())

  /**
    * Hover tooltips and completion in the Wvlet editors: column types and stdlib function docs from
    * the language server's providers, compiled against the current TPC-H tables
    */
  private def registerEditorServices(state: PlaygroundState): Unit = MonacoEditor
    .registerLanguageServices(
      (text, offset) =>
        state
          .engine
          .hover(text, offset)
          .map { h =>
            js.Dynamic
              .literal(
                content = h.content,
                startLine = h.startLine,
                startColumn = h.startColumn,
                endLine = h.endLine,
                endColumn = h.endColumn
              )
          }
          .orNull,
      (text, offset) =>
        js.Array(
          state
            .engine
            .complete(text, offset)
            .map(i => js.Dynamic.literal(label = i.label, kind = i.kind, detail = i.detail))*
        ),
      // Member functions of the standard library, colored as calls after a dot
      js.Array(StdLibIndex.allFunctions.filter(_.ownerType.nonEmpty).map(_.name).distinct.sorted*)
    )

end PlaygroundUI

/**
  * The Wvlet playground: an example library on the left, a notebook of text and query cells (each
  * query with its result below it) in the middle, and the focused cell's SQL, plans, and docs on
  * the right. Queries run in the browser on Wvlet's columnar engine over generated TPC-H tables.
  */
class PlaygroundUI(state: PlaygroundState) extends RxElement:
  private val toasts = Toaster()

  private def describeFocused(): Unit = state
    .focusedCell
    .orElse(state.cells.get.find(_.isCode))
    .foreach(c => state.runCell(c, Some(c.cursor()), QuerySelection.Describe))

  private var subscriptions: Seq[Cancelable] = Nil

  override def onMount(node: Any): Unit =
    subscriptions = Seq(
      narrowScreenDrawer(),
      state.dialect.run(_ => state.rerun()),
      state.disabledOptimizations.run(_ => state.rerun())
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
        .withLabel("Show the columns of the focused cell's query at the cursor (Shift+Enter)")
        .onClick(() => describeFocused()),
      Button("Run all")
        .withSize(ButtonSize.Small)
        .withVariant(ButtonVariant.Ghost)
        .withLabel("Run every query cell from the top")
        .onClick(() => state.runAll()),
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
            js.timers.setTimeout(30)(state.runAll())
          }
      ),
      ThemeSwitch(state.theme),
      IconButton("Toggle the SQL, plan, and docs panel", span(cls -> "pg-icon", "◨"))
        .withVariant(ButtonVariant.Ghost)
        .onClick(() => state.outputOpen.update(!_)),
      Button("Share")
        .withSize(ButtonSize.Small)
        .withVariant(ButtonVariant.Ghost)
        .withLabel("Copy a link to this notebook")
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
          div(cls -> "pg-notebook-pane", NotebookView(state)),
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
        )
      )
    ),
    footer(
      cls -> "pg-status",
      span(
        cls           -> "pg-status__dot",
        data("error") -> state.output.map(o => (o.hasError || o.failedChecks > 0).toString)
      ),
      state.status.map(s => span(s)),
      span(cls -> "pg-grow"),
      span(
        cls -> "pg-status__keys",
        Kbd("⌘↵"),
        " run cell ",
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
