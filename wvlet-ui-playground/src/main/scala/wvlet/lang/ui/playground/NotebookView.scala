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
import wvlet.uni.rx.Cancelable
import wvlet.uni.ui.Textarea

import scala.collection.mutable
import scala.scalajs.js

/**
  * The notebook: text cells and query cells, each query with its result right below it. Cells keep
  * their views (and editors) across adds, moves, and removals of other cells.
  */
class NotebookView(state: PlaygroundState) extends RxElement:
  private val views = mutable.Map.empty[Int, RxElement]

  private def viewOf(c: Cell): RxElement = views.getOrElseUpdate(
    c.id,
    c.kind match
      case CellKind.Code =>
        CodeCellView(state, c)
      case CellKind.Markdown =>
        TextCellView(state, c)
  )

  override def render: RxElement = div(
    cls -> "pg-notebook",
    state
      .cells
      .map { cs =>
        val ids = cs.map(_.id).toSet
        views.filterInPlace((id, _) => ids.contains(id))
        div(cls -> "pg-notebook__cells", cs.map(viewOf))
      },
    div(cls -> "pg-notebook__add", NotebookView.addButtons(state, None))
  )

end NotebookView

object NotebookView:
  /** Buttons that add a query or a text cell after `after` (at the end when None) */
  def addButtons(state: PlaygroundState, after: Option[Cell]): Seq[RxElement] = Seq(
    button(
      cls       -> "pg-cell-btn",
      titleAttr -> "Add a query cell",
      onclick   -> { () =>
        val c = state.insertCell(CellKind.Code, after)
        js.timers.setTimeout(50)(c.focusEditor())
      },
      "+ Query"
    ),
    button(
      cls       -> "pg-cell-btn",
      titleAttr -> "Add a text cell",
      onclick   -> { () =>
        state.insertCell(CellKind.Markdown, after)
      },
      "+ Text"
    )
  )

  /** Move, add, and delete buttons, shown on a cell's hover */
  def toolbar(state: PlaygroundState, c: Cell): RxElement = div(
    cls -> "pg-cell__toolbar",
    button(
      cls       -> "pg-cell-btn",
      titleAttr -> "Move up",
      onclick   -> { () =>
        state.moveCell(c, -1)
      },
      "↑"
    ),
    button(
      cls       -> "pg-cell-btn",
      titleAttr -> "Move down",
      onclick   -> { () =>
        state.moveCell(c, 1)
      },
      "↓"
    ),
    addButtons(state, Some(c)),
    button(
      cls       -> "pg-cell-btn pg-cell-btn--danger",
      titleAttr -> "Delete this cell",
      onclick   -> { () =>
        state.deleteCell(c)
      },
      "✕"
    )
  )

end NotebookView

/** A query cell: the editor, its run count, and the result of its last run */
class CodeCellView(state: PlaygroundState, c: Cell) extends RxElement:
  private val results = ResultPanel(state)

  private val editor: CodeEditor = CodeEditor(
    s"pg-cell-${c.id}",
    c.text,
    language = state.mode.map(_.editorLanguage),
    theme = state.theme,
    onAction = {
      case "run-query" | "run-production-query" =>
        state.runCell(c, Some(editor.cursor))
      case "run-subquery" =>
        state.runCell(c, Some(editor.cursor), QuerySelection.Subquery)
      case "describe-query" =>
        state.runCell(c, Some(editor.cursor), QuerySelection.Describe)
      case "focus" =>
        state.focus(c)
      case _ =>
    },
    // The gutter markers of the cell's test and debug lines, in the cell's own line numbers
    markers = c
      .output
      .map(
        _.map(out =>
            ChecksPanel.lineMarkers(c.checksOf(out)).map(m => m.copy(line = m.line - c.lineOffset))
          )
          .getOrElse(Nil)
      ),
    autoHeight = Some((22, 520))
  )

  private var subscription: Cancelable                       = Cancelable.empty
  private var pendingRun: Option[js.timers.SetTimeoutHandle] = None

  override def onMount(node: Any): Unit =
    c.cursor = () => editor.cursor
    c.reveal = line => editor.revealLine(line)
    c.focusEditor = () => editor.focus()
    // Run the cell shortly after typing stops
    subscription = c
      .text
      .run { t =>
        if t != c.lastRunText && t.trim.nonEmpty then
          pendingRun.foreach(js.timers.clearTimeout)
          pendingRun = Some(
            js.timers
              .setTimeout(400) {
                pendingRun = None
                if c.text.get == t then
                  state.runCell(c, Some(editor.cursor))
              }
          )
      }

  override def beforeUnmount: Unit =
    subscription.cancel
    pendingRun.foreach(js.timers.clearTimeout)
    pendingRun = None

  private def testEntry(t: Check.Test): RxElement =
    val (kind, icon) = ChecksPanel.marker(t.status)
    div(
      cls          -> s"pg-run-check pg-run-check--${kind}",
      data("line") -> t.line.toString,
      div(
        cls -> "pg-run-check__head",
        span(cls -> "pg-run-check__icon", aria("label") -> kind, icon),
        span(cls -> "pg-run-check__source", t.source),
        ChecksPanel.lineButton(state, t.line)
      ),
      when(t.status != TestStatus.Passed, pre(cls -> "pg-run-check__message", t.message))
    )

  /**
    * The result card under the editor, in Treasure Work's style: a header with the statement, a
    * summary line, a compact grid of the first rows, and a footer that shows the rest
    */
  private def output(out: RunOutput): RxElement =
    val tests = c
      .checksOf(out)
      .collect { case t: Check.Test =>
        t
      }
    val failed  = tests.count(_.status == TestStatus.Failed)
    val result  = out.result.filter(r => !out.hasError && r.columns.nonEmpty)
    val summary = div(
      cls -> "pg-cell__summary",
      span(
        (
          result.map(r =>
            if r.isTruncated then
              f"${r.rows.size}%,d of ${r.totalRows}%,d rows"
            else
              ChecksPanel.rowCount(r.totalRows)
          ) ++ Option.when(out.hasError)("Error") ++
            Option.when(out.elapsedMillis > 0)(f"${out.elapsedMillis}%.1f ms")
        ).mkString(" · ")
      ),
      when(
        tests.nonEmpty,
        if failed > 0 then
          span(cls -> "pg-badge pg-badge--danger", s"${failed} of ${tests.size} tests failed")
        else
          span(cls -> "pg-badge pg-badge--success", s"${tests.size} tests passed")
      ),
      result.map { r =>
        button(
          cls       -> "pg-cell__link",
          titleAttr -> "Copy the rows as tab-separated text",
          onclick   -> { () =>
            js.Dynamic.global.navigator.clipboard.writeText(ResultPanel.toTsv(r))
          },
          "Copy"
        )
      }
    )
    val body =
      out.error match
        case Some(err) =>
          div(cls -> "pg-result__error", role -> "alert", pre(err))
        case None =>
          result match
            case Some(r) =>
              val n = r.rows.size
              div(
                c.showAllRows
                  .map { all =>
                    val shown =
                      if all then
                        math.min(n, Cell.expandedRows)
                      else
                        math.min(n, Cell.previewRows)
                    // CSS sizes the table to its columns (lines stop at the last one) and the
                    // card to the table; a wider table scrolls sideways. A preview doesn't scroll
                    // down (sorting still orders all rows); an expanded table past 30 rows does
                    val scrollsDown = all && n > Cell.expandedRows
                    div(
                      cls             -> "pg-cell__table",
                      data("scrolls") -> scrollsDown.toString,
                      HScroll(
                        div(
                          cls       -> "pg-cell__table-rows",
                          styleAttr -> s"height: ${Cell.tableHeight(shown)}px",
                          results.table(r, rowNumbers = false)
                        )
                      )
                    )
                  },
                when(
                  n > Cell.previewRows,
                  button(
                    cls     -> "pg-cell__link pg-cell__more",
                    onclick -> { () =>
                      c.showAllRows.update(!_)
                    },
                    c.showAllRows
                      .map(all =>
                        if all then
                          "Show fewer rows"
                        else
                          f"+${n - Cell.previewRows}%,d more rows"
                      )
                  )
                )
              )
            case None =>
              div(cls -> "pg-result__empty", "Done: the cell defines no query result.")
    // No card around the result: the summary line, then the table with its own border, so the
    // border always meets the last column. Folding keeps the summary
    div(
      cls -> "pg-cell__result",
      summary,
      c.resultOpen
        .map { open =>
          if open then
            div(body, when(tests.nonEmpty, div(cls -> "pg-cell__tests", tests.map(testEntry))))
          else
            span()
        }
    )

  end output

  override def render: RxElement = div(
    cls             -> "pg-cell pg-cell--code",
    data("cell")    -> c.id.toString,
    data("focused") -> state.focused.map(_.contains(c.id).toString),
    div(
      cls -> "pg-cell__gutter",
      c.runCount
        .map(n =>
          span(
            cls -> "pg-cell__count",
            if n == 0 then
              "[ ]"
            else
              s"[${n}]"
          )
        )
    ),
    div(
      cls     -> "pg-cell__body",
      onclick -> { (_: dom.MouseEvent) =>
        state.focus(c)
      },
      editor
    ),
    // The fold button sits in the gutter, beside the result it folds
    div(
      cls -> "pg-cell__out-gutter",
      c.output
        .map(
          _.map(_ =>
              button(
                cls       -> "pg-cell__fold",
                titleAttr -> "Show or hide the result",
                onclick   -> { () =>
                  c.resultOpen.update(!_)
                },
                c.resultOpen
                  .map(open =>
                    if open then
                      "▾"
                    else
                      "▸"
                  )
              )
            )
            .getOrElse(span())
        )
    ),
    // The result sits under the editor, aligned with the query
    div(
      cls     -> "pg-cell__out",
      onclick -> { (_: dom.MouseEvent) =>
        state.focus(c)
      },
      c.output.map(_.map(output).getOrElse(span()))
    ),
    NotebookView.toolbar(state, c)
  )

end CodeCellView

/** A text cell: markdown, rendered; double-click (or Edit) to change it */
class TextCellView(state: PlaygroundState, c: Cell) extends RxElement:

  private def editorView: RxElement = div(
    cls       -> "pg-cell__md-editor",
    onkeydown -> { (e: dom.KeyboardEvent) =>
      if e.key == "Escape" || (e.key == "Enter" && (e.ctrlKey || e.metaKey)) then
        e.preventDefault()
        c.editing := false
    },
    Textarea(c.text)
      .withRows(math.max(3, c.text.get.split("\n", -1).length + 1))
      .withPlaceholder(
        "Write markdown: # headings, **bold**, `code`, [links](https://...), - lists"
      )
      .withLabel("Text cell"),
    div(
      cls -> "pg-cell__md-actions",
      button(
        cls     -> "pg-cell-btn",
        onclick -> { () =>
          c.editing := false
        },
        "Done"
      ),
      span(cls -> "pg-cell__hint", "⌘↵ or Esc to finish")
    )
  )

  override def render: RxElement = div(
    cls -> "pg-cell pg-cell--text",
    div(cls -> "pg-cell__gutter"),
    div(
      cls -> "pg-cell__body",
      c.editing
        .map { editing =>
          if editing then
            editorView
          else
            div(
              cls        -> "pg-cell__md",
              titleAttr  -> "Double-click to edit",
              ondblclick -> { (_: dom.MouseEvent) =>
                c.editing := true
              },
              if c.text.get.trim.isEmpty then
                p(cls -> "pg-cell__hint", "Empty text cell: double-click to write.")
              else
                Markdown.render(c.text.get)
            )
        }
    ),
    NotebookView.toolbar(state, c)
  )

end TextCellView
