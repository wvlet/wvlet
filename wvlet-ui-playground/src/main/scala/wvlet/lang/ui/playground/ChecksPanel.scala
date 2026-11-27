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

import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}

/** The results of the file's `test` assertions, in source order, each linked to its line */
class TestsPanel(state: PlaygroundState) extends RxElement:

  private def entry(t: Check.Test): RxElement =
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
      pre(cls -> "pg-run-check__message", t.message)
    )

  override def render: RxElement = div(
    cls -> "pg-run-checks",
    state
      .output
      .map { out =>
        if out.tests.isEmpty then
          div(
            cls -> "pg-result__empty",
            "No ",
            code("test"),
            " statements in this file. Try the Testing & Debugging examples."
          )
        else
          div(cls -> "pg-run-checks__list", out.tests.map(entry))
      }
  )

end TestsPanel

/**
  * The rows of each `debug { ... }` block of the file, in source order, for the right panel. A
  * block's line is a button that selects it in the editor.
  */
class DebugPanel(state: PlaygroundState) extends RxElement:
  private val rows = ResultPanel(state)

  private def entry(d: Check.Debug): RxElement = div(
    cls          -> "pg-run-check pg-run-check--debug",
    data("line") -> d.line.toString,
    div(
      cls -> "pg-run-check__head",
      span(cls -> "pg-run-check__icon", aria("label") -> "debug", "◆"),
      span(cls -> "pg-run-check__title", "debug"),
      span(cls -> "pg-grow"),
      d.result match
        case Right(r) =>
          span(cls -> "pg-badge pg-badge--muted", ChecksPanel.rowCount(r.totalRows))
        case Left(_) =>
          span(cls -> "pg-badge pg-badge--danger", "failed")
      ,
      ChecksPanel.lineButton(state, d.line)
    ),
    pre(cls -> "pg-run-check__code", d.source),
    d.result match
      case Right(r) =>
        // Tall enough for a few rows; the table scrolls inside for more
        val visible = math.min(math.max(r.rows.size, 1), 10)
        div(
          cls       -> "pg-run-check__rows",
          styleAttr -> s"height: ${visible * 26 + 64}px",
          rows.table(r)
        )
      case Left(err) =>
        pre(cls -> "pg-run-check__message pg-run-check__message--error", err)
  )

  override def render: RxElement = div(
    cls -> "pg-output__view pg-run-checks",
    state
      .output
      .map { out =>
        val blocks = out.debugBlocks
        if blocks.isEmpty then
          div(
            cls -> "pg-result__empty",
            "No ",
            code("debug { ... }"),
            " blocks in this file. A debug block shows the rows at that point of a pipeline; try the Testing & Debugging examples."
          )
        else
          div(cls -> "pg-run-checks__list", blocks.map(entry))
      }
  )

end DebugPanel

object ChecksPanel:
  /** `1 row`, `1,234 rows` */
  def rowCount(n: Int): String =
    if n == 1 then
      "1 row"
    else
      f"${n}%,d rows"

  /** A button that selects a 1-origin line in the editor */
  def lineButton(state: PlaygroundState, n: Int): RxElement = button(
    tpe       -> "button",
    cls       -> "pg-plan-line",
    titleAttr -> s"Select line ${n} in the editor",
    onclick   -> { () =>
      state.revealLine(n)
    },
    s"line ${n}"
  )

  /** The CSS kind and icon of a test outcome, shared with the editor's gutter markers */
  def marker(s: TestStatus): (String, String) =
    s match
      case TestStatus.Passed =>
        ("passed", "✓")
      case TestStatus.Failed =>
        ("failed", "✗")
      case TestStatus.Warning =>
        ("warning", "!")

  /** Gutter markers for the checks of a run: one per line, showing the worst outcome */
  def lineMarkers(checks: Seq[Check]): Seq[LineMarker] =
    def rank(kind: String): Int =
      kind match
        case "failed" =>
          3
        case "warning" =>
          2
        case "passed" =>
          1
        case _ =>
          0
    checks
      .filter(_.line > 0)
      .map {
        case t: Check.Test =>
          LineMarker(t.line, marker(t.status)._1, s"${t.source}\n${t.message}")
        case d: Check.Debug =>
          d.result match
            case Right(r) =>
              LineMarker(d.line, "debug", s"debug: ${rowCount(r.totalRows)}")
            case Left(err) =>
              LineMarker(d.line, "failed", s"debug failed: ${err}")
      }
      .groupBy(_.line)
      .values
      .map(ms => ms.maxBy(m => rank(m.kind)).copy(message = ms.map(_.message).mkString("\n\n")))
      .toSeq
      .sortBy(_.line)

end ChecksPanel
