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

import wvlet.lang.api.LinePosition
import wvlet.uni.rx.Rx
import wvlet.uni.rx.RxVar

/**
  * A cell of the notebook: a query with its last result, or a text in markdown.
  *
  * @param id
  *   unique in the page, so a cell keeps its editor when cells are added, moved, or removed
  */
class Cell(val id: Int, val kind: CellKind, initial: String):
  val text: RxVar[String] = Rx.variable(initial)

  /** The output of the cell's last run */
  val output: RxVar[Option[RunOutput]] = Rx.variable(None)

  /** How many times the cell ran, shown next to it like a notebook's execution count */
  val runCount: RxVar[Int] = Rx.variable(0)

  /** A text cell shows its editor instead of the rendered markdown */
  val editing: RxVar[Boolean] = Rx.variable(false)

  /** The line of the notebook text before the cell's first line, at its last run */
  var lineOffset: Int = 0

  /** The text of the last run, so an edit runs the cell again only when the text changed */
  var lastRunText: String = ""

  // Set by the cell's editor while it is shown
  var cursor: () => LinePosition = () => LinePosition.NoPosition
  var reveal: Int => Unit        = _ => ()
  var focusEditor: () => Unit    = () => ()

  def source: CellSource = CellSource(kind, text.get)

  def isCode: Boolean = kind == CellKind.Code

  /** The 1-origin line of the cell's last non-blank line, where a run selects its statement */
  def lastLine: Int =
    val lines = text.get.split("\n", -1)
    lines.lastIndexWhere(_.trim.nonEmpty) + 1 max 1

  /** The checks of a run that belong to this cell's lines */
  def checksOf(out: RunOutput): Seq[Check] =
    val n = text.get.split("\n", -1).length
    out.checks.filter(c => c.line > lineOffset && c.line <= lineOffset + n)

end Cell
