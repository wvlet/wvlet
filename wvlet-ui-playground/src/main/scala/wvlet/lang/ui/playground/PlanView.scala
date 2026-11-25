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
import wvlet.uni.rx.Rx

/**
  * One line of a source-first plan text (`LogicalPlanPrinter`, `RuntimePlan.explain`,
  * `Operator.explain`): a node, or an attribute line of the node above (`- ...`).
  *
  * @param marker
  *   `↓` (consumes the line above), `↙` (joins inputs), `▶`/`▼` (logical plan nesting), or empty
  * @param name
  *   the operator name, e.g. `Filter`, `HashJoin[Inner]`
  * @param outputs
  *   the output columns (`-> [...]`), when the plan prints them
  * @param sourceLine
  *   the 1-origin line of the query the node comes from (logical plans)
  */
case class PlanLine(
    indent: Int,
    marker: String,
    name: String,
    detail: String,
    outputs: String,
    sourceLine: Option[Int],
    isAttribute: Boolean
)

object PlanLine:
  private val markers      = Set('↓', '↙', '▶', '▼', '→')
  private val lineRef      = """\s*-?\s*\(line:(\d+)\.\d+\)\s*$""".r
  private val numberPrefix = """^\d+\)\s*""".r

  /** Parse a plan text into lines; a header such as `[LogicalPlan: ...]` is dropped */
  def parse(text: String): Seq[PlanLine] =
    text
      .linesIterator
      .filterNot(l => l.trim.isEmpty || (l.startsWith("[") && l.endsWith("]")))
      .map(parseLine)
      // Bare flags (e.g. a join's asof flag) say nothing without their names
      .filterNot(l => l.isAttribute && (l.detail == "false" || l.detail == "true"))
      .toSeq

  private def parseLine(raw: String): PlanLine =
    val indent = raw.takeWhile(_ == ' ').length
    var rest   = raw.drop(indent)
    if rest.startsWith("- ") then
      PlanLine(indent, "", "", rest.drop(2).trim, "", None, isAttribute = true)
    else
      val marker =
        if rest.nonEmpty && markers.contains(rest.head) then
          val m = rest.head.toString
          rest = rest.drop(1).trim
          m
        else
          ""
      rest = numberPrefix.replaceFirstIn(rest, "")
      // Source line reference of logical plans
      val sourceLine = lineRef.findFirstMatchIn(rest).map(_.group(1).toInt)
      rest = lineRef.replaceFirstIn(rest, "")
      // Output columns of engine plans
      val (body, outputs) =
        rest.lastIndexOf(" -> [") match
          case -1 =>
            (rest, "")
          case i =>
            (rest.take(i), rest.drop(i + 4).trim)
      // The name ends at the first `(` or `: ` (a `[...]` suffix such as [Inner] stays with it)
      val cut            = Seq(body.indexOf('('), body.indexOf(": ")).filter(_ >= 0).minOption
      val (name, detail) =
        cut match
          case Some(i) =>
            (body.take(i), body.drop(i).stripPrefix(": ").stripSuffix(":").trim)
          case None =>
            (body.stripSuffix(":").trim, "")
      PlanLine(indent, marker, name.trim, detail, outputs, sourceLine, isAttribute = false)
    end if

  end parseLine

end PlanLine

/**
  * A plan drawn as styled rows: operator names stand out, attributes and output columns are muted,
  * and a node's source line, when known, is a button that selects that line in the editor.
  *
  * @param showOutputs
  *   whether to show the output columns of each node
  */
class PlanView(
    plan: Rx[String],
    emptyMessage: String,
    showOutputs: Rx[Boolean] = Rx.const(false),
    onSourceLine: Int => Unit = _ => ()
) extends RxElement:

  private def row(l: PlanLine, outputs: Boolean): RxElement =
    if l.isAttribute then
      div(
        cls       -> "pg-plan-row pg-plan-row--attr",
        styleAttr -> s"padding-left: ${l.indent}ch",
        span(cls -> "pg-plan-attr", l.detail)
      )
    else
      div(
        cls       -> "pg-plan-row",
        styleAttr -> s"padding-left: ${l.indent}ch",
        when(l.marker.nonEmpty, span(cls -> "pg-plan-marker", l.marker)),
        span(cls -> "pg-plan-name", l.name),
        when(l.detail.nonEmpty, span(cls -> "pg-plan-detail", l.detail)),
        l.sourceLine
          .map { n =>
            button(
              tpe       -> "button",
              cls       -> "pg-plan-line",
              titleAttr -> s"Select line ${n} in the editor",
              onclick   -> { () =>
                onSourceLine(n)
              },
              s"line ${n}"
            )
          }
          .getOrElse(span()),
        when(outputs && l.outputs.nonEmpty, div(cls -> "pg-plan-outputs", l.outputs))
      )

  override def render: RxElement = div(
    cls -> "pg-plan-view",
    Rx.join(plan, showOutputs)
      .map { (text, outputs) =>
        val lines = PlanLine.parse(text)
        if lines.isEmpty then
          div(cls -> "pg-result__empty", emptyMessage)
        else
          div(cls -> "pg-plan-rows", lines.map(row(_, outputs)))
      }
  )

end PlanView
