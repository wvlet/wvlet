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
import wvlet.uni.ui.Column
import wvlet.uni.ui.DataTable

/** One result row with its position, the key of the table */
case class ResultRow(index: Int, values: IndexedSeq[Option[String]])

/** The query result as a sortable table, or the error of the last run */
class ResultPanel(state: PlaygroundState) extends RxElement:

  // Numbers sort numerically, NULLs first
  private given Ordering[Option[BigDecimal]] = Ordering.Option(Ordering.BigDecimal)

  private def numeric(v: Option[String]): Option[BigDecimal] = v.flatMap(s =>
    scala.util.Try(BigDecimal(s)).toOption
  )

  private def column(i: Int, c: ResultColumn): Column[ResultRow] =
    val header = s"${c.name}"
    val col    = Column[ResultRow](
      s"c${i}",
      header,
      cell =
        r =>
          r.values(i) match
            case None =>
              span(cls -> "pg-null", "null")
            case Some(v) =>
              span(titleAttr -> v, v)
    )
    val sorted =
      if c.isNumeric then
        col.sortBy(r => numeric(r.values(i))).alignEnd
      else
        col.sortBy(r => r.values(i).getOrElse(""))
    sorted.withWidth("minmax(96px, max-content)")

  private def table(r: ResultTable): RxElement =
    if r.columns.isEmpty then
      div(cls -> "pg-result__empty", "The statement returned no relation.")
    else
      val rows      = r.rows.zipWithIndex.map((v, i) => ResultRow(i, v))
      val rowNumber = Column[ResultRow](
        "row",
        "#",
        cell = r => span(cls -> "pg-rownum", (r.index + 1).toString)
      ).sortBy(_.index).alignEnd.withWidth("56px")
      div(
        cls -> "pg-result__table",
        div(
          cls -> "pg-result__types",
          r.columns.map(c => span(cls -> "pg-type-chip", s"${c.name}: ${c.typeName}"))
        ),
        DataTable[ResultRow, Int](
          Rx.const(rows),
          key = _.index,
          columns = rowNumber +: r.columns.zipWithIndex.map((c, i) => column(i, c))
        ).withLabel("Query result")
          .withRowLabel(r => r.values.map(_.getOrElse("null")).mkString(", "))
      )

  override def render: RxElement = div(
    cls -> "pg-result",
    state
      .output
      .map { out =>
        out.error match
          case Some(err) =>
            div(cls -> "pg-result__error", role -> "alert", pre(err))
          case None =>
            out.result match
              case Some(r) =>
                table(r)
              case None =>
                div(cls -> "pg-result__empty", "Run a query to see its result.")
      }
  )

end ResultPanel
