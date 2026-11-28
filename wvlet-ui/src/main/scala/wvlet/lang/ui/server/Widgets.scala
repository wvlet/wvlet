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
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Cancelable
import wvlet.uni.rx.Rx

/** Small building blocks shared by the server UI's pages */
object Widgets:

  /** A colored label of a query status */
  def statusBadge(s: QueryStatus): RxElement = span(
    cls -> s"wv-status wv-status--${s.toString.toLowerCase}",
    s.toString.toLowerCase
  )

  /** A horizontal bar filled to `percent`, turning warning past 80% and danger past 95% */
  def meter(percent: Int, label: String): RxElement =
    val level =
      if percent >= 95 then
        " wv-meter--danger"
      else if percent >= 80 then
        " wv-meter--warn"
      else
        ""
    div(
      cls           -> s"wv-meter${level}",
      role          -> "meter",
      ariaLabel     -> label,
      data("value") -> percent.toString,
      div(cls -> "wv-meter__fill", styleAttr -> s"width: ${percent.max(0).min(100)}%")
    )

  /**
    * A line of the last values of a metric, scaled to `max` (or to the largest value when `max` is
    * 0), drawn as inline SVG
    */
  def sparkline(values: Seq[Double], max: Double = 0, label: String): RxElement =
    val w   = 200.0
    val h   = 36.0
    val top =
      if max > 0 then
        max
      else
        values.maxOption.getOrElse(0.0).max(1.0)
    val step =
      if values.size > 1 then
        w / (values.size - 1)
      else
        w
    val pts = values
      .zipWithIndex
      .map((v, i) => f"${i * step}%.1f,${h - 2 - (v.min(top) / top) * (h - 4)}%.1f")
      .mkString(" ")
    div(
      cls -> "wv-spark-box",
      svg(
        cls                                                -> "wv-spark",
        viewBox                                            -> s"0 0 ${w.toInt} ${h.toInt}",
        wvlet.uni.dom.HtmlTags.attr("preserveAspectRatio") -> "none",
        role                                               -> "img",
        ariaLabel                                          -> label,
        if values.size > 1 then
          polyline(cls -> "wv-spark__line", points -> pts)
        else
          span()
      )
    )

  end sparkline

  /** A link to another page of the UI */
  def link(r: Page, content: DomNode*): RxElement = a(
    href    -> r.hash,
    onclick -> { (e: dom.MouseEvent) =>
      e.preventDefault()
      Page.go(r)
    },
    content
  )

  /** An error line, shown in place of data that failed to load */
  def errorLine(msg: Rx[Option[String]]): RxElement = msg.map {
    case Some(m) =>
      div(cls -> "wv-error", role -> "alert", m)
    case None =>
      span()
  }

end Widgets

/**
  * A page that reloads its data every `intervalMillis` while it is mounted, and once right away.
  * Pages are mounted only while shown, so hidden pages never poll
  */
trait Polling extends RxElement:
  protected def intervalMillis: Long = 1000L
  protected def refresh(): Unit

  private var timer = Cancelable.empty

  override def onMount(node: Any): Unit =
    refresh()
    timer = Rx.intervalMillis(intervalMillis).map(_ => refresh()).subscribe()

  override def beforeUnmount: Unit = timer.cancel
