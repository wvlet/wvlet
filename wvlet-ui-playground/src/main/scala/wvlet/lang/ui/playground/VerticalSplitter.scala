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
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.SizeRange
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx
import wvlet.uni.rx.RxVar

/**
  * A horizontal grip below a panel that resizes its height: drag it, or focus it and press Up and
  * Down (Shift for larger steps). Double-click restores the default height. uni-ui's `Splitter`
  * resizes widths only.
  *
  * @param height
  *   the panel's height in CSS pixels
  */
class VerticalSplitter(
    height: RxVar[Int],
    range: SizeRange,
    label: String,
    onResize: () => Unit = () => ()
) extends RxElement:
  private val dragging = Rx.variable(false)

  private def clamp(h: Int): Int =
    onResize()
    math.max(range.min, math.min(range.max, h))

  override def render: RxElement = div(
    cls                 -> "pg-vsplit",
    role                -> "separator",
    aria("orientation") -> "horizontal",
    ariaLabel           -> label,
    aria("valuenow")    -> height.map(_.toString),
    aria("valuemin")    -> range.min.toString,
    aria("valuemax")    -> range.max.toString,
    tabindex            -> "0",
    data("dragging")    -> dragging.map(_.toString),
    ondblclick          -> { (_: dom.MouseEvent) =>
      height := range.default
    },
    onkeydown -> { (e: dom.KeyboardEvent) =>
      val step =
        if e.shiftKey then
          64
        else
          16
      e.key match
        case "ArrowDown" =>
          e.preventDefault()
          height := clamp(height.get + step)
        case "ArrowUp" =>
          e.preventDefault()
          height := clamp(height.get - step)
        case _ =>
    },
    onpointerdown -> { (e: dom.PointerEvent) =>
      e.preventDefault()
      val grip   = e.currentTarget.asInstanceOf[dom.HTMLElement]
      val startY = e.clientY
      val startH = height.get
      dragging := true
      grip.setPointerCapture(e.pointerId)
      lazy val move: scala.scalajs.js.Function1[dom.PointerEvent, Unit] =
        (m: dom.PointerEvent) => height := clamp(startH + (m.clientY - startY).toInt)
      lazy val up: scala.scalajs.js.Function1[dom.PointerEvent, Unit] =
        (_: dom.PointerEvent) =>
          dragging := false
          grip.removeEventListener("pointermove", move)
          grip.removeEventListener("pointerup", up)
          grip.removeEventListener("pointercancel", up)
      grip.addEventListener("pointermove", move)
      grip.addEventListener("pointerup", up)
      grip.addEventListener("pointercancel", up)
    }
  )

end VerticalSplitter
