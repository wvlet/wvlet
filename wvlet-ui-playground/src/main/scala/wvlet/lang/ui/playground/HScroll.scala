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
import wvlet.uni.dom.all.{*, given}

import scala.scalajs.js

/**
  * Sideways scrolling without a native scrollbar, which would take room below the content where the
  * browser draws scrollbars that way. A thin thumb shows over the bottom edge on hover; drag it, or
  * scroll with a trackpad or Shift + wheel.
  */
class HScroll(content: RxElement, extraCls: String = "") extends RxElement:
  private var root: Option[dom.HTMLElement] = None
  private var cleanup: () => Unit           = () => ()

  private def parts: Option[(dom.HTMLElement, dom.HTMLElement)] = root.flatMap { r =>
    for
      view  <- Option(r.querySelector(".pg-hscroll__view"))
      thumb <- Option(r.querySelector(".pg-hscroll__thumb"))
    yield (view.asInstanceOf[dom.HTMLElement], thumb.asInstanceOf[dom.HTMLElement])
  }

  // Size and place the thumb to match the visible part of the content
  private def update(): Unit = parts.foreach { (view, thumb) =>
    val client = view.clientWidth.toDouble
    val total  = view.scrollWidth.toDouble
    if total <= client + 1 then
      thumb.style.display = "none"
    else
      val w = math.max(24.0, client * client / total)
      val x = (client - w) * view.scrollLeft / (total - client)
      thumb.style.display = "block"
      thumb.style.width = s"${w}px"
      thumb.style.transform = s"translateX(${x}px)"
  }

  override def onMount(node: Any): Unit =
    root = Option(node).collect { case e: dom.HTMLElement =>
      e
    }
    parts.foreach { (view, thumb) =>
      val onScroll: js.Function1[dom.Event, Unit]      = _ => update()
      val onEnter: js.Function1[dom.Event, Unit]       = _ => update()
      val onDown: js.Function1[dom.PointerEvent, Unit] =
        e =>
          e.preventDefault()
          val startX    = e.clientX
          val startLeft = view.scrollLeft
          val ratio     = view.scrollWidth.toDouble / view.clientWidth
          thumb.setPointerCapture(e.pointerId)
          root.foreach(_.setAttribute("data-dragging", "true"))
          lazy val move: js.Function1[dom.PointerEvent, Unit] =
            (m: dom.PointerEvent) => view.scrollLeft = startLeft + (m.clientX - startX) * ratio
          lazy val up: js.Function1[dom.PointerEvent, Unit] =
            (_: dom.PointerEvent) =>
              root.foreach(_.removeAttribute("data-dragging"))
              thumb.removeEventListener("pointermove", move)
              thumb.removeEventListener("pointerup", up)
              thumb.removeEventListener("pointercancel", up)
          thumb.addEventListener("pointermove", move)
          thumb.addEventListener("pointerup", up)
          thumb.addEventListener("pointercancel", up)
      view.addEventListener("scroll", onScroll)
      root.foreach(_.addEventListener("mouseenter", onEnter))
      thumb.addEventListener("pointerdown", onDown)
      cleanup =
        () =>
          view.removeEventListener("scroll", onScroll)
          root.foreach(_.removeEventListener("mouseenter", onEnter))
          thumb.removeEventListener("pointerdown", onDown)
      update()
    }

  end onMount

  override def beforeUnmount: Unit =
    cleanup()
    cleanup = () => ()
    root = None

  override def render: RxElement = div(
    cls -> s"pg-hscroll ${extraCls}".trim,
    div(cls -> "pg-hscroll__view", content),
    div(cls -> "pg-hscroll__bar", div(cls -> "pg-hscroll__thumb"))
  )

end HScroll
