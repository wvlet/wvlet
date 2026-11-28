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
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Cancelable
import wvlet.uni.rx.Rx

import scala.scalajs.js

/**
  * The server's one screen: the health strip, the query list with the inspector beside it, and the
  * editor below. Everything refreshes every second unless paused, and the keyboard drives the list
  * (see [[WorkloadConsole.Shortcuts]])
  */
class WorkloadConsole(state: ConsoleState, editor: EditorState) extends RxElement:
  private var timer: Cancelable                                         = Cancelable.empty
  private var keyHandler: Option[js.Function1[dom.KeyboardEvent, Unit]] = None
  private var armedCancel: Option[(String, Double)]                     = None
  private var noticeTimer: Option[js.timers.SetTimeoutHandle]           = None
  private var noticeSub: Cancelable                                     = Cancelable.empty

  private def openInEditor(id: String): Unit = editor.openQuery(id)

  /** Whether a key press belongs to a text field or the editor rather than the console */
  private def typing(e: dom.KeyboardEvent): Boolean =
    e.target match
      case el: dom.Element =>
        val tag = el.tagName.toLowerCase
        tag == "input" || tag == "textarea" || tag == "select" ||
        el.asInstanceOf[dom.html.Element].isContentEditable || el.closest(".monaco-editor") != null
      case _ =>
        false

  private def onKey(e: dom.KeyboardEvent): Unit =
    if e.key == "Escape" then
      if typing(e) then
        e.target.asInstanceOf[dom.html.Element].blur()
      else if state.checked.get.nonEmpty then
        state.checked := Set.empty
      else
        state.select(None)
    else if !typing(e) && !e.metaKey && !e.ctrlKey && !e.altKey then
      e.key match
        case "j" | "ArrowDown" =>
          e.preventDefault()
          state.moveSelection(1)
        case "k" | "ArrowUp" =>
          e.preventDefault()
          state.moveSelection(-1)
        case "Enter" =>
          state.selected.get.foreach(openInEditor)
        case "x" =>
          state.selected.get.foreach(state.toggleCheck)
        case "c" =>
          state.selected.get.foreach(cancelTwice)
        case "/" =>
          e.preventDefault()
          Option(dom.document.getElementById(QueryPane.SearchId)).foreach(
            _.asInstanceOf[dom.html.Input].focus()
          )
        case "e" =>
          e.preventDefault()
          editor.open := !editor.open.get
        case "v" =>
          state.view := (
            if state.view.get == ListView.Rows then
              ListView.Timeline
            else
              ListView.Rows
          )
        case " " =>
          e.preventDefault()
          state.paused := !state.paused.get
        case _ =>

  /** Cancel `id` on the second `c` within two seconds, so one stray key never cancels a query */
  private def cancelTwice(id: String): Unit =
    val now = js.Date.now()
    armedCancel match
      case Some((armed, at)) if armed == id && now - at < 2000 =>
        armedCancel = None
        state.notice := Some(s"Canceling ${id}")
        state.cancel(Seq(id))
      case _ =>
        state.queries.find(_.queryId == id) match
          case Some(q) if !q.status.isFinished =>
            armedCancel = Some(id -> now)
            state.notice := Some(s"Press c again to cancel ${id}")
          case _ =>
            state.notice := Some(s"${id} already finished")

  override def onMount(node: Any): Unit =
    Page.current.get match
      case Page.Console(sel) =>
        state.select(sel)
      case _ =>
    state.refresh()
    timer = Rx
      .intervalMillis(1000)
      .map { _ =>
        if !state.paused.get then
          state.refresh()
      }
      .subscribe()
    val h: js.Function1[dom.KeyboardEvent, Unit] = (e: dom.KeyboardEvent) => onKey(e)
    dom.document.addEventListener("keydown", h)
    keyHandler = Some(h)
    // Notices fade after a few seconds
    noticeSub = state
      .notice
      .run {
        case Some(_) =>
          noticeTimer.foreach(js.timers.clearTimeout)
          noticeTimer = Some(js.timers.setTimeout(3000)(state.notice := None))
        case None =>
      }

  override def beforeUnmount: Unit =
    timer.cancel
    noticeSub.cancel
    keyHandler.foreach(h => dom.document.removeEventListener("keydown", h))
    keyHandler = None

  override def render: RxElement = div(
    cls -> "wv-console",
    HealthStrip(state),
    Widgets.errorLine(state.error),
    div(
      cls -> "wv-console__work",
      div(
        cls -> "wv-console__left",
        QueryPane(state, openInEditor, () => editor.open := true),
        editor
          .open
          .map { open =>
            if open then
              EditorPanel(editor)
            else
              span()
          }
      ),
      Inspector(state, openInEditor)
    ),
    div(
      cls -> "wv-console__foot",
      state
        .notice
        .map {
          case Some(n) =>
            span(cls -> "wv-notice", role -> "status", n)
          case None =>
            span()
        },
      div(cls -> "wv-grow"),
      span(
        cls -> "wv-keys",
        WorkloadConsole
          .Shortcuts
          .map((k, what) =>
            span(cls -> "wv-keys__item", wvlet.uni.dom.HtmlTags.tag("kbd")(k), what)
          )
      )
    )
  )

end WorkloadConsole

object WorkloadConsole:
  /** The keyboard shortcuts of the console, shown in its footer */
  val Shortcuts: Seq[(String, String)] = Seq(
    "j/k"   -> "move",
    "Enter" -> "edit",
    "x"     -> "check",
    "c c"   -> "cancel",
    "/"     -> "search",
    "e"     -> "editor",
    "v"     -> "timeline",
    "Space" -> "pause",
    "Esc"   -> "close"
  )
