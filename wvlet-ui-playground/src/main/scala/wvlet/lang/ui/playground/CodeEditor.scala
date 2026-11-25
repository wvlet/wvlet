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
import wvlet.lang.ui.component.monaco.MonacoEditor
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Cancelable
import wvlet.uni.rx.Rx
import wvlet.uni.rx.RxVar
import wvlet.uni.ui.ColorScheme
import org.scalajs.dom

/**
  * A Monaco editor that fills its container (Monaco's automatic layout follows the pane sizes).
  *
  * @param text
  *   the editor text: edits are written to it, and a new value replaces the editor's text
  * @param language
  *   the language for syntax highlighting (`wvlet` or `sql`)
  * @param onAction
  *   called with the id of a Monaco action bound to a key, e.g. `run-query` for Cmd/Ctrl+Enter
  */
class CodeEditor(
    editorId: String,
    text: RxVar[String],
    language: Rx[String],
    theme: Rx[ColorScheme],
    readOnly: Boolean = false,
    onAction: String => Unit = _ => ()
) extends RxElement:
  private val editor = new MonacoEditor(editorId, "wvlet", text.get, action = cmd => onAction(cmd))

  private var subscriptions: Seq[Cancelable] = Nil

  /** The 1-origin line and column of the cursor */
  def cursor: LinePosition = LinePosition(
    editor.getLinePosition().toInt,
    editor.getColumnPosition().toInt
  )

  def focus(): Unit = editor.focus()

  /** Scroll to a 1-origin line, select it, and focus the editor */
  def revealLine(line: Int): Unit = editor.revealLine(line)

  override def onMount(node: Any): Unit =
    editor.render()
    if readOnly then
      editor.setReadOnly()
      editor.enableWordWrap()
    subscriptions = Seq(
      language.run(l => editor.setLanguage(l)),
      theme.run(t => editor.setDarkMode(CodeEditor.isDark(t))),
      text.run { t =>
        if editor.getText() != t then
          editor.setText(t)
      },
      // Monaco has no change callback in our facade; poll the text while mounted
      Rx.intervalMillis(150)
        .map { _ =>
          val t = editor.getText()
          if !readOnly && t != text.get then
            text := t
        }
        .subscribe()
    )

  override def beforeUnmount: Unit =
    subscriptions.foreach(_.cancel)
    subscriptions = Nil

  override def render: RxElement = div(cls -> "pg-editor", id -> editorId)

end CodeEditor

object CodeEditor:
  /** Whether the page shows dark colors: a forced scheme, or the OS preference for System */
  def isDark(scheme: ColorScheme): Boolean =
    scheme match
      case ColorScheme.Dark =>
        true
      case ColorScheme.Light =>
        false
      case ColorScheme.System =>
        dom.window.matchMedia("(prefers-color-scheme: dark)").matches
