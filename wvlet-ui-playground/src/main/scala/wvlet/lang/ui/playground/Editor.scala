package wvlet.lang.ui.playground

import typings.monacoEditor.mod.editor.{IDimension, IStandaloneCodeEditor}
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import typings.monacoEditor.mod.*
import typings.monacoEditor.mod.languages.*
import typings.monacoEditor.monacoEditorStrings
import org.scalajs.dom
import wvlet.lang.ui.component.MainFrame
import wvlet.lang.ui.component.MainFrame.NavBar

import scala.scalajs.js
import scala.scalajs.js.annotation.JSGlobal

class Editor extends RxElement:

  private val editor = new MonacoEditor()

  // Enforce shrinking the editor height when the window is resized
  dom.window.onresize = _ => editor.adjustSize()

  override def onMount: Unit = editor.renderTo("wvlet-editor")

  override def render: RxElement = div(cls -> "h-full", id -> "wvlet-editor")

end Editor
