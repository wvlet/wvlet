package wvlet.lang.ui.editor

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*

class WvletEditor extends RxElement:

  private val monacoEditor     = WvletMonacoEditor()
  private val consoleLogWindow = ConsoleLogWindow()

  override def render = div(monacoEditor, consoleLogWindow)
