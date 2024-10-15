package wvlet.lang.ui.editor

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*

class WvletEditor(
    monacoEditor: WvletMonacoEditor,
    previewWindow: PreviewWindow,
    consoleLogWindow: ConsoleLogWindow
) extends RxElement:

  private def title(title: String) = h1(
    cls -> "text-slate-700 dark:text-white mt-2 text-sm font-light tracking-tight",
    title
  )

  override def render = div(
    cls -> "container px-2",
    div(
      cls -> "flex flex-row",
      div(cls -> "basis-1/2", title("Editor"), monacoEditor),
      div(cls -> "basis-1/2", title("Preview"), previewWindow),
      div(
        cls -> "absolute inset-x-0 bottom-0 h-48 w-100 bg-cyan-950 text-gray-100",
        consoleLogWindow
      )
    )
  )

end WvletEditor
