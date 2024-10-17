package wvlet.lang.ui.editor

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*

class WvletEditor(
    monacoEditor: WvletMonacoEditor,
    previewWindow: PreviewWindow,
    consoleLogWindow: ConsoleLogWindow
) extends RxElement:

  private def title(title: String) = h1(
    cls -> "text-slate-300 dark:text-white mt-2 text-sm font-light tracking-tight",
    title
  )

  override def render = div(
    cls -> "grid grid-cols-3 bg-black h-full",
    div(cls -> "col-span-1 max-h-2/3", monacoEditor),
    div(
      cls -> "col-start-2 col-end-4 bg-black max-h-full overflow-y-auto",
      div(title("Preview"), previewWindow)
    ),
    div(
      // span to the bottom of the screen
      cls ->
        "col-start-1 col-end-4 bg-cyan-950 text-gray-100 h-screen max-h-full px-2 overflow-y-auto scroll-auto",
      div(title("Console"), consoleLogWindow)
    )
  )

end WvletEditor
