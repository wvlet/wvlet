package wvlet.lang.ui.editor

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*

object WvletEditor:
  val editorWidthRem: Int  = 32 // rem (chars)
  val editorHeightRem: Int = 24 // rem (lines)
  def editorStyle =
    s"min-width: ${editorWidthRem}rem; max-width: ${editorWidthRem}rem; min-height: ${editorHeightRem}rem;"

class WvletEditor(
    monacoEditor: WvletMonacoEditor,
    previewWindow: PreviewWindow,
    consoleLogWindow: ConsoleLogWindow
) extends RxElement:

  private def title(title: String) = h1(
    cls -> "text-slate-300 dark:text-white mt-2 text-sm font-light tracking-tight",
    title
  )

  override def render =
    // grid
    div(
      cls -> "flex flex-col h-screen",
      div(
        cls -> "flex bg-black",
        div(cls -> "flex-none", style -> WvletEditor.editorStyle, monacoEditor),
        div(
          // span to the bottom of the screen
          cls -> "grow bg-cyan-950 text-gray-100 px-2 overflow-y-auto scroll-auto",
          div(title("Console"), consoleLogWindow)
        )
      ),
      div(cls -> "h-dvh bg-black overflow-y-auto", div(title("Preview"), previewWindow))
    )

end WvletEditor
