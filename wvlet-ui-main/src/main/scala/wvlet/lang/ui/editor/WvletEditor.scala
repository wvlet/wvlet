package wvlet.lang.ui.editor

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.airframe.rx.html.svgTags.*
import wvlet.airframe.rx.html.svgAttrs.{style as _, xmlns as _, *}
import wvlet.lang.ui.component.{Icon, MainFrame}

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
      cls   -> "flex flex-col bg-zinc-800",
      style -> s"height: calc(100vh - ${MainFrame.navBarHeightPx}px);",
      div(FileNav),
      div(
        cls -> "flex bg-black",
        div(cls -> "flex-none", style -> WvletEditor.editorStyle, monacoEditor),
        div(
          // span to the bottom of the screen
          cls   -> "grow bg-cyan-950 text-gray-100 px-2 overflow-y-auto scroll-auto",
          style -> s"height: ${WvletEditor.editorHeightRem}rem;",
          div(title("Console"), consoleLogWindow)
        )
      ),
      div(cls -> "h-dvh bg-zinc-800 overflow-y-auto", div(title("Preview"), previewWindow))
    )

end WvletEditor

object FileNav extends RxElement:
  override def render: RxElement = nav(
    cls -> "flex h-4 text-sm text-gray-400",
    ol(role -> "list", cls -> "flex space-x-4 rounded-md px-1 shadow"),
    li(
      cls -> "flex",
      div(
        cls -> "flex items-center",
        a(href -> "#", cls -> "px-1 text-gray-400 hover:text-gray-300", Icon.home(cls -> "size-4"))
      )
    ),
    li(
      cls -> "flex",
      div(
        cls -> "flex items-center",
        Icon.chevron,
        a(
          href -> "#",
          cls  -> "px-1 text-sm font-medium text-gray-500 hover:text-gray-300",
          "Projects"
        )
      )
    )
  )

end FileNav
