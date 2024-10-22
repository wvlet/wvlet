package wvlet.lang.ui.editor

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.airframe.rx.html.svgTags.*
import wvlet.airframe.rx.html.svgAttrs.{xmlns as _, style as _, *}
import wvlet.lang.ui.component.MainFrame

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
        a(
          href -> "#",
          cls  -> "px-1 text-gray-400 hover:text-gray-300",
          svg(
            xmlns   -> "http://www.w3.org/2000/svg",
            viewBox -> "0 0 20 20",
            fill    -> "currentColor",
            cls     -> "size-4",
            path(
              fillRule -> "evenodd",
              d ->
                "M9.293 2.293a1 1 0 0 1 1.414 0l7 7A1 1 0 0 1 17 11h-1v6a1 1 0 0 1-1 1h-2a1 1 0 0 1-1-1v-3a1 1 0 0 0-1-1H9a1 1 0 0 0-1 1v3a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1v-6H3a1 1 0 0 1-.707-1.707l7-7Z",
              clipRule -> "evenodd"
            )
          )
        )
      )
    ),
    li(
      cls -> "flex",
      div(
        cls -> "flex items-center",
        svg(
          cls         -> "h-4 w-4 flex-shrink-0 text-gray-400",
          viewBox     -> "0 0 20 20",
          fill        -> "currentColor",
          aria.hidden -> "true",
          path(
            fillRule -> "evenodd",
            d ->
              "M8.22 5.22a.75.75 0 0 1 1.06 0l4.25 4.25a.75.75 0 0 1 0 1.06l-4.25 4.25a.75.75 0 0 1-1.06-1.06L11.94 10 8.22 6.28a.75.75 0 0 1 0-1.06Z",
            clipRule -> "evenodd"
          )
        ),
        a(
          href -> "#",
          cls  -> "px-1 text-sm font-medium text-gray-500 hover:text-gray-300",
          "Projects"
        )
      )
    )
  )

end FileNav
