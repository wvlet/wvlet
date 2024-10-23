package wvlet.lang.ui.editor

import wvlet.airframe.rx
import wvlet.airframe.rx.Rx
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.airframe.rx.html.svgTags.*
import wvlet.airframe.rx.html.svgAttrs.{style as _, xmlns as _, *}
import wvlet.lang.api.v1.frontend.FileApi.FileRequest
import wvlet.lang.api.v1.frontend.FrontendRPC.RPCAsyncClient
import wvlet.lang.api.v1.io.FileEntry
import wvlet.lang.ui.component.{Icon, MainFrame}

object WvletEditor:
  val editorWidthRem: Int  = 40 // rem (chars)
  val editorHeightRem: Int = 28 // rem (lines)
  def editorStyle =
    s"min-width: ${editorWidthRem}rem; max-width: ${editorWidthRem}rem; min-height: ${editorHeightRem}rem;"

class WvletEditor(
    fileNav: FileNav,
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
      div(fileNav),
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
