package wvlet.lang.ui.playground

import wvlet.airframe.Design
import wvlet.airframe.rx.html.RxElement
import wvlet.log.LogSupport
import wvlet.airframe.rx.html.all.*
import wvlet.lang.ui.component.MainFrame

object WvletPlaygroundMain extends LogSupport:
  private def design: Design = Design.newDesign.bindSingleton[PlaygroundFrame]

  def main(args: Array[String]): Unit =
    val ui = design.newSession.build[PlaygroundFrame]
    MainFrame(ui).renderTo("main")

class PlaygroundFrame(
    fileExplorer: FileExplorer,
    editor: Editor,
    sqlPreview: SQLPreview,
    resultViewer: ResultViewer
) extends RxElement:

  override def render = div(
    cls   -> "flex",
    style -> s"height: calc(100vh - ${MainFrame.navBarHeightPx}px);",
    div(cls -> "flex-none w-44 h-full", fileExplorer),
    div(
      cls -> "glow w-full h-full bg-black",
      div(
        cls -> "flex flex-col h-full",
        // two-column blocks with tailwind css
        div(
          cls -> "grid grid-cols-2 h-full",
          div(editor),
          div(cls -> "bg-gray-200 p-4 h-full", sqlPreview)
        ),
        resultViewer
      )
    )
  )

end PlaygroundFrame

class FileExplorer extends RxElement:
  override def render = div(
    cls -> "h-full bg-slate-700 p-3 text-sm text-slate-200",
    h2("Examples"),
    ul(li("file1.wv"), li("file2.wv"), li("file3.wv")),
    // border
    div(cls -> "border-t border-gray-300 mt-2 mb-2")
  )

end FileExplorer

class SQLPreview extends RxElement:
  override def render: RxElement = div(
    h2("SQL"),
    pre(
      cls -> "bg-gray-300 p-4",
      code(
        cls -> "language-sql",
        """select * from tbl
          |""".stripMargin
      )
    )
  )

end SQLPreview

class ResultViewer extends RxElement:
  override def render: RxElement = div(
    cls   -> "bg-zinc-800 text-xs text-slate-300 dark:text-white p-2",
    style -> "height: 32rem;",
    pre(code(cls -> "font-mono", "preview result"))
  )

end ResultViewer
