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

class PlaygroundFrame(fileExplorer: FileExplorer, editor: Editor, sqlPreview: SQLPreview)
    extends RxElement:

  override def render = div(
    cls   -> "flex",
    style -> s"height: calc(100vh - ${MainFrame.navBarHeightPx}px);",
    div(cls -> "flex-none w-44 h-full bg-gray-200 p-1", fileExplorer),
    div(
      cls -> "glow w-full h-full",
      div(
        cls -> "flex flex-col h-full",
        // two-column blocks with tailwind css
        div(
          cls -> "grid grid-cols-2 h-full",
          div(cls -> "bg-gray-300 p-4 h-full", editor),
          div(cls -> "bg-gray-200 p-4 h-full", sqlPreview)
        ),
        div(
          cls   -> "bg-zinc-800 text-xs text-slate-300 dark:text-white p-2",
          style -> "height: 32rem;",
          pre(code(cls -> "font-mono", "preview result"))
        )
      )
    )
  )

end PlaygroundFrame

class FileExplorer extends RxElement:
  override def render = div(
    cls -> "bg-gray-200 p-2",
    h2("Examples"),
    ul(li("file1.wv"), li("file2.wv"), li("file3.wv")),
    // border
    div(cls -> "border-t border-gray-300 mt-2 mb-2")
  )

end FileExplorer

class Editor extends RxElement:
  override def render: RxElement = div(
    h2("Editor"),
    pre(
      cls -> "bg-gray-300 p-4",
      code(
        cls -> "language-sql",
        """select * from tbl
          |""".stripMargin
      )
    )
  )

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
