package wvlet.lang.ui.playground

import wvlet.airframe.Design
import wvlet.airframe.rx.html.RxElement
import wvlet.log.LogSupport
import wvlet.airframe.rx.html.all.*
import wvlet.lang.ui.component.MainFrame

object PlaygroundUI extends LogSupport:
  val previewWindowHeightPx = 512;

  private def design: Design = Design.newDesign.bindSingleton[QueryRunner]

  def main(args: Array[String]): Unit =
    val ui = design.newSession.build[PlaygroundUI]
    MainFrame(ui).renderTo("main")

end PlaygroundUI

class PlaygroundUI(
    fileExplorer: QuerySetSelector,
    queryEditor: QueryEditor,
    sqlPreview: SQLPreview,
    resultViewer: QueryResultViewer
) extends RxElement:

  override def render = div(
    cls   -> "flex",
    style -> s"height: calc(100vh - ${MainFrame.navBarHeightPx}px);",
    div(cls -> "flex-none w-44 h-full", fileExplorer),
    div(
      cls -> "glow w-full h-full bg-slate-900",
      div(
        cls -> "flex flex-col h-full",
        // two-column blocks with tailwind css
        div(cls -> "grid grid-cols-2 h-full", queryEditor, sqlPreview),
        resultViewer
      )
    )
  )

end PlaygroundUI
