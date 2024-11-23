package wvlet.lang.ui.playground

import wvlet.airframe.Design
import wvlet.airframe.rx.html.RxElement
import wvlet.log.LogSupport
import wvlet.airframe.rx.html.all.*
import wvlet.lang.ui.component.{Icon, MainFrame}
import scalajs.js

object PlaygroundUI extends LogSupport:
  val previewWindowHeightPx = 512;
  val editorTabHeight       = 24;

  private def design: Design = Design
    .newDesign
    .bindSingleton[QueryRunner]
    .bindInstance[CurrentQuery] {
      val c = CurrentQuery()
      c.setQuery(DemoQuerySet.defaultQuerySet.head)
      c
    }

  def main(args: Array[String]): Unit =
    val ui = design.newSession.build[PlaygroundUI]
    MainFrame(ui).renderTo("main")

end PlaygroundUI

class PlaygroundUI(
    currentQuery: CurrentQuery,
    fileExplorer: QuerySelector,
    queryEditor: QueryEditor,
    sqlPreview: SQLPreview,
    resultViewer: QueryResultViewer
) extends RxElement:

  override def render =
    def clipButton(editor: EditorBase) = button(
      tpe   -> "button",
      cls   -> "flex-none mr-4 rounded bg-white/5 hover:text-white text-white/50 px-2 py-0",
      title -> "Copy to clipboard",
      Icon.clip,
      onclick -> { e =>
        val text = editor.getText
        js.Dynamic.global.navigator.clipboard.writeText(text)
      }
    )

    div(
      cls   -> "flex",
      style -> s"height: calc(100vh - ${MainFrame.navBarHeightPx}px);",
      div(cls -> "w-44 h-full", fileExplorer),
      div(
        cls -> "w-full h-full bg-slate-900",
        div(
          cls -> "flex flex-col h-full",
          // Editor header
          div(
            cls -> "h-7 text-xs font-light bg-stone-900 text-slate-400 p-2",
            div(
              cls -> "grid grid-cols-2",
              div(
                cls -> "flex",
                span(cls -> "flex-none px-2", "Wvlet"),
                currentQuery
                  .queryName
                  .map { queryName =>
                    span(cls -> "flex-none px-2 text-slate-200", queryName)
                  },
                span(cls -> "grow"),
                clipButton(queryEditor)
              ),
              div(
                cls -> "flex",
                span(cls -> "flex-none px-2", "Compiled SQL"),
                span(cls -> "grow"),
                clipButton(sqlPreview)
              )
            )
          ),
          // Two-column blocks for editors
          div(
            cls -> "grid grid-cols-2 h-full",
            div(cls -> "h-full", queryEditor),
            div(cls -> "h-full", sqlPreview)
          ),
          resultViewer
        )
      )
    )

  end render

end PlaygroundUI
