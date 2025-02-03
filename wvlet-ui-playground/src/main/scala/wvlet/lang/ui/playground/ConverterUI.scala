package wvlet.lang.ui.playground

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.lang.ui.component.WindowSize
import wvlet.lang.ui.component.monaco.EditorBase

class ConverterUI(sqlEditor: SqlEditor) extends RxElement:
  override def render: RxElement = div(sqlEditor)

class SqlEditor(currentQuery: CurrentQuery, windowSize: WindowSize)
    extends EditorBase(
      windowSize,
      "sql-editor",
      "sql",
      marginHeight = PlaygroundUI.editorMarginHeight
    ):
  override def initialText: String =
    """
      |select *
      |from lineitem
      |limit 10
    """.stripMargin

  override def render: RxElement = div(cls -> "h-full", id -> editor.id)

class WvletViewer(currentQuery: CurrentQuery) extends RxElement:
  override def render: RxElement = div(
    cls   -> "h-full bg-slate-700 p-3 text-sm text-slate-200 overflow-y-auto",
    style -> s"max-height: calc(100vh - ${MainFrame.navBarHeightPx}px); ",
    h2(cls -> "text-slate-400", "Wvlet"),
    separator(),
    div(
      h2(cls -> "text-sm font-bold text-slate-300", currentQuery.queryName.get),
      ul(
        li(
          cls -> "text-slate-400 hover:text-white px-2",
          a(
            href -> "#",
            "Run",
            onclick -> { e =>
              e.preventDefault()
              println(s"Run query: ${currentQuery.wvletQueryRequest.get.query}")
            }
          )
        )
      )
    ),
    separator()
  )

  def separator(): RxElement = div(cls -> "border-t border-gray-600 mt-2 mb-2")
end WvletViewer
