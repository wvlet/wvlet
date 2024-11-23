package wvlet.lang.ui.playground

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.lang.api.v1.query.QueryResult
import wvlet.lang.compiler.TablePrinter

class QueryResultViewer(currentQuery: CurrentQuery) extends RxElement:
  override def render: RxElement =
    val consoleHeight = PlaygroundUI.previewWindowHeightPx - 50
    div(
      cls -> "bg-zinc-800 text-xs text-slate-300 dark:text-white p-2",
      div(cls -> "h-7 font-light text-slate-400", "Preview"),
      div(
        // Important: Setting the width relative to the viewport width in order to hide overflowed contents
        style ->
          s"overflow-x: scroll; overflow-y: scroll; max-width: calc(100vw - 184px); height: ${consoleHeight}px; max-height: ${consoleHeight}px;",
        currentQuery
          .lastQueryResult
          .map { result =>
            pre(cls -> "font-mono", printQueryResult(result))
          }
      )
    )

  def printQueryResult(result: QueryResult): String = TablePrinter.printTableRows(result)

end QueryResultViewer
