package wvlet.lang.ui.playground

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.lang.api.v1.query.QueryResult
import wvlet.lang.compiler.TablePrinter
import org.scalajs.dom

class QueryResultViewer(currentQuery: CurrentQuery, windowSize: WindowSize) extends RxElement:
  override def render: RxElement =

    def renderQueryResult(consoleHeight: Int): RxElement = div(
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

    windowSize
      .getInnerHeight
      .map { h =>
        val consoleHeight = (dom.window.innerHeight / 3)
          .min(PlaygroundUI.previewWindowHeightPx)
          .toInt
        renderQueryResult(consoleHeight)
      }

  def printQueryResult(result: QueryResult): String = TablePrinter.printTableRows(result)

end QueryResultViewer
