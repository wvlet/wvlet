package wvlet.lang.ui.playground

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.lang.api.v1.query.QueryResult
import wvlet.lang.compiler.TablePrinter
import org.scalajs.dom
import wvlet.lang.api.v1.query.QuerySelection.{Describe, Subquery}
import wvlet.lang.ui.component.WindowSize

class QueryResultViewer(currentQuery: CurrentQuery, windowSize: WindowSize) extends RxElement:
  override def render: RxElement =

    def renderQueryResult(consoleHeight: Int): RxElement = div(
      cls -> "bg-zinc-800 text-xs text-slate-300 dark:text-white p-2",
      div(
        cls -> "h-7 font-light text-slate-400",
        "Preview",
        currentQuery
          .wvletQueryRequest
          .map { req =>
            val queryLine = req.queryLine
            req.querySelection match
              case Describe =>
                span(
                  span(cls -> "px-2 text-teal-400", s"describe"),
                  span(cls -> "text-blue-400", s"(line:${req.linePosition.line}): "),
                  span(cls -> "text-slate-400", queryLine)
                )
              case Subquery =>
                span(
                  span(cls -> "px-2 text-pink-400", "subquery"),
                  span(cls -> "text-blue-400", s"(line:${req.linePosition.line}): "),
                  span(cls -> "text-slate-300", queryLine)
                )
              case _ if req.isDebugRun =>
                span()
              case _ =>
                span(cls -> "px-2 text-sky-400", "production mode")
          }
      ),
      div(
        // Important: Setting the width relative to the viewport width in order to hide overflowed contents
        style ->
          s"overflow-x: scroll; overflow-y: scroll; max-width: fit-content; height: ${consoleHeight}px; max-height: ${consoleHeight}px;",
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
        val consoleHeight = (dom.window.innerHeight / 5 * 2)
          .min(PlaygroundUI.previewWindowMaxHeightPx)
          .toInt
        renderQueryResult(consoleHeight)
      }

  end render

  private def printQueryResult(result: QueryResult): String = TablePrinter.printTableRows(result)

end QueryResultViewer
