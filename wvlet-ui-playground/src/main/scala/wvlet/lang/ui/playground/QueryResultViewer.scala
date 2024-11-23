package wvlet.lang.ui.playground

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.lang.api.v1.query.QueryResult

class QueryResultViewer(currentQuery: CurrentQuery) extends RxElement:
  override def render: RxElement = div(
    cls   -> "bg-zinc-800 text-xs text-slate-300 dark:text-white p-2",
    style -> s"height: ${PlaygroundUI.previewWindowHeightPx}px;",
    div(cls -> "h-7 font-light text-slate-400", "Preview"),
    currentQuery
      .lastQueryResult
      .map { result =>
        pre(cls -> "font-mono", printQueryResult(result))
      }
  )

  def printQueryResult(result: QueryResult): String = result.toString

end QueryResultViewer
