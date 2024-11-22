package wvlet.lang.ui.playground

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*

class QueryResultViewer extends RxElement:
  override def render: RxElement = div(
    cls   -> "bg-zinc-800 text-xs text-slate-300 dark:text-white p-2",
    style -> s"height: ${PlaygroundUI.previewWindowHeightPx}px;",
    pre(cls -> "font-mono", "preview result")
  )

end QueryResultViewer
