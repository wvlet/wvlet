package wvlet.lang.ui.playground

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*

class QuerySetSelector extends RxElement:
  override def render = div(
    cls -> "h-full bg-slate-700 p-3 text-sm text-slate-200",
    h2("Examples"),
    ul(li("file1.wv"), li("file2.wv"), li("file3.wv")),
    // border
    div(cls -> "border-t border-gray-300 mt-2 mb-2")
  )

end QuerySetSelector
