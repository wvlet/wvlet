package wvlet.lang.ui.editor

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.airframe.rx.Rx

object PreviewWindow:
  val previewResult = Rx.variable("[query result]")

class PreviewWindow extends RxElement:
  override def render = div(
    cls -> "text-xs text-slate-300 dark:text-white overflow-x-scroll overflow-y-auto",
    PreviewWindow
      .previewResult
      .map { result =>
        pre(code(cls -> "font-mono", result))
      }
  )
