package wvlet.lang.ui.component.editor

import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx

object PreviewWindow:
  val previewResult = Rx.variable("[query result]")

class PreviewWindow extends RxElement:
  override def render = div(
    cls ->
      "text-xs text-slate-300 dark:text-white overflow-x-scroll overflow-y-auto scrollbar-hidden",
    PreviewWindow
      .previewResult
      .map { result =>
        pre(code(cls -> "font-mono", result))
      }
  )
