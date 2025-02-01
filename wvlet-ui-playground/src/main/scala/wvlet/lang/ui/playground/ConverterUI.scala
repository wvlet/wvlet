package wvlet.lang.ui.playground

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.lang.ui.component.WindowSize
import wvlet.lang.ui.component.monaco.EditorBase

class ConverterUI extends RxElement:
  override def render: RxElement = div("Converter")

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
