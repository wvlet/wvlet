package wvlet.lang.ui.playground

import wvlet.airframe.rx.{Cancelable, Rx}
import wvlet.lang.api.LinePosition
import wvlet.lang.api.v1.query.{QueryRequest, QuerySelection}
import wvlet.lang.api.v1.query.QuerySelection.Describe
import wvlet.lang.compiler.QuerySelector
import wvlet.lang.ui.component.{MainFrame, WindowSize}
import wvlet.lang.ui.component.monaco.EditorBase

class QueryEditor(currentQuery: CurrentQuery, windowSize: WindowSize)
    extends EditorBase(
      windowSize,
      "wvlet-editor",
      "wvlet",
      marginHeightPx = PlaygroundUI.editorMarginHeight
    ):
  override def initialText: String = currentQuery.wvletQueryRequest.get.query

  private var monitor = Cancelable.empty

  override def onMount: Unit =
    super.onMount
    monitor = Rx
      .intervalMillis(100)
      .map { _ =>
        val query = getText
        if query != currentQuery.wvletQueryRequest.get.query then
          currentQuery.wvletQueryRequest := QueryRequest(query = query)
      }
      .subscribe()

  override def beforeUnmount: Unit =
    super.beforeUnmount
    monitor.cancel

  private def processRequest(req: QueryRequest): Unit = currentQuery.wvletQueryRequest := req

  override protected def action: String => Unit = {
    case "describe-query" =>
      val req = QueryRequest(
        query = editor.getText(),
        querySelection = Describe,
        linePosition = currentLinePosition(),
        isDebugRun = true
      )
      processRequest(req)
    case "run-query" =>
      val req = QueryRequest(
        query = editor.getText(),
        querySelection = QuerySelection.Single,
        linePosition = currentLinePosition(),
        isDebugRun = true
      )
      processRequest(req)
    case "run-subquery" =>
      val req = QueryRequest(
        query = editor.getText(),
        querySelection = QuerySelection.Subquery,
        linePosition = currentLinePosition(),
        isDebugRun = true
      )
      processRequest(req)

    case "run-production-query" =>
      val req = QueryRequest(
        query = editor.getText(),
        querySelection = QuerySelection.Single,
        linePosition = currentLinePosition(),
        isDebugRun = false
      )
      processRequest(req)
    case other =>
      warn(s"Unknown action: ${other}")
  }

end QueryEditor
