package wvlet.lang.ui.playground

import wvlet.airframe.rx.{Cancelable, Rx}
import wvlet.lang.api.LinePosition
import wvlet.lang.api.v1.query.{QueryRequest, QuerySelection}
import wvlet.lang.api.v1.query.QuerySelection.Describe
import wvlet.lang.ui.component.{MainFrame, WindowSize}
import wvlet.lang.ui.component.monaco.EditorBase

class QueryEditor(currentQuery: CurrentQuery, windowSize: WindowSize)
    extends EditorBase(
      windowSize,
      "wvlet-editor",
      "wvlet",
      marginHeightPx = PlaygroundUI.editorMarginHeight
    ):
  override def initialText: String = currentQuery.wvletQuery.get

  private var monitor = Cancelable.empty

  override def onMount: Unit =
    super.onMount
    monitor = Rx
      .intervalMillis(100)
      .map { _ =>
        val query = getText
        if query != currentQuery.wvletQuery.get then
          currentQuery.wvletQuery := query
      }
      .subscribe()

  override def beforeUnmount: Unit =
    super.beforeUnmount
    monitor.cancel

  private def processRequest(req: QueryRequest): Unit =
    // TODO Process query
    info(s"Query request: ${req}")

  override protected def action: String => Unit = {
    case "describe-query" =>
      val req = QueryRequest(
        query = editor.getText(),
        querySelection = Describe,
        nodeLocation = currentNodeLocation(),
        isDebugRun = true
      )
      processRequest(req)
    case "run-query" =>
      val req = QueryRequest(
        query = editor.getText(),
        querySelection = QuerySelection.Single,
        nodeLocation = currentNodeLocation(),
        isDebugRun = true
      )
      processRequest(req)
    case "run-subquery" =>
      val req = QueryRequest(
        query = editor.getText(),
        querySelection = QuerySelection.Subquery,
        nodeLocation = currentNodeLocation(),
        isDebugRun = true
      )
      processRequest(req)

    case "run-production-query" =>
      val req = QueryRequest(
        query = editor.getText(),
        querySelection = QuerySelection.Single,
        nodeLocation = currentNodeLocation(),
        isDebugRun = false
      )
      processRequest(req)
    case other =>
      warn(s"Unknown action: ${other}")
  }

end QueryEditor
