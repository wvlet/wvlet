package wvlet.lang.ui.component.editor

import wvlet.airframe.rx
import wvlet.airframe.rx.RxVar
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.airframe.rx.html.svgAttrs.{style as _, xmlns as _}
import wvlet.lang.api.v1.frontend.FrontendRPC.RPCAsyncClient
import wvlet.lang.api.v1.query.{QueryError, QueryRequest, QuerySelection}
import wvlet.lang.api.v1.query.QuerySelection.Describe
import wvlet.lang.ui.component.monaco.EditorBase
import wvlet.lang.ui.component.{MainFrame, WindowSize}

object WvletEditor:
  val editorWidthRem: Int  = 40 // rem (chars)
  val editorHeightRem: Int = 24 // rem (lines)
  def editorStyle =
    s"min-width: ${editorWidthRem}rem; max-width: ${editorWidthRem}rem; min-height: ${editorHeightRem}rem;"

class WvletEditor(
    fileNav: FileNav,
    monacoEditor: WvletMonacoEditor,
    previewWindow: PreviewWindow,
    consoleLogWindow: ConsoleLogWindow
) extends RxElement:

  private def title(title: String) = h1(
    cls -> "text-slate-300 dark:text-white mt-2 text-sm font-light tracking-tight",
    title
  )

  override def render =
    // grid
    div(
      cls   -> "flex flex-col bg-zinc-800",
      style -> s"height: calc(100vh - ${MainFrame.navBarHeightPx}px);",
      div(fileNav),
      div(
        cls -> "flex bg-black",
        div(cls -> "flex-none", style -> WvletEditor.editorStyle, monacoEditor),
        div(
          // span to the bottom of the screen
          cls   -> "grow bg-cyan-950 text-gray-100 px-2 overflow-y-auto scroll-auto",
          style -> s"height: ${WvletEditor.editorHeightRem}rem;",
          div(title("Console"), consoleLogWindow)
        )
      ),
      div(cls -> "h-dvh bg-zinc-800 overflow-y-auto", div(title("Preview"), previewWindow))
    )

end WvletEditor

class WvletMonacoEditor(
    windowSize: WindowSize,
    rpcClient: RPCAsyncClient,
    queryResultReader: QueryResultReader,
    errorReports: RxVar[List[QueryError]]
) extends EditorBase(
      windowSize,
      "main-editor",
      "wvlet",
      marginHeightPx = MainFrame.navBarHeightPx - 512
    ):
  override protected def initialText: String =
    """-- Enter a query
      |from lineitem
      |where l_quantity > 10.0
      |limit 10""".stripMargin

  private def processRequest(request: QueryRequest): Unit =
    ConsoleLog.write(s"Processing query with mode:${request.querySelection}\n${request.query}")
    queryResultReader.submitQuery(request)

  override protected def action: String => Unit = {
    case "describe-query" =>
      val req = QueryRequest(
        query = editor.getText(),
        querySelection = QuerySelection.Describe,
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

end WvletMonacoEditor
