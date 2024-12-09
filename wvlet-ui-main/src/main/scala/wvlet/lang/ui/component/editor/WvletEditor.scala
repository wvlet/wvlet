package wvlet.lang.ui.component.editor

import wvlet.airframe.rx
import wvlet.airframe.rx.{Cancelable, RxVar}
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
  val previewHeightPx: Int = 512
  def editorStyle =
    s"min-width: ${editorWidthRem}rem; max-width: ${editorWidthRem}rem; min-height: ${editorHeightRem}rem;"

class WvletEditor(
    fileNav: FileNav,
    monacoEditor: WvletMonacoEditor,
    previewWindow: PreviewWindow,
    consoleLogWindow: ConsoleLogWindow,
    windowSize: WindowSize
) extends RxElement:

  private def title(title: String) = h1(
    cls -> "text-slate-300 dark:text-white mt-2 text-sm font-light tracking-tight",
    title
  )

  override def render =
    // grid
    div(
      cls   -> "flex flex-col h-full bg-zinc-800",
      style -> s"width: max-screen; height: calc(100vh - ${MainFrame.navBarHeightPx}px);",
      div(fileNav),
      div(
        cls -> "grid grid-cols-2 h-full w-full",
        div(monacoEditor),
        div(
          cls -> "bg-cyan-950 text-gray-100 px-2 overflow-y-auto scroll-auto",
          windowSize
            .getInnerHeight
            .map { h =>
              style -> s"height: ${(h - MainFrame.navBarHeightPx) / 2}px;"
            },
          // span to the bottom of the screen
          title("Console"),
          consoleLogWindow
        )
      ),
      div(cls -> "h-full bg-zinc-800", div(title("Preview"), previewWindow))
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
      marginHeightPx = MainFrame.navBarHeightPx + WvletEditor.previewHeightPx
    ):
  override protected def initialText: String =
    """-- Enter a query
      |from lineitem
      |where l_quantity > 10.0
      |limit 10""".stripMargin

  private def processRequest(request: QueryRequest): Unit =
    ConsoleLog.write(s"Processing query with mode:${request.querySelection}\n${request.query}")
    queryResultReader.submitQuery(request)

  private var errorMonitor: Cancelable = Cancelable.empty

  override def onMount: Unit =
    super.onMount
    errorMonitor = errorReports.subscribe { (errors: List[QueryError]) =>
      // TODO set error markers
    }

  override def beforeUnmount: Unit =
    super.beforeUnmount
    errorMonitor.cancel

  override protected def action: String => Unit = {
    case "describe-query" =>
      val req = QueryRequest(
        query = editor.getText(),
        querySelection = QuerySelection.Describe,
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

end WvletMonacoEditor
