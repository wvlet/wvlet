package wvlet.lang.ui.editor

import org.scalablytyped.runtime.StringDictionary
import org.scalajs.dom
import typings.monacoEditor.mod.*
import typings.monacoEditor.mod.editor.{
  BuiltinTheme,
  IActionDescriptor,
  ICodeEditor,
  IStandaloneCodeEditor,
  ITextModel
}
import typings.monacoEditor.mod.languages.*
import typings.monacoEditor.monacoEditorStrings
import wvlet.airframe.rx.{Cancelable, Rx}
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.lang.api.v1.frontend.FileApi.FileRequest
import wvlet.lang.api.v1.frontend.FrontendRPC.RPCAsyncClient
import wvlet.lang.ui.component.GlobalState

import scala.scalajs.js
import scala.scalajs.js.Promise

object WvletMonacoEditor:
  import WvletMonarchLanguage.*

  private val keywordCompletionProvider =
    new CompletionItemProvider:
      override def provideCompletionItems(
          model: ITextModel,
          position: Position,
          context: CompletionContext,
          token: CancellationToken
      ): ProviderResult[CompletionList] =
        new CompletionList:
          val suggestions = (keywords ++ typeKeywords).map(word =>
            new CompletionItem:
              val label: String      = word
              val kind               = CompletionItemKind.Keyword
              val insertText: String = word
              val range: IRange =
                new:
                  val startLineNumber = position.lineNumber
                  val startColumn     = position.column - 1
                  val endLineNumber   = position.lineNumber
                  val endColumn       = position.column
          )

end WvletMonacoEditor

class WvletMonacoEditor(rpcClient: RPCAsyncClient, queryResultReader: QueryResultReader)
    extends RxElement:
  import WvletMonacoEditor.*

  private def editorTheme: editor.IStandaloneThemeData =
    val theme = editor.IStandaloneThemeData(
      base = BuiltinTheme.`vs-dark`,
      inherit = true,
      colors = StringDictionary(("editor.background", "#202124")),
      rules = js.Array(
        editor.ITokenThemeRule("keyword").setForeground("#58ccf0"),
        editor.ITokenThemeRule("identifier").setForeground("#ffffff"),
        editor.ITokenThemeRule("type.identifier").setForeground("#aaaaff"),
        editor.ITokenThemeRule("type.keyword").setForeground("#cc99cc"),
        editor.ITokenThemeRule("string").setForeground("#f4c099"),
        editor.ITokenThemeRule("string.backquoted").setForeground("#f4c0cc"),
        editor.ITokenThemeRule("comment").setForeground("#99cc99"),
        editor.ITokenThemeRule("operator").setForeground("#aaaaaa"),
        editor.ITokenThemeRule("invalid").setForeground("#ff9999")
      )
    )
    theme

  private var textEditor: IStandaloneCodeEditor = null

  private val sampleText =
    """-- Enter a query
      |from lineitem
      |where l_quantity > 10.0
      |limit 10""".stripMargin

  private def monacoEditorOptions: editor.IStandaloneEditorConstructionOptions =
    val languageId = "wvlet"
    languages.register(
      new:
        val id = languageId
        extensions = js.Array(".wv")
        aliases = js.Array("Wvlet")
    )

    languages.setMonarchTokensProvider(languageId, WvletMonarchLanguage())
    languages.registerCompletionItemProvider(languageId, keywordCompletionProvider)
    languages.setLanguageConfiguration(
      languageId,
      new:
        brackets = js.Array(
          js.Tuple2("(", ")"),
          js.Tuple2("{", "}"),
          js.Tuple2("[", "]"),
          js.Tuple2("${", "}")
        )
    )

    editor.defineTheme("vs-wvlet", editorTheme)

    // Disable minimap, which shows a small preview of the code
    val minimapOptions = editor.IEditorMinimapOptions()
    minimapOptions.enabled = false

    val editorOptions = editor.IStandaloneEditorConstructionOptions()
    editorOptions
      .setValue(sampleText)
      // TODO Add a new language wvlet
      .setLanguage(languageId)
      .setTheme("vs-wvlet")
      // minimap options
      .setMinimap(minimapOptions)
      // Hide horizontal scrollbar as it's annoying
      .setScrollbar(editor.IEditorScrollbarOptions().setHorizontal(monacoEditorStrings.hidden))
      .setBracketPairColorization(
        new:
          val enables = true
      )

    editorOptions.tabSize = 2.0
    editorOptions

  end monacoEditorOptions

  private def queryUpToTheLine: String =
    val query    = getTextValue
    val lines    = query.split("\n")
    val lineNum  = textEditor.getPosition().lineNumber.toInt
    val subQuery = lines.take(lineNum).mkString("\n")
    subQuery

  private def runQuery: Unit =
    val query = getTextValue
    ConsoleLog.write(s"Run query:\n${query}")
    queryResultReader.submitQuery(query, isTestRun = false)

  private def testRunQuery(): Unit =
    val queryFragment = queryUpToTheLine
    val subQuery      = s"${queryFragment}\nlimit 40"
    ConsoleLog.write(s"Run test query:\n${subQuery}")
    queryResultReader.submitQuery(subQuery, isTestRun = true)

  private def describeQuery(): Unit =
    val subQuery = queryUpToTheLine
    ConsoleLog.write(s"Describe query:\n${subQuery}")
    queryResultReader.submitQuery(s"${subQuery}\ndescribe", isTestRun = true)

  private def buildEditor: Unit =
    textEditor = editor.create(
      dom.document.getElementById("editor").asInstanceOf[dom.HTMLElement],
      monacoEditorOptions
    )

    // Add shortcut keys
    textEditor.onKeyDown { (e: IKeyboardEvent) =>
      // ctrl + enter to submit the query
      if e.keyCode == KeyCode.Enter && (e.ctrlKey || e.metaKey) then
        runQuery
    }

    {
      val acc = IActionDescriptor(
        id = "run-query",
        label = "run query",
        run = (editor: ICodeEditor, args: Any) => runQuery
      )
      acc.keybindings = js.Array(
        KeyMod.chord(
          KeyMod.WinCtrl.toInt | KeyCode.KeyJ.toInt,
          KeyMod.WinCtrl.toInt | KeyCode.KeyR.toInt
        )
      )
      textEditor.addAction(acc)
    }

    {
      val acc = IActionDescriptor(
        id = "test-query",
        label = "test query",
        run = (editor: ICodeEditor, args: Any) => testRunQuery()
      )
      acc.keybindings = js.Array(
        KeyMod.chord(
          KeyMod.WinCtrl.toInt | KeyCode.KeyJ.toInt,
          KeyMod.WinCtrl.toInt | KeyCode.KeyT.toInt
        )
      )
      textEditor.addAction(acc)
    }

    {
      val acc = IActionDescriptor(
        id = "describe-query",
        label = "Describe query",
        run = (editor: ICodeEditor, args: Any) => describeQuery()
      )
      acc.keybindings = js.Array(
        KeyMod.chord(
          KeyMod.WinCtrl.toInt | KeyCode.KeyJ.toInt,
          KeyMod.WinCtrl.toInt | KeyCode.KeyD.toInt
        )
      )
      textEditor.addAction(acc)
    }

  end buildEditor

  def getTextValue: String = textEditor.getValue()

  override def onMount: Unit = buildEditor

  private var editorTextReloadMonitor = Cancelable.empty

  override def beforeRender: Unit =
    val rx = GlobalState
      .selectedPath
      .flatMap { path =>
        rpcClient
          .FileApi
          .readFile(FileRequest(path))
          .map { file =>
            if file.isFile && file.content.isDefined then
              textEditor.setValue(file.content.get)
          }
      }
    editorTextReloadMonitor.cancel
    editorTextReloadMonitor = rx.run()

  override def beforeUnmount: Unit = editorTextReloadMonitor.cancel

  override def render = div(
    cls -> "pl-0 pr-2",
    div(
      id -> "editor",
      // Need to set the exact size here to set the initial size of the Monaco editor
      style -> WvletEditor.editorStyle
    )
  )

end WvletMonacoEditor
