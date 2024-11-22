package wvlet.lang.ui.playground

import typings.monacoEditor.mod.editor.{IDimension, IStandaloneCodeEditor}
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import typings.monacoEditor.mod.*
import typings.monacoEditor.mod.languages.*
import typings.monacoEditor.monacoEditorStrings
import org.scalajs.dom
import wvlet.lang.ui.component.MainFrame
import wvlet.lang.ui.component.MainFrame.NavBar

import scala.scalajs.js

class Editor extends RxElement:

  private var textEditor: IStandaloneCodeEditor = null
  private def initEditor(): Unit =
    textEditor = editor.create(
      dom.document.getElementById("wvlet-editor").asInstanceOf[dom.HTMLElement],
      monacoEditorOptions
    )

  private def monacoEditorOptions: editor.IStandaloneEditorConstructionOptions =
    val languageId = "sql"
    languages.register(
      new:
        val id = languageId
        extensions = js.Array(".wv")
        aliases = js.Array("Wvlet")
    )

    // languages.setMonarchTokensProvider(languageId, WvletMonarchLanguage())
    // languages.registerCompletionItemProvider(languageId, keywordCompletionProvider)
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

    // editor.defineTheme("vs-wvlet", editorTheme)

    // Disable minimap, which shows a small preview of the code
    val minimapOptions = editor.IEditorMinimapOptions()
    minimapOptions.enabled = false

    val editorOptions = editor.IStandaloneEditorConstructionOptions()
    editorOptions
      .setValue("from lineitem")
      // TODO Add a new language wvlet
      .setLanguage(languageId)
      .setFontFamily("Consolas, ui-monospace, SFMono-Regular, Menlo, Monaco, monospace")
      .setTheme("vs-dark")
      .setFontSize(13)
      // minimap options
      .setMinimap(minimapOptions)
      // Hide horizontal scrollbar as it's annoying
      .setScrollbar(editor.IEditorScrollbarOptions().setHorizontal(monacoEditorStrings.hidden))
      .setBracketPairColorization(
        new:
          val enables = true
      )

    editorOptions.tabSize = 2.0
    editorOptions.automaticLayout = true
    editorOptions

  end monacoEditorOptions

  // Enforce shrinking the editor height when the window is resized
  dom.window.onresize =
    _ =>
      if textEditor != null then
        val newWidth = textEditor.getContentWidth()
        val newHeight =
          dom.window.outerHeight - WvletPlaygroundMain.previewWindowHeightPx -
            MainFrame.navBarHeightPx
        textEditor.layout(IDimension(width = newWidth, height = newHeight))

  override def onMount: Unit = initEditor()

  override def render: RxElement = div(cls -> "h-full", id -> "wvlet-editor")

end Editor
