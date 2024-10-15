package wvlet.lang.ui.editor

import org.scalablytyped.runtime.StringDictionary
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import typings.monacoEditor.mod.editor
import typings.monacoEditor.mod.languages
import org.scalajs.dom
import typings.monacoEditor.mod.editor.BuiltinTheme

import scala.scalajs.js

class WvletMonacoEditor extends RxElement:

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

  private def monacoEditorOptions: editor.IStandaloneEditorConstructionOptions =
    val languageId = "wvlet"
    languages.register(
      new:
        val id = languageId
        extensions = js.Array(".wv")
        aliases = js.Array("Wvlet")
    )

    languages.setMonarchTokensProvider(languageId, WvletMonarchLanguage)
    languages.setLanguageConfiguration(
      languageId,
      new:
        brackets = js.Array(js.Tuple2("(", ")"), js.Tuple2("{", "}"), js.Tuple2("[", "]"))
    )

    editor.defineTheme("vs-wvlet", editorTheme)

    // Disable minimap, which shows a small preview of the code
    val minimapOptions = editor.IEditorMinimapOptions()
    minimapOptions.enabled = false

    val sampleText =
      """-- Enter your query
         |from line item
         |where l_quantity > 10.0""".stripMargin

    val editorOptions = editor.IStandaloneEditorConstructionOptions()
    editorOptions
      .setValue(sampleText)
      // TODO Add a new language wvlet
      .setLanguage(languageId)
      .setTheme("vs-wvlet")
      // minimap options
      .setMinimap(minimapOptions)
      .setBracketPairColorization(
        new:
          val enables = true
      )

    editorOptions.tabSize = 2.0
    editorOptions

  end monacoEditorOptions

  override def onMount: Unit = editor.create(
    dom.document.getElementById("editor").asInstanceOf[dom.HTMLElement],
    monacoEditorOptions
  )

  override def render = div(cls -> "pl-0 pr-2", div(id -> "editor", style -> "min-height: 250px;"))

end WvletMonacoEditor
