package wvlet.lang.ui.editor

import scala.scalajs.js
import typings.monacoEditor.mod.languages.CompletionItemKind
import typings.monacoEditor.mod.languages.CompletionItemInsertTextRule
import typings.monacoEditor.mod.languages.IMonarchLanguage
import typings.monacoEditor.mod.languages.CompletionItemProvider
import typings.monacoEditor.mod.languages.CompletionItem
import typings.monacoEditor.mod.Position
import typings.monacoEditor.mod.editor.ITextModel
import typings.monacoEditor.mod.languages.CompletionContext
import typings.monacoEditor.mod.languages.ProviderResult
import typings.monacoEditor.mod.CancellationToken
import typings.monacoEditor.mod.languages.CompletionList
import typings.monacoEditor.mod.IRange

val WvletMonacoKeywordCompletionProvider =
  new CompletionItemProvider:
    override def provideCompletionItems(
        model: ITextModel,
        position: Position,
        context: CompletionContext,
        token: CancellationToken
    ): ProviderResult[CompletionList] =
      new CompletionList:
        val suggestions = (Keywords ++ TypeKeywords).map(word =>
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
