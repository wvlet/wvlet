package wvlet.lang.ui.component.monaco

import scala.scalajs.js
import scala.scalajs.js.annotation.JSGlobal

/**
  * The Scala.js facade of the Monaco editor wrapper in `MonacoEditor.ts`, which an app's `main.js`
  * imports and assigns to `window.MonacoEditor`
  */
@js.native
@JSGlobal("MonacoEditor")
class MonacoEditor(
    val id: String,
    lang: String,
    initialText: String,
    action: js.Function1[String, Unit] = { cmd =>
      // do nothing by default
    }
) extends js.Object:
  def hello(): Unit       = js.native
  def render(): Unit      = js.native
  def setReadOnly(): Unit = js.native

  /** Release the editor and its model; call when its element leaves the page */
  def dispose(): Unit                                 = js.native
  def adjustHeight(newHeight: Int): Unit              = js.native
  def adjustSize(newWidth: Int, newHeight: Int): Unit = js.native
  def getText(): String                               = js.native
  def setText(txt: String): Unit                      = js.native
  def getLinePosition(): Double                       = js.native

  /** The selected text, or an empty string when nothing is selected */
  def getSelectedText(): String        = js.native
  def getColumnPosition(): Double      = js.native
  def enableWordWrap(): Unit           = js.native
  def setDarkMode(dark: Boolean): Unit = js.native
  def setLanguage(lang: String): Unit  = js.native
  def focus(): Unit                    = js.native
  def revealLine(line: Int): Unit      = js.native

  /** Size the editor to its text, between `min` and `max` pixels (for notebook cells) */
  def enableAutoHeight(min: Int, max: Int): Unit = js.native

  /**
    * Show a marker in the glyph margin of lines: each element has `line` (1-origin), `kind` (CSS
    * class `wv-line-marker--<kind>`), and `message` (shown on hover). An empty array clears them.
    */
  def setLineMarkers(markers: js.Array[js.Dynamic]): Unit = js.native

end MonacoEditor

/** Static members of the MonacoEditor class */
@js.native
@JSGlobal("MonacoEditor")
object MonacoEditor extends js.Object:
  /**
    * Register hover and completion for the `wvlet` language on every editor of the page (only the
    * first call takes effect). Both callbacks get the editor text and a 0-origin offset: `hover`
    * returns `{content, startLine, startColumn, endLine, endColumn}` (markdown, 1-origin range) or
    * null, and `complete` returns an array of `{label, kind, detail}` with LSP item kinds.
    */
  def registerLanguageServices(
      hover: js.Function2[String, Int, js.Any],
      complete: js.Function2[String, Int, js.Array[js.Dynamic]]
  ): Unit = js.native

end MonacoEditor
