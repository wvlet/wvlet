package wvlet.lang.ui.component.monaco

import org.scalajs.dom
import org.scalajs.dom.HTMLElement
import org.scalajs.dom.ResizeObserver
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.airframe.rx.Cancelable
import wvlet.airframe.rx.Rx
import wvlet.airframe.rx.RxVar
import wvlet.lang.api.LinePosition
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.Symbol
import wvlet.lang.ui.component.MainFrame
import wvlet.lang.ui.component.WindowSize
import wvlet.lang.ui.component.MainFrame.NavBar
import wvlet.log.LogSupport

import java.util.concurrent.TimeUnit
import org.scalajs.dom
import wvlet.lang.ui.WvletUIMain

import scala.scalajs.js
import scala.scalajs.js.annotation.JSGlobal

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
  def hello(): Unit                                   = js.native
  def render(): Unit                                  = js.native
  def setReadOnly(): Unit                             = js.native
  def adjustHeight(newHeight: Int): Unit              = js.native
  def adjustSize(newWidth: Int, newHeight: Int): Unit = js.native
  def getText(): String                               = js.native
  def setText(txt: String): Unit                      = js.native
  def getLinePosition(): Double                       = js.native
  def getColumnPosition(): Double                     = js.native
  def enableWordWrap(): Unit                          = js.native

abstract class EditorBase(
    windowSize: WindowSize,
    editorId: String,
    lang: String,
    marginHeight: Int = MainFrame.navBarHeightPx
) extends RxElement:
  protected def initialText: String

  protected val editor = new MonacoEditor(editorId, lang, initialText, action = action)

  protected def currentLinePosition(): LinePosition = LinePosition(
    editor.getLinePosition().toInt,
    editor.getColumnPosition().toInt
  )

  protected def action: String => Unit = cmd => logger.info(s"Action: ${cmd}")

  private var c = Cancelable.empty

  override def onMount(node: Any): Unit =
    editor.render()
    c = windowSize
      .getSize
      .map { (w, h) =>
        editor.adjustHeight(h - marginHeight)
      }
      .subscribe()

  override def beforeUnmount: Unit = c.cancel
  override def render: RxElement   = div(cls -> "h-full", id -> editor.id)

  def getText: String            = editor.getText()
  def setText(txt: String): Unit = editor.setText(txt)

end EditorBase
