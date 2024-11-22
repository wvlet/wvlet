package wvlet.lang.ui.playground

import org.scalajs.dom
import org.scalajs.dom.ResizeObserver
import wvlet.airframe.rx.{Cancelable, Rx, RxVar}
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.{CompilationUnit, Compiler, Symbol}
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.ui.component.MainFrame
import wvlet.lang.ui.component.MainFrame.NavBar
import wvlet.log.LogSupport

import java.util.concurrent.TimeUnit
import scala.scalajs.js
import scala.scalajs.js.annotation.JSGlobal

@js.native
@JSGlobal("MonacoEditor")
class MonacoEditor(val id: String, lang: String, initialText: String) extends js.Object:
  def hello(): Unit                      = js.native
  def render(): Unit                     = js.native
  def setReadOnly(): Unit                = js.native
  def adjustHeight(newHeight: Int): Unit = js.native
  def getText(): String                  = js.native
  def setText(txt: String): Unit         = js.native

abstract class EditorBase(windowSize: WindowSize, editorId: String, lang: String) extends RxElement:
  protected def initialText: String

  protected val editor = new MonacoEditor(editorId, lang, initialText)
  private var c        = Cancelable.empty

  override def onMount: Unit =
    editor.render()
    c = windowSize
      .getInnerHeight
      .map { h =>
        editor.adjustHeight(
          h - PlaygroundUI.previewWindowHeightPx - PlaygroundUI.editorTabHeight -
            MainFrame.navBarHeightPx
        )
      }
      .subscribe()

  override def beforeUnmount: Unit = c.cancel
  override def render: RxElement   = div(cls -> "h-full", id -> editor.id)

  def getText: String            = editor.getText()
  def setText(txt: String): Unit = editor.setText(txt)

class QueryEditor(currentQuery: CurrentQuery, windowSize: WindowSize)
    extends EditorBase(windowSize, "wvlet-editor", "wvlet"):
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

class SQLPreview(currentQuery: CurrentQuery, windowSize: WindowSize)
    extends EditorBase(windowSize, "wvlet-sql-preview", "sql"):
  override def initialText: String = "select * from lineitem\nlimit 10"

  private var monitor = Cancelable.empty

  private val compiler = Compiler.default(".")

  override def onMount: Unit =
    super.onMount
    monitor = currentQuery
      .wvletQuery
      .map { newWvletQuery =>
        val unit = CompilationUnit.fromString(newWvletQuery)
        try
          val compileResult = compiler.compileSingleUnit(unit)
          if !compileResult.hasFailures then
            val ctx = compileResult
              .context
              .withCompilationUnit(unit)
              .withDebugRun(false)
              .newContext(Symbol.NoSymbol)
            val sql = GenSQL.generateSQL(unit, ctx)
            setText(sql)
        catch
          case e: WvletLangException =>
          // Ignore compilation errors
      }
      .subscribe()

  override def beforeUnmount: Unit =
    super.beforeUnmount
    monitor.cancel

end SQLPreview
