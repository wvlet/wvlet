package wvlet.lang.ui.playground

import org.scalajs.dom
import org.scalajs.dom.ResizeObserver
import wvlet.airframe.rx.{Cancelable, Rx, RxVar}
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.log.LogSupport

import java.util.concurrent.TimeUnit
import scala.scalajs.js
import scala.scalajs.js.annotation.JSGlobal

@js.native
@JSGlobal("MonacoEditor")
class MonacoEditor(val id: String, initialText: String) extends js.Object:
  def hello(): Unit                      = js.native
  def render(): Unit                     = js.native
  def setReadOnly(): Unit                = js.native
  def adjustHeight(newHeight: Int): Unit = js.native

abstract class EditorBase(windowSize: WindowSize, editorId: String) extends RxElement:
  protected def initialText: String

  protected val editor = new MonacoEditor(editorId, initialText)
  private var c        = Cancelable.empty

  override def onMount: Unit =
    editor.render()
    c = windowSize
      .getInnerHeight
      .map { h =>
        editor.adjustHeight(h - 512 - 44)
      }
      .subscribe()

  override def beforeUnmount: Unit = c.cancel
  override def render: RxElement   = div(cls -> "h-full", id -> editor.id)

class QueryEditor(windowSize: WindowSize) extends EditorBase(windowSize, "wvlet-editor"):
  override def initialText: String = "from lineitem\nlimit 10"

class SQLPreview(windowSize: WindowSize) extends EditorBase(windowSize, "wvlet-sql-preview"):
  override def initialText: String = "select * from lineitem\nlimit 10"
