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

class QueryEditor(windowSize: WindowSize) extends RxElement:
  private val editor = new MonacoEditor("wvlet-editor", "from lineitem\nlimit 10")

  private var c = Cancelable.empty

  override def onMount: Unit =
    editor.render()
    c = windowSize
      .getInnerHeight
      .map { h =>
        editor.adjustHeight(h - 512 - 44)
      }
      .subscribe()

  override def beforeUnmount: Unit = c.cancel

  override def render: RxElement = div(cls -> "h-full", id -> editor.id)

end QueryEditor

class SQLPreview(windowSize: WindowSize) extends RxElement:
  private val viewer = new MonacoEditor("wvlet-sql-preview", "select * from lineitem\nlimit 10")
  private var c      = Cancelable.empty

  override def onMount: Unit =
    viewer.render()
    c = windowSize
      .getInnerHeight
      .map { h =>
        viewer.adjustHeight(h - 512 - 44)
      }
      .subscribe()

  override def beforeUnmount: Unit = c.cancel

  override def render: RxElement = div(cls -> "h-full", id -> viewer.id)

end SQLPreview

class WindowSize extends AutoCloseable with LogSupport:
  private val innerWidth: RxVar[Int]  = Rx.variable(dom.window.innerWidth.toInt)
  private val innerHeight: RxVar[Int] = Rx.variable(dom.window.innerHeight.toInt)

  private val observer =
    val ob =
      new ResizeObserver(callback =
        (entries, observer) =>
          innerWidth  := dom.window.innerWidth.toInt
          innerHeight := dom.window.innerHeight.toInt
      )
    ob.observe(dom.document.body)
    ob

  def getInnerWidth: Rx[Int] = innerWidth

  def getInnerHeight: Rx[Int] = innerHeight

  override def close(): Unit =
    // dom.window.onresize = null
    observer.disconnect()
