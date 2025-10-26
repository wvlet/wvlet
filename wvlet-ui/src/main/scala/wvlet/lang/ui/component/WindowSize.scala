package wvlet.lang.ui.component

import org.scalajs.dom
import org.scalajs.dom.ResizeObserver
import wvlet.airframe.rx.Rx
import wvlet.airframe.rx.RxVar
import wvlet.log.LogSupport

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

  def getInnerWidth: Rx[Int]  = innerWidth
  def getInnerHeight: Rx[Int] = innerHeight
  def getSize: Rx[(Int, Int)] = innerWidth.zip(innerHeight)

  override def close(): Unit =
    // dom.window.onresize = null
    observer.disconnect()
