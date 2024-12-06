package wvlet.lang.ui.component.editor

import wvlet.airframe.rx.{Rx, RxVar}
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.lang.BuildInfo
import wvlet.lang.ui.component.{MainFrame, WindowSize}
import wvlet.log.LogTimestampFormatter

object ConsoleLog:

  private def currentTimeStamp(): RxElement = span(
    cls -> "text-blue-300",
    LogTimestampFormatter.formatTimestamp(System.currentTimeMillis())
  )

  private var logBuffer: List[RxElement] = List(
    span(currentTimeStamp(), " ", span(s"wvlet version:${BuildInfo.version}"))
  )

  val logMessages: RxVar[List[RxElement]] = Rx.variable(logBuffer)
  private val bufferSize                  = 1000

  def write(log: String): Unit =
    logBuffer = span(currentTimeStamp(), " ", span(log)) :: logBuffer.take(bufferSize - 1)
    logMessages := logBuffer.reverse

  def writeError(log: String): Unit =
    logBuffer =
      span(currentTimeStamp(), " ", span(cls -> "text-red-300", log)) ::
        logBuffer.take(bufferSize - 1)
    logMessages := logBuffer.reverse

class ConsoleLogWindow extends RxElement:

  // fixed length buffer for log messages
  private val logBuffer = List.empty

  override def render = div(
    pre(
      id  -> "console-log",
      cls -> "text-xs text-slate-300 dark:text-white scrollbar-hidden",
      ConsoleLog
        .logMessages
        .map { logBuffer =>
          logBuffer.map { log =>
            code(log, "\n")
          }
        }
        .tap { _ =>
          // Scroll to the bottom after rendering new log messages
          // TODO Airframe RxElement should support componentDidUpdate hook
          Rx.delay(50, java.util.concurrent.TimeUnit.MILLISECONDS)
            .run { _ =>
              val elem = org.scalajs.dom.document.getElementById("console-log")
              Option(elem).foreach(_.scrollIntoView(false))
            }
        }
    )
  )

end ConsoleLogWindow
