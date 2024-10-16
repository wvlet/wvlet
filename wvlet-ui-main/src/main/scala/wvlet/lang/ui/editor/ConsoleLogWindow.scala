package wvlet.lang.ui.editor

import wvlet.airframe.rx.{Rx, RxVar}
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.lang.BuildInfo
import wvlet.log.LogTimestampFormatter

object ConsoleLog:

  private def currentTimeStamp(): String = LogTimestampFormatter
    .formatTimestamp(System.currentTimeMillis())

  private var logBuffer: List[String] = List(
    s"${currentTimeStamp()} wvlet version:${BuildInfo.version}"
  )

  val logMessages: RxVar[List[String]] = Rx.variable(logBuffer)
  private val bufferSize               = 1000

  def write(log: String): Unit =
    logBuffer = s"${currentTimeStamp()} ${log}" :: logBuffer.take(bufferSize - 1)
    logMessages := logBuffer.reverse

class ConsoleLogWindow extends RxElement:

  // fixed length buffer for log messages
  private val logBuffer = List.empty

  override def render = div(
    cls -> "overflow-clip",
    pre(
      cls -> "text-xs text-slate-300 dark:text-white",
      ConsoleLog
        .logMessages
        .map { logBuffer =>
          logBuffer.map { log =>
            code(log + "\n")
          }
        }
    )
  )
