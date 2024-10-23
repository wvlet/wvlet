package wvlet.lang.ui.component

import org.scalajs.dom
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*

class URLParamManager extends RxElement:
  init()

  private def init(): Unit =
    // Read URL parameters and initialize global states
    val urlQuery = dom.window.location.hash.stripPrefix("#")
    urlQuery
      .split("&")
      .foreach { q =>
        val kv = q.split("=")
        if kv.length == 2 then
          kv(0) match
            case "path" =>
              GlobalState.selectedPath := kv(1)
      }

  override def render: RxElement = div(
    GlobalState
      .selectedPath
      .map { path =>
        if path.isEmpty then
          dom.window.location.hash = "#"
        else
          val urlHash = s"#path=${path}"
          dom.window.location.hash = urlHash
        // dummy element
        span()
      }
  )
