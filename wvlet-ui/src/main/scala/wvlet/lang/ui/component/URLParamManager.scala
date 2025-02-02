package wvlet.lang.ui.component

import org.scalajs.dom
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.lang.ui.component.GlobalState.{Page, SelectedPage}

import scala.collection.immutable.ListMap

class URLParamManager extends RxElement:
  initFromURL()

  private def initFromURL(): Unit =
    // Read URL parameters and initialize global states
    val urlQuery = dom.window.location.hash.stripPrefix("#")
    // Split query parameters after `?`
    val queryDelimPos = urlQuery.indexOf('?')
    val pageName =
      if queryDelimPos >= 0 then
        urlQuery.substring(0, queryDelimPos)
      else
        urlQuery

    val params =
      if queryDelimPos >= 0 then
        urlQuery.substring(queryDelimPos + 1)
      else
        ""

    val queryMap = List.newBuilder[(String, String)]
    params
      .split("&")
      .toSeq
      .map { q =>
        val kv = q.split("=")
        if kv.length == 2 then
          queryMap += kv(0) -> kv(1)
      }

    val p = Page.values.find(_.path == pageName).getOrElse(Page.Editor)
    GlobalState.selectedPage := SelectedPage(p, queryMap.result())

  end initFromURL

  override def render: RxElement = div(
    GlobalState
      .selectedPage
      .map { p =>
        val hash = List.newBuilder[String]
        hash += s"#${p.page.path}"
        if p.params.nonEmpty then
          hash += "?"
          hash +=
            p.params
              .map { case (k, v) =>
                s"$k=$v"
              }
              .mkString("&")

        dom.window.location.hash = hash.result().mkString
        // return a dummy element
        span()
      }
  )

end URLParamManager
