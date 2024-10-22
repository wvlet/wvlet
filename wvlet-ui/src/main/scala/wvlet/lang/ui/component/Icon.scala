package wvlet.lang.ui.component

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.airframe.rx.html.svgTags.*
import wvlet.airframe.rx.html.svgAttrs.{xmlns as _, style as _, *}

object Icon:

  val DEFAULT_SIZE = 4

  def home: RxElement = svg(
    viewBox -> "0 0 20 20",
    fill    -> "currentColor",
    cls     -> s"size-${DEFAULT_SIZE}",
    path(
      d ->
        "M9.293 2.293a1 1 0 0 1 1.414 0l7 7A1 1 0 0 1 17 11h-1v6a1 1 0 0 1-1 1h-2a1 1 0 0 1-1-1v-3a1 1 0 0 0-1-1H9a1 1 0 0 0-1 1v3a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1v-6H3a1 1 0 0 1-.707-1.707l7-7Z",
      clipRule -> "evenodd"
    )
  )

  def chevron: RxElement = svg(
    viewBox -> "0 0 20 20",
    fill    -> "currentColor",
    cls     -> s"size-${DEFAULT_SIZE}",
    path(
      d ->
        "M8.22 5.22a.75.75 0 0 1 1.06 0l4.25 4.25a.75.75 0 0 1 0 1.06l-4.25 4.25a.75.75 0 0 1-1.06-1.06L11.94 10 8.22 6.28a.75.75 0 0 1 0-1.06Z",
      clipRule -> "evenodd"
    )
  )

  def slash: RxElement = svg(
    viewBox -> "0 0 20 20",
    fill    -> "currentColor",
    cls     -> s"size-${DEFAULT_SIZE}",
    path(d -> "M5.555 17.776l8-16 .894.448-8 16-.894-.448z")
  )

end Icon
