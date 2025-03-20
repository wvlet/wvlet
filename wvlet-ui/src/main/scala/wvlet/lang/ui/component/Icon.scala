package wvlet.lang.ui.component

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.airframe.rx.html.svgTags.*
import wvlet.airframe.rx.html.svgAttrs.{xmlns as _, style as _, *}

object Icon:

  val DEFAULT_SIZE = "size-4"

  private def baseSvg: RxElement = svg(viewBox -> "0 0 24 24", cls -> "size-6")

  private def iconOutline: RxElement = baseSvg(
    fill        -> "none",
    strokeWidth -> "1.5",
    stroke      -> "currentColor"
  )

  private def iconFilled = baseSvg(fill -> "currentColor")

  def menuBars3: RxElement = iconOutline(
    path(
      strokeLinecap  -> "round",
      strokeLinejoin -> "round",
      d              -> "M3.75 6.75h16.5M3.75 12h16.5m-16.5 5.25h16.5"
    )
  )

  def home: RxElement = svg(
    viewBox -> "0 0 20 20",
    fill    -> "currentColor",
    cls     -> DEFAULT_SIZE,
    path(
      d ->
        "M9.293 2.293a1 1 0 0 1 1.414 0l7 7A1 1 0 0 1 17 11h-1v6a1 1 0 0 1-1 1h-2a1 1 0 0 1-1-1v-3a1 1 0 0 0-1-1H9a1 1 0 0 0-1 1v3a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1v-6H3a1 1 0 0 1-.707-1.707l7-7Z",
      clipRule -> "evenodd"
    )
  )

  def chevron: RxElement = svg(
    viewBox -> "0 0 20 20",
    fill    -> "currentColor",
    cls     -> DEFAULT_SIZE,
    path(
      d ->
        "M8.22 5.22a.75.75 0 0 1 1.06 0l4.25 4.25a.75.75 0 0 1 0 1.06l-4.25 4.25a.75.75 0 0 1-1.06-1.06L11.94 10 8.22 6.28a.75.75 0 0 1 0-1.06Z",
      clipRule -> "evenodd"
    )
  )

  def slash: RxElement = svg(
    viewBox -> "0 0 20 20",
    fill    -> "currentColor",
    cls     -> DEFAULT_SIZE,
    path(d -> "M5.555 17.776l8-16 .894.448-8 16-.894-.448z")
  )

  def folder: RxElement = svg(
    fill        -> "none",
    viewBox     -> "0 0 24 24",
    strokeWidth -> "1.5",
    stroke      -> "currentColor",
    cls         -> DEFAULT_SIZE,
    path(
      strokeLinecap  -> "round",
      strokeLinejoin -> "round",
      d ->
        "M2.25 12.75V12A2.25 2.25 0 0 1 4.5 9.75h15A2.25 2.25 0 0 1 21.75 12v.75m-8.69-6.44-2.12-2.12a1.5 1.5 0 0 0-1.061-.44H4.5A2.25 2.25 0 0 0 2.25 6v12a2.25 2.25 0 0 0 2.25 2.25h15A2.25 2.25 0 0 0 21.75 18V9a2.25 2.25 0 0 0-2.25-2.25h-5.379a1.5 1.5 0 0 1-1.06-.44Z"
    )
  )

  def clip: RxElement = svg(
    viewBox     -> "0 0 24 24",
    fill        -> "none",
    cls         -> DEFAULT_SIZE,
    strokeWidth -> "1.5",
    stroke      -> "currentColor",
    path(
      strokeLinecap  -> "round",
      strokeLinejoin -> "round",
      d ->
        "M15.666 3.888A2.25 2.25 0 0 0 13.5 2.25h-3c-1.03 0-1.9.693-2.166 1.638m7.332 0c.055.194.084.4.084.612v0a.75.75 0 0 1-.75.75H9a.75.75 0 0 1-.75-.75v0c0-.212.03-.418.084-.612m7.332 0c.646.049 1.288.11 1.927.184 1.1.128 1.907 1.077 1.907 2.185V19.5a2.25 2.25 0 0 1-2.25 2.25H6.75A2.25 2.25 0 0 1 4.5 19.5V6.257c0-1.108.806-2.057 1.907-2.185a48.208 48.208 0 0 1 1.927-.184"
    )
  )

end Icon
