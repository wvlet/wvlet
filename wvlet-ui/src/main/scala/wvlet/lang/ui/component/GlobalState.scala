package wvlet.lang.ui.component

import wvlet.airframe.rx.{Rx, RxVar}

object GlobalState:
  var selectedPath: RxVar[String] = Rx.variable("")
