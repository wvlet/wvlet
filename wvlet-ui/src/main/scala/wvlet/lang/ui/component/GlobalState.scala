package wvlet.lang.ui.component

import wvlet.airframe.rx.Rx
import wvlet.airframe.rx.RxVar

object GlobalState:
  var selectedPath: RxVar[String] = Rx.variable("")
