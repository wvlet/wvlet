package wvlet.lang.ui.component

import wvlet.uni.rx.Rx
import wvlet.uni.rx.RxVar

object GlobalState:
  var selectedPath: RxVar[String] = Rx.variable("")
