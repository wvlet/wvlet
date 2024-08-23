package wvlet.lang.ui.component

import wvlet.airspec.AirSpec
import wvlet.lang.ui.component.MainFrame

class MainFrameTest extends AirSpec:
  test("sanity test") {
    new MainFrame().render
  }
