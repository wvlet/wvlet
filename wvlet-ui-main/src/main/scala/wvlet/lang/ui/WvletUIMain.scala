package wvlet.lang.ui

import wvlet.airframe.rx.html.all.*
import wvlet.lang.ui.component.MainFrame

object WvletUIMain:

  def main(args: Array[String]): Unit = render

  def render: Unit = new MainFrame().renderTo("main")
