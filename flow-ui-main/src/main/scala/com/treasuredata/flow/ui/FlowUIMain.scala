package com.treasuredata.flow.ui

import com.treasuredata.flow.ui.component.MainFrame
import wvlet.airframe.rx.html.all.*

object FlowUIMain:

  def main(args: Array[String]): Unit =
    render

  def render: Unit = new MainFrame().renderTo("main")
