package wvlet.lang.api.server

import wvlet.airframe.launcher.Launcher

object WvletServer:
  def main(args: Array[String]): Unit =
    Launcher.of[WvletServer].execute(args)

class WvletServer:
  def version:
