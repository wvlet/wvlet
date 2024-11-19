package wvlet.lang.native

import scala.scalanative.unsafe.*

object WvcLib:

  @exported("wvlet_compile_main")
  def compile_main(): Unit = WvcMain.main(Array.empty)
