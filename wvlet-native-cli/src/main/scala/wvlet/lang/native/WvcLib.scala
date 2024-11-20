package wvlet.lang.native

import scala.scalanative.unsafe.*

object WvcLib:

  @exported("wvlet_compile_main")
  def compile_main(): Int =
    WvcMain.main(Array.empty)
    0
