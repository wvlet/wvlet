package wvlet.lang.runner.compat

import scala.scalajs.js

private[runner] trait SleepCompatImpl:
  /**
    * Node.js has no `Thread.sleep`; block on `Atomics.wait` against a throwaway
    * `SharedArrayBuffer` (the primitive uni's Node sync HTTP channel is built on). Falls back to
    * a busy wait where `SharedArrayBuffer` is unavailable.
    */
  def sleepMillis(millis: Long): Unit =
    try
      val sab = js.Dynamic.newInstance(js.Dynamic.global.SharedArrayBuffer)(4)
      val arr = js.Dynamic.newInstance(js.Dynamic.global.Int32Array)(sab)
      js.Dynamic.global.Atomics.applyDynamic("wait")(arr, 0, 0, millis.toDouble)
      ()
    catch
      case _: Throwable =>
        val deadline = System.currentTimeMillis() + millis
        while System.currentTimeMillis() < deadline do ()
