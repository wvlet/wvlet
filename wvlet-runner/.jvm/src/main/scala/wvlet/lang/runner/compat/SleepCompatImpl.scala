package wvlet.lang.runner.compat

private[runner] trait SleepCompatImpl:
  def sleepMillis(millis: Long): Unit = Thread.sleep(millis)
