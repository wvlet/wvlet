package wvlet.lang.engine.exec

import wvlet.uni.test.UniTest

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class ThreadPoolRunnerLoadTest extends UniTest:

  private def eventually(cond: => Boolean): Unit =
    val deadline = System.currentTimeMillis() + 10000
    while !cond && System.currentTimeMillis() < deadline do
      Thread.sleep(10)
    cond shouldBe true

  private def checkLoad(runner: ThreadPoolRunner, submit: (() => Unit) => Unit): Unit =
    val gate = CountDownLatch(1)
    // Two tasks hold both threads, a third waits
    (1 to 3).foreach(_ => submit(() => gate.await(10, TimeUnit.SECONDS)))
    eventually(runner.load == TaskRunner.Load(threads = 2, busy = 2, waitingTasks = 1))
    gate.countDown()
    eventually(runner.load == TaskRunner.Load(threads = 2, busy = 0, waitingTasks = 0))

  test("should report an idle runner's threads before any pool starts") {
    val runner = ThreadPoolRunner(3)
    try runner.load shouldBe TaskRunner.Load(threads = 3, busy = 0, waitingTasks = 0)
    finally runner.shutdown()
  }

  test("should count busy threads and waiting tasks of the FIFO pool") {
    val runner = ThreadPoolRunner(2)
    try checkLoad(runner, t => runner.submit(t))
    finally runner.shutdown()
  }

  test("should count busy threads and waiting tasks of the fair pool") {
    val runner = ThreadPoolRunner(2)
    try
      val g = TaskGroup.newGroup()
      checkLoad(runner, t => TaskGroup.withGroup(g)(runner.submit(t)))
    finally
      runner.shutdown()
  }

end ThreadPoolRunnerLoadTest
