/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.cli

import wvlet.lang.cli.terminal.REPLTerminal
import wvlet.uni.log.LogSupport
import wvlet.uni.rx.Rx

import java.util.concurrent.CancellationException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
  * The REPL's platform hooks on the JVM and Native, which have threads and complete terminal reads
  * synchronously
  */
private[cli] trait REPLCompatImpl extends REPLCompat:
  override def newQueries(): REPLQueries = ThreadQueries()

  /**
    * Run `step` until it returns false. Each step blocks with `await` instead of chaining steps
    * with `Rx.flatMap`: the reads complete synchronously here, so a flatMap chain would grow the
    * stack with every input and overflow in a long session.
    */
  override def loop(step: () => Rx[Boolean]): Rx[Unit] =
    while step().await do
      (
    )
    Rx.single(())

  /** Run the REPL to its end, then clean up */
  override def runToEnd(repl: Rx[Unit])(cleanup: => Unit): Unit =
    try repl.await
    finally cleanup

end REPLCompatImpl

/**
  * Runs each query on a daemon thread, so that Ctrl-C cancels just the query
  */
private[cli] class ThreadQueries extends REPLQueries with LogSupport:
  private val executor: ExecutorService = Executors.newCachedThreadPool(
    new ThreadFactory:
      override def newThread(r: Runnable): Thread =
        val t = Thread(r, "wvlet-repl-executor")
        t.setDaemon(true)
        t
  )

  override def run[A](terminal: REPLTerminal)(body: => A): Option[A] =
    // Register the handler before submitting, so that an early Ctrl-C is not lost
    val cancelled = AtomicBoolean(false)
    val task      = AtomicReference[Future[A]]()
    val onCtrlC   = terminal.onInterrupt { () =>
      cancelled.set(true)
      Option(task.get()).foreach(_.cancel(true))
    }
    try
      task.set(executor.submit(() => body))
      if cancelled.get() then
        task.get().cancel(true)
      Some(task.get().get())
    catch
      case e: CancellationException =>
        logger.error("Cancelled the query")
        None
    finally
      onCtrlC.cancel

  override def close(): Unit = executor.shutdown()

end ThreadQueries
