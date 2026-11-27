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
import wvlet.lang.compiler.QueryCancel
import wvlet.uni.log.LogSupport
import wvlet.uni.rx.Rx

import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.scalajs.js
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

/**
  * The REPL's platform hooks on Node.js, where terminal reads complete asynchronously and every
  * query runs synchronously on the event loop, so Ctrl-C reaches a running query through
  * [[CtrlCWatcher]]
  */
private[cli] trait REPLCompatImpl extends REPLCompat:
  private given ExecutionContext = scala.scalajs.concurrent.JSExecutionContext.queue

  override def newQueries(): REPLQueries = WatchedQueries(CtrlCWatcher.start())

  /**
    * Run `step` until it returns false. `Rx.await` is unavailable here, so steps are chained
    * through callbacks. Each step starts on a later event-loop turn, which keeps the stack flat
    * even when a read completes synchronously (e.g., after stdin ended)
    */
  override def loop(step: () => Rx[Boolean]): Rx[Unit] =
    val done         = Promise[Unit]()
    def next(): Unit = step()
      .transform(identity)
      .run {
        case Success(true) =>
          js.timers.setTimeout(0)(next())
        case Success(false) =>
          done.trySuccess(())
        case Failure(e) =>
          done.tryFailure(e)
      }
    next()
    Rx.future(done.future)

  /**
    * Clean up when the REPL ends. The command returns before that, so errors are reported here and
    * set the exit code
    */
  override def runToEnd(repl: Rx[Unit])(cleanup: => Unit): Unit = repl
    .transform(identity)
    .run { result =>
      try cleanup
      finally
        result match
          case Failure(e) if !WvletCliCommands.isSuccessfulExit(e) =>
            Console.err.println(e.getMessage)
            js.Dynamic.global.process.exitCode = 1
          case _ =>
    }

end REPLCompatImpl

/**
  * Runs each query on the caller. While it runs, the [[CtrlCWatcher]] turns Ctrl-C into a
  * [[QueryCancel]] request, which the engine, DuckDB, and Trino check, so the query stops and the
  * session (saved tables, `use`, the DuckDB connection) stays usable. A SIGINT listener keeps Node
  * from exiting meanwhile; it can't run until the query ends, so the watcher does the work.
  *
  * Without a watcher (stdin is not a terminal, or the helper failed to start), no listener is
  * registered, so Ctrl-C exits wvlet as before rather than being swallowed until the query ends.
  */
private[cli] class WatchedQueries(watcher: Option[CtrlCWatcher])
    extends REPLQueries
    with LogSupport:
  override def run[A](terminal: REPLTerminal)(body: => A): Option[A] =
    runThen(terminal)(body)(identity)

  override def runThen[A, B](terminal: REPLTerminal)(query: => A)(show: A => B): Option[B] =
    watcher.filter(_.isReady) match
      case None =>
        Some(show(query))
      case Some(w) =>
        val process  = js.Dynamic.global.process
        val listener = (() => ()): js.Function0[Unit]
        process.on("SIGINT", listener)
        w.arm()
        val result =
          try
            Right(query)
          catch
            case NonFatal(e) =>
              Left(e)
          finally
            process.removeListener("SIGINT", listener)
        if QueryCancel.disarm() then
          logger.error("Cancelled the query")
          None
        else
          result match
            case Right(r) =>
              Some(show(r))
            case Left(e) =>
              throw e

  override def close(): Unit = watcher.foreach(_.close())

end WatchedQueries
