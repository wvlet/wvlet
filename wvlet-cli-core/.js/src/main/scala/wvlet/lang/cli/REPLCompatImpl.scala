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
import wvlet.uni.rx.Rx

import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.scalajs.js
import scala.util.Failure
import scala.util.Success

/**
  * The REPL's platform hooks on Node.js, where terminal reads complete asynchronously and every
  * query runs synchronously on the event loop
  */
private[cli] trait REPLCompatImpl extends REPLCompat:
  private given ExecutionContext = scala.scalajs.concurrent.JSExecutionContext.queue

  override def newQueries(): REPLQueries = InlineQueries()

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
  * Runs each query on the caller. A blocked event loop can't run a SIGINT listener, so no listener
  * is registered: Ctrl-C during a query exits wvlet, as it does for `wvlet run`
  */
private[cli] class InlineQueries extends REPLQueries:
  override def run[A](terminal: REPLTerminal)(body: => A): Option[A] = Some(body)
  override def close(): Unit                                         = ()
