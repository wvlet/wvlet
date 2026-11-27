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

/**
  * Runs the REPL's queries so that Ctrl-C cancels just the query: on a thread on the JVM and
  * Native, and on the caller on Node.js, where a Ctrl-C watcher thread raises the cooperative
  * [[wvlet.lang.compiler.QueryCancel]] flag while the query blocks the event loop
  */
private[cli] trait REPLQueries extends AutoCloseable:
  /**
    * Run a query. Returns None when Ctrl-C cancelled it
    */
  def run[A](terminal: REPLTerminal)(body: => A): Option[A]

  /**
    * Run a query, then show its result. Returns None when Ctrl-C cancelled the query. Node.js
    * cancels only during `query`, so Ctrl-C in the pager never counts as a cancel request there
    */
  def runThen[A, B](terminal: REPLTerminal)(query: => A)(show: A => B): Option[B] =
    run(terminal)(show(query))

/**
  * The REPL's platform hooks: how queries run, how the read loop repeats, and how the REPL ends.
  * [[REPLCompatImpl]] implements them for the JVM and Native, and for Node.js
  */
private[cli] trait REPLCompat:
  /** A new runner for the REPL's queries */
  def newQueries(): REPLQueries

  /** Run `step` until it returns false, keeping the stack flat however many steps run */
  def loop(step: () => Rx[Boolean]): Rx[Unit]

  /**
    * Run the REPL to its end, then clean up: before returning on the JVM and Native, and when the
    * REPL ends on Node.js, where errors are reported and set the exit code
    */
  def runToEnd(repl: Rx[Unit])(cleanup: => Unit): Unit

private[cli] object REPLCompat extends REPLCompatImpl
