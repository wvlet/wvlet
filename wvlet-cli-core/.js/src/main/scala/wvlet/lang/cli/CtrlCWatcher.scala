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

import wvlet.lang.compiler.QueryCancel
import wvlet.uni.log.LogSupport

import scala.scalajs.js
import scala.util.control.NonFatal

/**
  * Notices Ctrl-C while a query blocks Node's main thread, and asks the query to stop.
  *
  * Node delivers signals only to the main thread's event loop, which a running query blocks. So a
  * worker thread spawns a helper `node` process in the terminal's process group: Ctrl-C sends it
  * the same SIGINT, and it writes a byte to its pipe. The worker's event loop is free, so it sees
  * the byte at once, raises the [[QueryCancel]] flag, and calls `duckdb_interrupt` on the DuckDB
  * connection the query waits on, if any. A second Ctrl-C during the same query ends wvlet, so a
  * query without a stop point never traps the user.
  *
  * The helper exits when its stdin pipe closes, so it never outlives wvlet.
  */
private[cli] class CtrlCWatcher private (worker: js.Dynamic) extends LogSupport:
  private val atomics = js.Dynamic.global.Atomics
  private val slots = js.Dynamic.newInstance(js.Dynamic.global.Int32Array)(QueryCancel.sharedBuffer)
  // The worker binds duckdb_interrupt itself. A message posted while a query blocks the main
  // thread still arrives: the worker reads it on its own event loop
  QueryCancel.onDuckDBLibraryLoaded { path =>
    worker.postMessage(js.Dynamic.literal(lib = path))
  }

  /** Whether the helper process is running and listens for SIGINT */
  def isReady: Boolean = atomics.load(slots, CtrlCWatcher.ReadySlot).asInstanceOf[Int] == 1

  /** Accept Ctrl-C for the query that is about to run */
  def arm(): Unit = QueryCancel.arm()

  def close(): Unit = worker.terminate()

end CtrlCWatcher

private[cli] object CtrlCWatcher extends LogSupport:
  private[cli] val ReadySlot = 2

  /**
    * Start the watcher when stdin is a terminal, where Ctrl-C sends SIGINT to the process group.
    * Returns None when it can't start, so that Ctrl-C keeps exiting wvlet
    */
  def start(): Option[CtrlCWatcher] =
    val process = js.Dynamic.global.process
    if !js.DynamicImplicits.truthValue(process.stdin.isTTY) then
      None
    else
      try
        val workerThreads = process.getBuiltinModule("node:worker_threads")
        val data          = js
          .Dynamic
          .literal(
            buffer = QueryCancel.sharedBuffer,
            koffi = koffiPath.orNull,
            execPath = process.execPath,
            readySlot = ReadySlot
          )
        val worker =
          js.Dynamic
            .newInstance(workerThreads.Worker)(
              WorkerScript,
              js.Dynamic.literal(eval = true, workerData = data)
            )
        // Neither the worker nor its helper keeps wvlet running once the REPL ends
        worker.unref()
        worker.on("error", (e: js.Any) => logger.debug(s"Ctrl-C watcher failed: ${e}"))
        Some(CtrlCWatcher(worker))
      catch
        case NonFatal(e) =>
          logger.debug(s"Ctrl-C watcher is unavailable: ${e.getMessage}")
          None

  /**
    * Where koffi is, for the worker's `duckdb_interrupt` binding. An eval'd worker resolves modules
    * from the working folder, so resolve koffi next to the wvlet bundle instead
    */
  private def koffiPath: Option[String] =
    try
      val module  = js.Dynamic.global.process.getBuiltinModule("node:module")
      val require = module.createRequire(js.`import`.meta.url)
      Some(require.resolve("koffi").toString)
    catch
      case NonFatal(_) =>
        None

  /** The helper process: report readiness, then a byte per SIGINT, until its stdin closes */
  private val HelperScript =
    "process.on('SIGINT',()=>process.stdout.write('i'));" +
      "process.stdin.on('end',()=>process.exit(0));process.stdin.resume();" +
      "process.stdout.write('r')"

  private val WorkerScript =
    s"""
const { workerData, parentPort } = require('node:worker_threads');
const { spawn } = require('node:child_process');
const slots = new Int32Array(workerData.buffer, 0, 4);
const connection = new BigInt64Array(workerData.buffer, 16, 1);
const READY = workerData.readySlot;
let interrupt = null;
parentPort.on('message', (m) => {
  if (!m.lib || !workerData.koffi) return;
  try {
    const koffi = require(workerData.koffi);
    interrupt = koffi.load(m.lib).func('void duckdb_interrupt(uint64_t connection)');
  } catch (e) {
    interrupt = null;
  }
});
const helper = spawn(workerData.execPath, ['-e', ${js.JSON.stringify(HelperScript)}], {
  stdio: ['pipe', 'pipe', 'ignore']
});
const notReady = () => Atomics.store(slots, READY, 0);
helper.on('error', notReady);
helper.on('exit', notReady);
helper.stdout.on('data', (bytes) => {
  for (const b of bytes) {
    if (b === 0x72) { Atomics.store(slots, READY, 1); continue; }
    // Ctrl-C at the prompt or in the pager: no cancellable query is running
    if (Atomics.load(slots, 1) !== 1) continue;
    if (Atomics.load(slots, 0) === 1) {
      // A second Ctrl-C during the same query: the query has no stop point, so end wvlet
      process.kill(process.pid, 'SIGTERM');
      return;
    }
    Atomics.store(slots, 0, 1);
    Atomics.notify(slots, 0);
    const c = Atomics.load(connection, 0);
    if (c !== 0n && interrupt) {
      try { interrupt(c); } catch (e) {}
    }
  }
});
"""

end CtrlCWatcher
