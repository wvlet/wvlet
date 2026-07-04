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
package wvlet.lang.runner

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.WorkEnv

import java.nio.file.Path

/**
  * Persistent store of flow run records, observable across processes so that `wvlet flow session`
  * commands and schedulers can inspect, cancel, and resume runs.
  *
  * Two implementations exist: [[FlowRunRegistry]] (one JSON file per run, no dependencies) and
  * [[SQLiteFlowRunStore]] (a single SQLite database, transactional across processes). The file
  * store is the default; select the SQLite store with `--run-store sqlite` or the
  * `WVLET_FLOW_STORE` environment variable
  */
trait FlowRunStore extends AutoCloseable:
  /** Persist (create or overwrite) the record of a run */
  def save(record: FlowRunRecord): Unit

  /** Read the record of a specific run */
  def get(runId: String): Option[FlowRunRecord]

  /** List all recorded runs, most recent first */
  def list(): List[FlowRunRecord]

  /** The most recent run of the given flow, if any */
  def latestRunOf(flowName: String): Option[FlowRunRecord] = list().find(_.flowName == flowName)

  /**
    * Atomically save the given (running) record if fewer than `concurrencyLimit` runs of the same
    * flow are currently in the running state. Returns true when the slot was claimed and the record
    * was saved. Schedulers use this to enforce flow-level `concurrency:` limits
    */
  def claimRunSlot(record: FlowRunRecord, concurrencyLimit: Int): Boolean

  /**
    * Request cancellation of a run. The marker is polled by the executor's event loop, so a run can
    * be cancelled from another process (e.g. `wvlet flow session cancel`)
    */
  def requestCancel(runId: String): Unit

  /** True if cancellation of the given run has been requested */
  def cancelRequested(runId: String): Boolean

  /** Remove the cancellation marker of a run (called when the run reaches a terminal state) */
  def clearCancelRequest(runId: String): Unit

  /** Delete the record and any cancellation marker of a run */
  def delete(runId: String): Unit

  override def close(): Unit = ()

end FlowRunStore

object FlowRunStore:
  val STORE_TYPE_FILE   = "file"
  val STORE_TYPE_SQLITE = "sqlite"

  /** The environment variable overriding the default run store type */
  val STORE_TYPE_ENV = "WVLET_FLOW_STORE"

  /**
    * Create the run store of the given work environment. The store type defaults to the file store
    * and can be overridden with the WVLET_FLOW_STORE environment variable
    */
  def forWorkEnv(workEnv: WorkEnv): FlowRunStore = ofType(
    sys.env.getOrElse(STORE_TYPE_ENV, STORE_TYPE_FILE),
    workEnv
  )

  /** Create a run store of the given type (file or sqlite) under the work environment */
  def ofType(storeType: String, workEnv: WorkEnv): FlowRunStore =
    val dir = Path.of(workEnv.targetFolder, "flow-runs")
    storeType match
      case STORE_TYPE_FILE =>
        FlowRunRegistry(dir)
      case STORE_TYPE_SQLITE =>
        SQLiteFlowRunStore(dir.resolve("registry.db"))
      case other =>
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Unknown flow run store type: ${other}. Use file or sqlite")

end FlowRunStore
