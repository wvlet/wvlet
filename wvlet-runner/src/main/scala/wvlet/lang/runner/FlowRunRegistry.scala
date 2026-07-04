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

import wvlet.lang.api.v1.flow.StageState
import wvlet.lang.compiler.WorkEnv
import wvlet.uni.log.LogSupport
import wvlet.uni.weaver.Weaver

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/**
  * The persisted state of a single stage within a flow run
  */
case class StageRunRecord(
    name: String,
    state: String,
    attempts: Int,
    error: Option[String] = None,
    table: Option[String] = None
)

/**
  * The persisted record of a flow run. Records are written on run start, on every stage state
  * change, and on completion, so that `wvlet flow session` commands (and future schedulers) can
  * observe runs across processes
  *
  * @param runId
  *   Unique run identifier (ULID)
  * @param flowName
  *   Name of the executed flow
  * @param state
  *   Flow-level state: running, success, failed, or cancelled
  * @param startedAtMillis
  *   Epoch millis when the run started
  * @param finishedAtMillis
  *   Epoch millis when the run reached a terminal state
  * @param stages
  *   Per-stage records in stage definition order
  */
case class FlowRunRecord(
    runId: String,
    flowName: String,
    state: String,
    startedAtMillis: Long,
    finishedAtMillis: Option[Long] = None,
    stages: List[StageRunRecord] = Nil
):
  def isTerminal: Boolean = state != FlowRunRecord.STATE_RUNNING

object FlowRunRecord:
  val STATE_RUNNING   = "running"
  val STATE_SUCCESS   = "success"
  val STATE_FAILED    = "failed"
  val STATE_CANCELLED = "cancelled"
  // The flow was not started because its cross-flow dependency was not satisfied
  val STATE_SKIPPED = "skipped"

  /** Derive the flow-level state from its stage states */
  def flowStateOf(stageStates: Iterable[StageState]): String =
    if stageStates.exists(_ == StageState.Failed) then
      STATE_FAILED
    else if stageStates.exists(_ == StageState.Cancelled) then
      STATE_CANCELLED
    else if stageStates.forall(_.isTerminal) then
      STATE_SUCCESS
    else
      STATE_RUNNING

/**
  * A file-based registry of flow runs. Each run is stored as a single JSON file
  * (`<registryDir>/<runId>.json`), which keeps reads and writes atomic enough for a local,
  * single-writer-per-run setup without a database dependency
  */
class FlowRunRegistry(registryDir: Path) extends LogSupport:

  private val weaver = Weaver.of[FlowRunRecord]

  private def fileFor(runId: String): Path = registryDir.resolve(s"${runId.toLowerCase}.json")

  private def cancelMarkerFor(runId: String): Path = registryDir.resolve(
    s"${runId.toLowerCase}.cancel"
  )

  /** Persist (create or overwrite) the record of a run */
  def save(record: FlowRunRecord): Unit =
    Files.createDirectories(registryDir)
    Files.writeString(fileFor(record.runId), weaver.toJson(record))

  /** Read the record of a specific run */
  def get(runId: String): Option[FlowRunRecord] =
    val f = fileFor(runId)
    if Files.exists(f) then
      readRecord(f)
    else
      None

  /** List all recorded runs, most recent first */
  def list(): List[FlowRunRecord] =
    if !Files.isDirectory(registryDir) then
      Nil
    else
      Files
        .list(registryDir)
        .iterator()
        .asScala
        .filter(_.getFileName.toString.endsWith(".json"))
        .flatMap(readRecord)
        .toList
        .sortBy(-_.startedAtMillis)

  /** The most recent run of the given flow, if any */
  def latestRunOf(flowName: String): Option[FlowRunRecord] = list().find(_.flowName == flowName)

  /**
    * Request cancellation of a run by placing a marker file next to its record. The marker is
    * polled by the executor's event loop, so a run can be cancelled from another process (e.g.
    * `wvlet flow session cancel`)
    */
  def requestCancel(runId: String): Unit =
    Files.createDirectories(registryDir)
    Files.writeString(cancelMarkerFor(runId), "")

  /** True if cancellation of the given run has been requested */
  def cancelRequested(runId: String): Boolean = Files.exists(cancelMarkerFor(runId))

  /** Remove the cancellation marker of a run (called when the run reaches a terminal state) */
  def clearCancelRequest(runId: String): Unit = Files.deleteIfExists(cancelMarkerFor(runId))

  /** Delete the record and any cancellation marker of a run */
  def delete(runId: String): Unit =
    Files.deleteIfExists(fileFor(runId))
    Files.deleteIfExists(cancelMarkerFor(runId))

  private def readRecord(f: Path): Option[FlowRunRecord] =
    try
      Some(weaver.fromJson(Files.readString(f, StandardCharsets.UTF_8)))
    catch
      case NonFatal(e) =>
        warn(s"Failed to read flow run record ${f}: ${e.getMessage}")
        None

end FlowRunRegistry

object FlowRunRegistry:
  /** The registry folder of the given work environment */
  def forWorkEnv(workEnv: WorkEnv): FlowRunRegistry = FlowRunRegistry(
    Path.of(workEnv.targetFolder, "flow-runs")
  )
