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
package wvlet.lang.flow

import wvlet.lang.api.v1.flow.StageState

import scala.concurrent.Future

/**
  * A single result row returned from the host's SQL engine.
  */
case class Row(values: Map[String, Any]):
  def apply(column: String): Any       = values(column)
  def get(column: String): Option[Any] = values.get(column)

/**
  * An agent invocation extracted from a stage body that calls the `agent(...)` function.
  *
  * The scheduler does not interpret the invocation beyond its name and arguments; the host decides
  * how to map it onto its own agent runtime (skill, model, tools, ...).
  *
  * @param name
  *   The invoked function name (currently always "agent")
  * @param args
  *   Positional argument values rendered as strings
  * @param namedArgs
  *   Named argument values rendered as strings
  */
case class AgentInvocation(
    name: String,
    args: List[String] = Nil,
    namedArgs: Map[String, String] = Map.empty
)

/**
  * The result of an agent invocation, as reported by the host.
  *
  * @param output
  *   The agent's final output (host-defined encoding, typically JSON)
  */
case class AgentResult(output: String)

/**
  * A row appended to the per-run message bus (see the flow-runtime package's bus-schema.sql).
  *
  * @param runId
  *   The flow run this message belongs to
  * @param fromMember
  *   Sender identity, stamped by the runtime — never by the agent
  * @param kind
  *   Message topic (e.g. "stage_state")
  * @param payload
  *   JSON-encoded payload
  * @param correlationId
  *   Optional id threading request/response pairs
  */
case class BusRow(
    runId: String,
    fromMember: String,
    kind: String,
    payload: String,
    correlationId: Option[String] = None
)

/**
  * A snapshot of a flow run, persisted by the host so a run can be inspected or resumed.
  *
  * @param runId
  *   Unique id of this run
  * @param flowName
  *   Name of the flow being run
  * @param stageStates
  *   Current state of every stage
  * @param attempts
  *   Number of started attempts per stage
  * @param steps
  *   Total attempts started in this run (the unit of the max_steps budget)
  */
case class RunState(
    runId: String,
    flowName: String,
    stageStates: Map[String, StageState],
    attempts: Map[String, Int],
    steps: Long
)

/**
  * The host interface of the portable flow scheduler.
  *
  * Wvlet is the logical plan; the host implementing this port is the physical executor (the first
  * target is Treasure Work, an Electron app driving SQLite + its own agent runtime). All methods
  * are Future-based so implementations work on both JVM and Scala.js.
  */
trait FlowEnginePort:
  /** Run a SQL statement on the host's engine and return the result rows. */
  def runSql(sql: String): Future[Seq[Row]]

  /** Invoke an agent on behalf of the given stage and return its result. */
  def invokeAgent(stage: String, spec: AgentInvocation): Future[AgentResult]

  /** Append a message to the per-run bus. */
  def appendBus(row: BusRow): Future[Unit]

  /** Sleep for the given duration (used for retry delays). */
  def sleep(millis: Long): Future[Unit]

  /** Persist a snapshot of the run state. */
  def persistRunState(state: RunState): Future[Unit]
