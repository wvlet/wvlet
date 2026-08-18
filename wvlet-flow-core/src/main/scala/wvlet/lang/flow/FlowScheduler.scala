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
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success as TrySuccess

/**
  * Configuration of the portable flow scheduler.
  *
  * @param maxSteps
  *   Total step budget of a run: the maximum number of stage attempts the scheduler may start.
  *   Overrides a `max_steps:` item in the flow's config block when set.
  * @param defaultRetryDelayMillis
  *   Retry delay applied when a stage has `retries:` but no `retry_delay:` config
  * @param schedulerMemberName
  *   The `from_member` identity the scheduler stamps on bus messages
  */
case class FlowSchedulerConfig(
    maxSteps: Option[Long] = None,
    defaultRetryDelayMillis: Long = 0L,
    schedulerMemberName: String = "scheduler"
):
  def withMaxSteps(maxSteps: Long): FlowSchedulerConfig = copy(maxSteps = Some(maxSteps))
  def noMaxSteps(): FlowSchedulerConfig                 = copy(maxSteps = None)
  def withDefaultRetryDelayMillis(millis: Long): FlowSchedulerConfig = copy(
    defaultRetryDelayMillis = millis
  )

  def withSchedulerMemberName(name: String): FlowSchedulerConfig = copy(schedulerMemberName = name)

/**
  * The outcome of a flow run: the terminal state of every stage plus attempt/step counters.
  */
case class FlowRunResult(
    runId: String,
    flowName: String,
    stageStates: Map[String, StageState],
    attempts: Map[String, Int],
    steps: Long,
    errors: Map[String, String]
):
  /** True when every stage ended in success or was legitimately skipped */
  def isSuccess: Boolean = stageStates
    .values
    .forall(s => s == StageState.Success || s == StageState.Skipped)

/**
  * A portable scheduler skeleton for wvlet flow definitions.
  *
  * This runs the stage DAG of a [[wvlet.lang.model.plan.FlowDef]] against a host-provided
  * [[FlowEnginePort]]: dependency edges come from stage `from`/`depends on`/`merge` references,
  * stage states follow the documented stage execution model, triggers (`if x.failed`, `if x.done`,
  * and/or combinations) are evaluated on upstream terminal states, `retries:` is honored per stage,
  * and a total-step budget (max_steps) bounds the run.
  *
  * Stage bodies are treated as opaque units executed by the port: a body containing an `agent(...)`
  * call is dispatched to [[FlowEnginePort.invokeAgent]]; any other body is dispatched to
  * [[FlowEnginePort.runSql]] (with a placeholder statement — SQL lowering is not part of this
  * skeleton). Route/fork/wait/activate lowering is intentionally not implemented.
  *
  * Execution is sequential for now, but stage selection is data-driven (any stage whose
  * dependencies are terminal is eligible), so independent stages could run concurrently later.
  */
class FlowScheduler(port: FlowEnginePort, config: FlowSchedulerConfig = FlowSchedulerConfig()):
  /** Run the given flow and return the terminal state of every stage */
  def run(flow: FlowDef, runId: String)(using ExecutionContext): Future[FlowRunResult] = FlowRun(
    flow,
    runId,
    port,
    config
  ).run()

object FlowScheduler:
  /**
    * The placeholder statement dispatched to [[FlowEnginePort.runSql]] for non-agent stage bodies.
    * SQL lowering of stage bodies is the host compiler's job, not this skeleton's; the stage name
    * is embedded so ports and tests can attribute the call.
    */
  def placeholderSql(flowName: String, stageName: String): String =
    s"select 1 -- flow ${flowName} stage ${stageName}: SQL lowering not implemented in flow-core"

private class FlowRun(
    flow: FlowDef,
    runId: String,
    port: FlowEnginePort,
    config: FlowSchedulerConfig
)(using ExecutionContext):
  import StageState.*

  private val stages                          = flow.stages
  private val stageNames                      = stages.map(_.name.name).toSet
  private var states: Map[String, StageState] = stages.map(s => s.name.name -> Pending).toMap
  private var attempts: Map[String, Int]      = Map.empty.withDefaultValue(0)
  private var errors: Map[String, String]     = Map.empty
  private var steps: Long                     = 0L
  private val maxSteps: Option[Long]          = config.maxSteps.orElse(flowConfigMaxSteps)

  def run(): Future[FlowRunResult] = persist()
    .flatMap(_ => step())
    .map(_ =>
      FlowRunResult(
        runId = runId,
        flowName = flow.name.name,
        stageStates = states,
        attempts = stages.map(s => s.name.name -> attempts(s.name.name)).toMap,
        steps = steps,
        errors = errors
      )
    )

  /** Process ready stages one at a time until every stage is terminal or none can make progress */
  private def step(): Future[Unit] =
    val nextReady = stages.find(s =>
      states(s.name.name) == Pending && dependencies(s).forall(isTerminalState)
    )
    nextReady match
      case Some(stage) =>
        processStage(stage).flatMap(_ => step())
      case None =>
        // Any stage still pending here has an unsatisfiable dependency (e.g. a reference cycle):
        // cancel it rather than looping forever
        val stuck = stages.filter(s => states(s.name.name) == Pending)
        stuck.foldLeft(Future.unit) { (f, s) =>
          f.flatMap { _ =>
            errors += s.name.name -> "unsatisfiable dependency (cycle or unresolved reference)"
            transition(s.name.name, Cancelled)
          }
        }

  private def isTerminalState(stageName: String): Boolean = states(stageName).isTerminal

  /**
    * The stage names this stage waits for: `from` refs, `depends on` refs, `merge` sources, and any
    * stage referenced by its trigger condition. References that are not stages of this flow (e.g.
    * tables or files) impose no dependency.
    */
  private def dependencies(stage: StageDef): List[String] =
    val mergeSources = List.newBuilder[String]
    stage
      .body
      .foreach(
        _.traverse { case m: FlowMerge =>
          mergeSources ++= m.sources.map(_.fullName)
        }
      )
    val refs =
      stage.inputRefs.map(_.fullName) ++ stage.dependsOn.map(_.fullName) ++ mergeSources.result() ++
        stage.trigger.map(triggerRefs).getOrElse(Nil)
    refs.distinct.filter(stageNames.contains)

  private def triggerRefs(trigger: StageTrigger): List[String] =
    trigger match
      case StatePredicate(stageName, _, _) =>
        List(stageName.fullName)
      case TriggerAnd(l, r, _) =>
        triggerRefs(l) ++ triggerRefs(r)
      case TriggerOr(l, r, _) =>
        triggerRefs(l) ++ triggerRefs(r)

  /**
    * Decide whether a ready stage should run. Success dependencies (`from`, `depends on`, merge
    * sources) require upstream success; an explicit trigger is evaluated on upstream terminal
    * states. This mirrors the trigger-evaluation table of the stage execution model.
    */
  private def shouldRun(stage: StageDef): Boolean =
    val successDeps = dependencies(stage).diff(stage.trigger.map(triggerRefs).getOrElse(Nil))
    successDeps.forall(d => states(d) == Success) && stage.trigger.forall(evalTrigger)

  private def evalTrigger(trigger: StageTrigger): Boolean =
    trigger match
      case StatePredicate(stageName, state, _) =>
        val upstream = states.getOrElse(stageName.fullName, Pending)
        state match
          case "failed" =>
            upstream == Failed
          case "done" =>
            upstream.isTerminal
          case "success" =>
            upstream == Success
          case _ =>
            false
      case TriggerAnd(l, r, _) =>
        evalTrigger(l) && evalTrigger(r)
      case TriggerOr(l, r, _) =>
        evalTrigger(l) || evalTrigger(r)

  private def processStage(stage: StageDef): Future[Unit] =
    val name = stage.name.name
    if !shouldRun(stage) then
      transition(name, Skipped)
    else if budgetExhausted then
      errors += name -> s"max_steps budget (${maxSteps.getOrElse(0L)}) exhausted"
      transition(name, Cancelled)
    else
      attempt(stage, attemptCount = 1, maxAttempts = stageRetries(stage) + 1)

  private def budgetExhausted: Boolean = maxSteps.exists(steps >= _)

  private def attempt(stage: StageDef, attemptCount: Int, maxAttempts: Int): Future[Unit] =
    val name = stage.name.name
    steps += 1
    attempts += name -> attemptCount
    states += name   -> Running
    execute(stage).transformWith {
      case TrySuccess(_) =>
        transition(name, Success)
      case Failure(e) =>
        errors += name -> Option(e.getMessage).getOrElse(e.toString)
        if attemptCount < maxAttempts && !budgetExhausted then
          states += name -> Retrying
          port
            .sleep(stageRetryDelayMillis(stage))
            .flatMap(_ => attempt(stage, attemptCount + 1, maxAttempts))
        else
          transition(name, Failed)
    }

  /**
    * Execute a stage body as an opaque unit via the port. A body calling `agent(...)` becomes an
    * agent invocation; any other body is delegated to the host's SQL engine with a placeholder
    * statement. A body-less stage is a control-only barrier and succeeds immediately.
    */
  private def execute(stage: StageDef): Future[?] =
    stage.body match
      case None =>
        Future.unit
      case Some(body) =>
        findAgentCall(body) match
          case Some(invocation) =>
            port.invokeAgent(stage.name.name, invocation)
          case None =>
            port.runSql(FlowScheduler.placeholderSql(flow.name.name, stage.name.name))

  /**
    * Find the first `agent(...)` function call in a stage body, if any. A piped call like `from x
    * | agent('...')` reaches the resolved plan as an (unresolved) PartialQueryApply; direct
    * function forms are covered as well.
    */
  private def findAgentCall(body: Relation): Option[AgentInvocation] =
    var found: Option[AgentInvocation]                     = None
    def record(name: String, args: List[Expression]): Unit =
      if found.isEmpty then
        val (named, positional) = args.partition {
          case a: FunctionArg =>
            a.name.isDefined
          case _ =>
            false
        }
        found = Some(
          AgentInvocation(
            name = name,
            args = positional.map(renderArgValue),
            namedArgs =
              named
                .collect { case a: FunctionArg =>
                  a.name.get.name -> renderArgValue(a.value)
                }
                .toMap
          )
        )
    body.traverse {
      case p: PartialQueryApply if p.partialQueryRef.leafName == "agent" =>
        record(p.partialQueryRef.leafName, p.args)
      case t: TableFunctionCall if t.name.leafName == "agent" =>
        record(t.name.leafName, t.args)
    }
    body.traverseExpressions { case f: FunctionApply =>
      f.base match
        case n: NameExpr if n.leafName == "agent" =>
          record(n.leafName, f.args)
        case _ =>
    }
    found

  end findAgentCall

  private def renderArgValue(e: Expression): String =
    e match
      case a: FunctionArg =>
        renderArgValue(a.value)
      case s: StringLiteral =>
        s.unquotedValue
      case l: LongLiteral =>
        l.value.toString
      case d: DoubleLiteral =>
        d.value.toString
      case other =>
        other.toString

  /** Move a stage to a terminal state, publish it on the bus, and persist the run state */
  private def transition(stageName: String, state: StageState): Future[Unit] =
    states += stageName -> state
    port
      .appendBus(
        BusRow(
          runId = runId,
          fromMember = config.schedulerMemberName,
          kind = "stage_state",
          payload = s"""{"stage":"${stageName}","state":"${state.stateName}"}"""
        )
      )
      .flatMap(_ => persist())

  private def persist(): Future[Unit] = port.persistRunState(
    RunState(
      runId = runId,
      flowName = flow.name.name,
      stageStates = states,
      attempts = attempts.toMap,
      steps = steps
    )
  )

  private def stageRetries(stage: StageDef): Int = stage
    .config
    .collectFirst {
      case ConfigItem(key, l: LongLiteral, _) if key.unquotedValue == "retries" =>
        l.value.toInt
    }
    .getOrElse(0)

  private def stageRetryDelayMillis(stage: StageDef): Long = stage
    .config
    .collectFirst {
      case ConfigItem(key, d: DurationLiteral, _) if key.unquotedValue == "retry_delay" =>
        d.toMillis
    }
    .getOrElse(config.defaultRetryDelayMillis)

  private def flowConfigMaxSteps: Option[Long] = flow
    .config
    .collectFirst {
      case ConfigItem(key, l: LongLiteral, _) if key.unquotedValue == "max_steps" =>
        l.value
    }

end FlowRun
