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
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.flow.StageState
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.lang.runner.connector.DBConnector
import wvlet.uni.log.LogSupport

import scala.util.control.NonFatal

/**
  * Orchestration configuration of a single stage, extracted from its `with { ... }` block
  */
case class StageExecutionConfig(
    retries: Int = 0,
    retryDelayMillis: Long = 1000L,
    backoff: String = StageExecutionConfig.BACKOFF_EXPONENTIAL,
    maxRetryDelayMillis: Option[Long] = None,
    timeoutMillis: Option[Long] = None
):
  def withRetries(retries: Int): StageExecutionConfig               = copy(retries = retries)
  def withRetryDelayMillis(delayMillis: Long): StageExecutionConfig = copy(retryDelayMillis =
    delayMillis
  )

  def withBackoff(backoff: String): StageExecutionConfig               = copy(backoff = backoff)
  def withMaxRetryDelayMillis(delayMillis: Long): StageExecutionConfig = copy(maxRetryDelayMillis =
    Some(delayMillis)
  )

  def noMaxRetryDelay(): StageExecutionConfig               = copy(maxRetryDelayMillis = None)
  def withTimeoutMillis(millis: Long): StageExecutionConfig = copy(timeoutMillis = Some(millis))
  def noTimeout(): StageExecutionConfig                     = copy(timeoutMillis = None)

  /**
    * Compute the delay before the next retry attempt. attempt is 1-origin (the number of attempts
    * made so far)
    */
  def retryDelayFor(attempt: Int): Long =
    val base =
      backoff match
        case StageExecutionConfig.BACKOFF_CONSTANT =>
          retryDelayMillis
        case StageExecutionConfig.BACKOFF_LINEAR =>
          retryDelayMillis * attempt
        case _ =>
          // exponential (default): base * 2^(attempt-1)
          retryDelayMillis * (1L << (attempt - 1).min(30))
    maxRetryDelayMillis.fold(base)(_.min(base))

end StageExecutionConfig

object StageExecutionConfig:
  val BACKOFF_CONSTANT    = "constant"
  val BACKOFF_LINEAR      = "linear"
  val BACKOFF_EXPONENTIAL = "exponential"

  def fromConfigItems(items: List[ConfigItem]): StageExecutionConfig =
    items.foldLeft(StageExecutionConfig()) { (config, item) =>
      def durationMillis: Option[Long] =
        item.value match
          case d: DurationLiteral =>
            Some(d.toMillis)
          case _ =>
            None
      item.key.unquotedValue match
        case "retries" =>
          item.value match
            case l: LongLiteral =>
              config.withRetries(l.value.toInt)
            case _ =>
              config
        case "retry_delay" =>
          durationMillis.fold(config)(config.withRetryDelayMillis)
        case "backoff" =>
          item.value match
            case s: StringLiteral =>
              config.withBackoff(s.unquotedValue)
            case _ =>
              config
        case "max_retry_delay" =>
          durationMillis.fold(config)(config.withMaxRetryDelayMillis)
        case "timeout" =>
          durationMillis.fold(config)(config.withTimeoutMillis)
        case _ =>
          // Unknown or not-yet-supported properties (e.g. heartbeat) are ignored by this executor
          config
    }

end StageExecutionConfig

/**
  * The first flow executor implementing the stage execution model.
  *
  * Stages are executed sequentially in definition order (the Typer guarantees that stage references
  * point to earlier stages). Each stage progresses through the state machine defined in
  * [[StageState]]:
  *
  *   - A stage without an `if` trigger runs when all of its upstream stages (referenced via `from`,
  *     `merge`, or `depends on`) succeeded, and is skipped otherwise.
  *   - A stage with an `if` trigger runs when the trigger condition over upstream terminal states
  *     evaluates to true (`X.failed` / `X.done`), and is skipped otherwise.
  *   - A failing stage attempt is retried up to `retries` times with the configured backoff before
  *     reaching the failed state.
  *
  * A successful stage is materialized as a temporary table named after the stage, so that
  * downstream stages can reference it by name in the generated SQL.
  *
  * Current limitations (to be lifted in future iterations):
  *   - Stages run sequentially on a single connection; no parallel execution.
  *   - `timeout` and `heartbeat` are parsed but not enforced.
  *   - Flow operators such as route/fork/wait/activate/jump/end are not yet executable.
  *   - Flow-level schedules and cross-flow dependencies are not evaluated here.
  *
  * @param connector
  *   The database connector used to materialize stage results
  * @param workEnv
  *   Work environment for logging
  * @param sleeper
  *   Sleep function used between retry attempts (injectable for testing)
  */
class FlowExecutor(
    connector: DBConnector,
    workEnv: WorkEnv,
    sleeper: Long => Unit = Thread.sleep(_)
) extends LogSupport:

  @volatile
  private var cancelled = false

  /** Request cancellation. Stages that have not started yet will end in the cancelled state */
  def cancel(): Unit = cancelled = true

  def execute(flow: FlowDef)(using ctx: Context): FlowExecutionResult =
    workEnv.info(s"Executing flow ${flow.name.name}")
    val stageNames = flow.stages.map(_.name.name).toSet
    var states     = flow.stages.map(s => s.name.name -> StageState.Pending).to(Map)
    val results    = List.newBuilder[StageResult]

    def evalTrigger(t: StageTrigger): Boolean =
      t match
        case StatePredicate(stageName, "failed", _) =>
          states.get(stageName.fullName).contains(StageState.Failed)
        case StatePredicate(stageName, "done", _) =>
          states.get(stageName.fullName).exists(_.isTerminal)
        case StatePredicate(_, _, _) =>
          false
        case TriggerAnd(left, right, _) =>
          evalTrigger(left) && evalTrigger(right)
        case TriggerOr(left, right, _) =>
          evalTrigger(left) || evalTrigger(right)

    // Upstream stages referenced from this stage via from/merge/depends on. Names that do not
    // match a stage (e.g. real tables or models) impose no state dependency
    def upstreamStagesOf(s: StageDef): List[String] =
      val refs = List.newBuilder[String]
      s.inputRefs.foreach(r => refs += r.fullName)
      s.dependsOn.foreach(r => refs += r.fullName)
      s.body
        .foreach {
          _.traverse { case m: FlowMerge =>
            m.sources.foreach(src => refs += src.fullName)
          }
        }
      refs.result().distinct.filter(stageNames.contains)

    flow
      .stages
      .foreach { s =>
        val name        = s.name.name
        val stageResult =
          if cancelled then
            StageResult(name, StageState.Cancelled, 0)
          else
            val ready =
              s.trigger match
                case Some(t) =>
                  // An explicit trigger overrides the implicit success dependency
                  evalTrigger(t)
                case None =>
                  upstreamStagesOf(s).forall(up => states.get(up).contains(StageState.Success))
            if ready then
              runStage(s)
            else
              workEnv.info(s"Stage ${name} is skipped")
              StageResult(name, StageState.Skipped, 0)
        states += name -> stageResult.state
        results += stageResult
      }
    val result = FlowExecutionResult(flow.name.name, results.result())
    workEnv.info(s"Completed flow ${flow.name.name}")
    result

  end execute

  /**
    * Run a single stage with retries: running -> success, or running -> attempt_failed -> (retrying
    * -> running)* -> failed
    */
  private def runStage(s: StageDef)(using ctx: Context): StageResult =
    val name        = s.name.name
    val config      = StageExecutionConfig.fromConfigItems(s.config)
    val maxAttempts = config.retries + 1

    var attempts                     = 0
    var state: StageState            = StageState.Pending
    var lastError: Option[Throwable] = None

    while !state.isTerminal do
      state = StageState.Running
      attempts += 1
      workEnv.info(s"Running stage ${name} (attempt ${attempts}/${maxAttempts})")
      try
        materializeStage(s)
        state = StageState.Success
      catch
        case e: WvletLangException if e.statusCode == StatusCode.NOT_IMPLEMENTED =>
          // Not retryable: the stage can never succeed
          lastError = Some(e)
          state = StageState.Failed
        case NonFatal(e) =>
          lastError = Some(e)
          state = StageState.AttemptFailed
          if attempts >= maxAttempts then
            workEnv.error(s"Stage ${name} failed after ${attempts} attempts: ${e.getMessage}")
            state = StageState.Failed
          else
            val delay = config.retryDelayFor(attempts)
            workEnv.warn(s"Stage ${name} attempt ${attempts} failed, retrying in ${delay}ms")
            state = StageState.Retrying
            sleeper(delay)
    end while

    StageResult(
      name,
      state,
      attempts,
      if state == StageState.Failed then
        lastError
      else
        None
    )

  end runStage

  /**
    * Execute the stage body and materialize the result as a temporary table named after the stage
    */
  private def materializeStage(s: StageDef)(using ctx: Context): Unit =
    val body = s
      .body
      .getOrElse(
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Stage ${s.name.name} has no executable body")
      )

    // Reject flow operators that this executor cannot run yet
    body.traverse {
      case op: (FlowRoute | FlowFork | FlowWait | FlowActivate | FlowJump | FlowEnd) =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"Flow operator ${op.nodeName} in stage ${s
                .name
                .name} is not supported by the flow executor yet"
          )
    }

    // Rewrite merge (fan-in) into union all over the materialized stage tables
    val executable = body
      .transformUp { case m: FlowMerge =>
        m.sources
          .map(stageTableRef(_, m))
          .reduceLeft[Relation] { (l, r) =>
            Union(l, r, isDistinct = false, m.span)
          }
      }
      .asInstanceOf[Relation]

    val sql = GenSQL.generateSQLFromRelation(executable, addHeader = false).sql

    given QueryProgressMonitor = ctx.queryProgressMonitor

    // Quote the table name as stage names may collide with reserved SQL keywords (e.g. primary)
    connector.execute(s"""create or replace temp table "${s.name.name}" as\n${sql}""")

  end materializeStage

  private def stageTableRef(src: NameExpr, m: FlowMerge): Relation =
    src match
      case q: QualifiedName =>
        TableRef(q, src.span)
      case other =>
        TableRef(UnquotedIdentifier(other.fullName, other.span), other.span)

end FlowExecutor
