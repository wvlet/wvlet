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

import wvlet.lang.api.Span
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.flow.StageState
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.lang.runner.connector.DBConnector
import wvlet.uni.log.LogSupport
import wvlet.uni.util.ULID

import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.util.Using
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
  * Configuration of the flow executor
  *
  * @param maxParallelism
  *   Maximum number of stages that can run concurrently
  */
case class FlowExecutorConfig(maxParallelism: Int = 4):
  def withMaxParallelism(n: Int): FlowExecutorConfig = copy(maxParallelism = n)

/**
  * Runs the body of a single stage and materializes the result into the given target table.
  * Injectable for testing the scheduler without a database
  */
trait FlowStageRunner:
  def run(stage: StageDef, targetTable: String)(using ctx: Context): Unit

/**
  * The flow executor implementing the stage execution model with a ready-set DAG scheduler.
  *
  * Each stage progresses through the state machine defined in [[StageState]]. The scheduler is a
  * single-threaded event loop over a completion queue:
  *
  *   - A pending stage becomes *decidable* once all stages it references (via `from`, `merge`,
  *     `depends on`, or `if` triggers) reached a terminal state.
  *   - A decidable stage is *ready* when its implicit success dependencies succeeded, or its `if`
  *     trigger evaluates to true; otherwise it is skipped.
  *   - All ready stages are launched concurrently on a bounded worker pool (`maxParallelism`).
  *   - A failing attempt is retried up to `retries` times; retries are scheduled asynchronously
  *     with the configured backoff instead of blocking a worker thread.
  *   - A `timeout` config bounds each attempt: on expiry the attempt is treated as failed
  *     (retryable). Note: the underlying SQL statement is not cancelled server-side yet, so a
  *     timed-out attempt may keep occupying a worker slot until the statement completes.
  *
  * A successful stage is materialized as a run-scoped table (`__wv_flow_<run_id>_<stage>`), so
  * stage names never collide with real tables and concurrent runs do not interfere. Parallel stages
  * materialize through per-worker sessions sharing the same database instance.
  *
  * Current limitations (to be lifted in future iterations):
  *   - `heartbeat` is parsed but not enforced.
  *   - Flow operators such as route/fork/wait/activate/jump/end are not yet executable.
  *   - Flow-level schedules and cross-flow dependencies are not evaluated here.
  *
  * @param connector
  *   The database connector used to materialize stage results
  * @param workEnv
  *   Work environment for logging
  * @param config
  *   Executor configuration (parallelism)
  * @param stageRunner
  *   Optional stage body runner override (for testing the scheduler)
  * @param retryScheduler
  *   Optional (delayMillis, action) scheduler override (for testing retry backoff)
  */
class FlowExecutor(
    connector: DBConnector,
    workEnv: WorkEnv,
    config: FlowExecutorConfig = FlowExecutorConfig(),
    stageRunner: Option[FlowStageRunner] = None,
    retryScheduler: Option[(Long, () => Unit) => Unit] = None
) extends LogSupport:
  import FlowExecutor.FlowEvent
  import FlowExecutor.FlowEvent.*

  @volatile
  private var cancelled = false

  private val eventQueue = LinkedBlockingQueue[FlowEvent]()

  /** Request cancellation. Stages that have not started yet will end in the cancelled state */
  def cancel(): Unit =
    cancelled = true
    eventQueue.put(CancelRequested)

  def execute(flow: FlowDef)(using ctx: Context): FlowExecutionResult =
    val runId = ULID.newULIDString
    workEnv.info(s"Executing flow ${flow.name.name} (run: ${runId})")

    val stageNames                                      = flow.stages.map(_.name.name).toSet
    val stageConfigs: Map[String, StageExecutionConfig] =
      flow.stages.map(s => s.name.name -> StageExecutionConfig.fromConfigItems(s.config)).toMap

    // Stages materialize into run-scoped tables so that concurrent runs and real tables with the
    // same name never collide
    def tableFor(stageName: String): String = FlowExecutor.stageTableName(runId, stageName)

    val runBody: (StageDef, String) => Unit =
      stageRunner match
        case Some(r) =>
          (s, table) => r.run(s, table)
        case None =>
          (s, table) => materializeStage(s, stageNames, tableFor)

    // All mutable scheduler state below is owned by this (caller) thread; worker threads only
    // post events to the eventQueue
    val states   = mutable.Map.from(flow.stages.map(s => s.name.name -> StageState.Pending))
    val attempts = mutable.Map.empty[String, Int].withDefaultValue(0)
    val errors   = mutable.Map.empty[String, Throwable]
    // (stage, attempt) pairs whose outcome has been decided (guards against duplicate events
    // from a timed-out attempt completing later)
    val handledAttempts  = mutable.Set.empty[(String, Int)]
    var scheduledRetries = 0
    var runningCount     = 0
    var cancellationDone = false

    val threadFactory =
      new ThreadFactory:
        override def newThread(r: Runnable): Thread =
          val t = Thread(r, s"wvlet-flow-${flow.name.name}")
          t.setDaemon(true)
          t

    val workerPool = Executors.newFixedThreadPool(config.maxParallelism.max(1), threadFactory)
    val timer      = Executors.newSingleThreadScheduledExecutor(threadFactory)
    val scheduleRetry: (Long, () => Unit) => Unit = retryScheduler.getOrElse { (delay, action) =>
      timer.schedule(
        new Runnable:
          def run(): Unit = action()
        ,
        delay,
        TimeUnit.MILLISECONDS
      )
    }

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

    def triggerRefsOf(t: StageTrigger): List[String] =
      t match
        case StatePredicate(stageName, _, _) =>
          List(stageName.fullName)
        case TriggerAnd(left, right, _) =>
          triggerRefsOf(left) ::: triggerRefsOf(right)
        case TriggerOr(left, right, _) =>
          triggerRefsOf(left) ::: triggerRefsOf(right)

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

    // Stages whose terminal states this stage's scheduling decision depends on
    def schedulingRefsOf(s: StageDef): List[String] =
      s.trigger match
        case Some(t) =>
          // An explicit trigger overrides the implicit success dependency
          triggerRefsOf(t).distinct.filter(stageNames.contains)
        case None =>
          upstreamStagesOf(s)

    def isDecidable(s: StageDef): Boolean = schedulingRefsOf(s).forall(n => states(n).isTerminal)

    def isReady(s: StageDef): Boolean =
      s.trigger match
        case Some(t) =>
          evalTrigger(t)
        case None =>
          upstreamStagesOf(s).forall(up => states(up) == StageState.Success)

    def submitAttempt(s: StageDef, attempt: Int): Unit =
      val name        = s.name.name
      val stageConfig = stageConfigs(name)
      states(name) = StageState.Running
      runningCount += 1
      workEnv.info(s"Running stage ${name} (attempt ${attempt}/${stageConfig.retries + 1})")
      workerPool.submit(
        new Runnable:
          override def run(): Unit =
            val error =
              try
                runBody(s, tableFor(name))
                None
              catch
                case NonFatal(e) =>
                  Some(e)
            eventQueue.put(AttemptResult(s, attempt, error))
      )
      // Bound the attempt duration with the configured timeout. The timed-out attempt is
      // reported as a retryable failure; a late completion event is ignored via handledAttempts
      stageConfig
        .timeoutMillis
        .foreach { timeout =>
          timer.schedule(
            new Runnable:
              override def run(): Unit = eventQueue.put(
                AttemptResult(
                  s,
                  attempt,
                  Some(
                    StatusCode
                      .OPERATION_TIMED_OUT
                      .newException(s"Stage ${name} timed out after ${timeout}ms")
                  )
                )
              )
            ,
            timeout,
            TimeUnit.MILLISECONDS
          )
        }

    end submitAttempt

    // Launch every pending stage that has become decidable; skipping a stage can make later
    // stages decidable, so iterate to a fixpoint
    def launchReadyStages(): Unit =
      var progress = true
      while progress do
        progress = false
        flow
          .stages
          .foreach { s =>
            val name = s.name.name
            if states(name) == StageState.Pending && isDecidable(s) then
              if isReady(s) then
                submitAttempt(s, attempts(name) + 1)
              else
                workEnv.info(s"Stage ${name} is skipped")
                states(name) = StageState.Skipped
                progress = true
          }

    def handleCancellation(): Unit =
      if !cancellationDone then
        cancellationDone = true
        flow
          .stages
          .foreach { s =>
            val name = s.name.name
            if !states(name).isTerminal then
              states(name) = StageState.Cancelled
          }

    def allTerminal: Boolean = states.values.forall(_.isTerminal)

    try
      var done = false
      while !done do
        if cancelled then
          handleCancellation()
        else
          launchReadyStages()
        if allTerminal then
          done = true
        else if runningCount == 0 && scheduledRetries == 0 then
          // Defensive: no outstanding work can change any state. This indicates a scheduling
          // bug (the stage DAG is acyclic by construction), so skip the undecidable remainder
          flow
            .stages
            .filter(s => !states(s.name.name).isTerminal)
            .foreach { s =>
              workEnv.warn(s"Stage ${s.name.name} is unschedulable; marking as skipped")
              states(s.name.name) = StageState.Skipped
            }
          done = true
        else
          eventQueue.take() match
            case CancelRequested =>
            // handled at the top of the loop via the cancelled flag
            case RetryDue(s, attempt) =>
              scheduledRetries -= 1
              if states(s.name.name) == StageState.Retrying then
                submitAttempt(s, attempt)
            case AttemptResult(s, attempt, error) =>
              val name = s.name.name
              if !handledAttempts.contains((name, attempt)) then
                handledAttempts += name -> attempt
                runningCount -= 1
                attempts(name) = attempt
                if states(name) == StageState.Running then
                  error match
                    case None =>
                      states(name) = StageState.Success
                    case Some(e) =>
                      errors(name) = e
                      val stageConfig  = stageConfigs(name)
                      val maxAttempts  = stageConfig.retries + 1
                      val nonRetryable =
                        e match
                          case we: WvletLangException =>
                            we.statusCode == StatusCode.NOT_IMPLEMENTED
                          case _ =>
                            false
                      // attempt_failed: decide between retrying and failed
                      if nonRetryable || attempt >= maxAttempts then
                        workEnv.error(
                          s"Stage ${name} failed after ${attempt} attempts: ${e.getMessage}"
                        )
                        states(name) = StageState.Failed
                      else
                        val delay = stageConfig.retryDelayFor(attempt)
                        workEnv.warn(
                          s"Stage ${name} attempt ${attempt} failed, retrying in ${delay}ms"
                        )
                        states(name) = StageState.Retrying
                        scheduledRetries += 1
                        scheduleRetry(delay, () => eventQueue.put(RetryDue(s, attempt + 1)))
              end if
        end if
      end while
    finally
      workerPool.shutdownNow()
      timer.shutdownNow()
    end try

    // Report results in stage definition order for deterministic summaries
    val stageResults = flow
      .stages
      .map { s =>
        val name  = s.name.name
        val state = states(name)
        StageResult(
          name,
          state,
          attempts(name),
          if state == StageState.Failed then
            errors.get(name)
          else
            None
          ,
          if state == StageState.Success then
            Some(tableFor(name))
          else
            None
        )
      }
    val result = FlowExecutionResult(flow.name.name, runId, stageResults)
    workEnv.info(s"Completed flow ${flow.name.name} (run: ${runId})")
    result

  end execute

  /**
    * Execute the stage body and materialize the result as a run-scoped table. References to other
    * stages in the body are rewritten to their run-scoped table names. Materialization runs on a
    * dedicated session so that parallel stages do not contend on a single connection
    */
  private def materializeStage(s: StageDef, stageNames: Set[String], tableFor: String => String)(
      using ctx: Context
  ): Unit =
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

    def stageTableRef(stageName: String, span: Span): Relation = TableRef(
      DoubleQuotedIdentifier(tableFor(stageName), span),
      span
    )

    // Rewrite stage references to their run-scoped tables, and merge (fan-in) into union all
    val executable = body
      .transformUp {
        case t: TableRef if stageNames.contains(t.name.fullName) =>
          stageTableRef(t.name.fullName, t.span)
        case m: FlowMerge =>
          m.sources
            .map(src => stageTableRef(src.fullName, src.span))
            .reduceLeft[Relation] { (l, r) =>
              Union(l, r, isDistinct = false, m.span)
            }
      }
      .asInstanceOf[Relation]

    val sql = GenSQL.generateSQLFromRelation(executable, addHeader = false).sql

    // Quote the table name: it contains a ULID and stage names may collide with SQL keywords.
    // The table is a regular (non-temp) table because parallel stages materialize on separate
    // sessions, and temp tables are session-scoped
    val ddl = s"""create or replace table "${tableFor(s.name.name)}" as\n${sql}"""
    debug(s"Materializing stage ${s.name.name}:\n${ddl}")
    connector.withSession { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        stmt.execute(ddl)
      }
    }

  end materializeStage

end FlowExecutor

object FlowExecutor:
  /** The run-scoped table name holding the materialized result of a stage */
  def stageTableName(runId: String, stageName: String): String =
    s"__wv_flow_${runId.toLowerCase}_${stageName}"

  /** Drop the run-scoped tables of the given stages (best-effort cleanup) */
  def dropRunTables(connector: DBConnector, runId: String, stageNames: Seq[String]): Unit =
    connector.withSession { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        stageNames.foreach { name =>
          try
            stmt.execute(s"""drop table if exists "${stageTableName(runId, name)}"""")
          catch
            case NonFatal(e) =>
          // best-effort cleanup
        }
      }
    }

  private enum FlowEvent:
    case AttemptResult(stage: StageDef, attempt: Int, error: Option[Throwable])
    case RetryDue(stage: StageDef, attempt: Int)
    case CancelRequested

end FlowExecutor
