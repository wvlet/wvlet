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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
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
  * @param cancelPollIntervalMillis
  *   Interval for polling the run registry for cross-process cancellation requests
  * @param maxJumpDepth
  *   Maximum depth of `-> Flow` jump chains; guards against jump cycles (flow A -> B -> A)
  */
case class FlowExecutorConfig(
    maxParallelism: Int = 4,
    cancelPollIntervalMillis: Long = 500L,
    maxJumpDepth: Int = 8
):
  def withMaxParallelism(n: Int): FlowExecutorConfig                 = copy(maxParallelism = n)
  def withCancelPollIntervalMillis(millis: Long): FlowExecutorConfig = copy(
    cancelPollIntervalMillis = millis
  )

  def withMaxJumpDepth(depth: Int): FlowExecutorConfig = copy(maxJumpDepth = depth)

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
  *     (retryable), the underlying SQL statement is cancelled server-side (best-effort via JDBC
  *     `Statement.cancel()`), and the worker thread is interrupted so that the worker slot is freed
  *     immediately for other stages and retries.
  *
  * A successful stage is materialized as a run-scoped table (`__wv_flow_<run_id>_<stage>`), so
  * stage names never collide with real tables and concurrent runs do not interfere. Parallel stages
  * materialize through per-worker sessions sharing the same database instance.
  *
  * Flow operators in stage bodies are lowered with [[FlowLowering]] before scheduling: fork stages
  * are flattened, route cases become filter predicates on the target stages' reads, wait becomes a
  * pre-materialization delay, activate is a local logging stub, and end is a pass-through.
  *
  * A `-> Flow` jump transfers control only: when the jumping stage succeeds, the target flow is
  * triggered as a new run (with its own run id) after the current flow completes. Jump chains are
  * bounded by `maxJumpDepth` to guard against cycles (flow A -> B -> A).
  *
  * Current limitations (to be lifted in future iterations):
  *   - `heartbeat` is parsed but not enforced.
  *   - Flow-level cron schedules are not evaluated here.
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
    retryScheduler: Option[(Long, () => Unit) => Unit] = None,
    registry: Option[FlowRunStore] = None
) extends LogSupport:
  import FlowExecutor.FlowEvent
  import FlowExecutor.FlowEvent.*

  @volatile
  private var cancelled = false

  private val eventQueue = LinkedBlockingQueue[FlowEvent]()

  /**
    * Request cancellation. Stages that have not started yet will end in the cancelled state, and
    * in-flight attempts are stopped: their SQL statements are cancelled server-side (best-effort)
    * and their worker threads are interrupted
    */
  def cancel(): Unit =
    cancelled = true
    eventQueue.put(CancelRequested)

  /**
    * Check whether the flow's cross-flow dependency (`depends on X` or `if X.failed/done`) is
    * satisfied by the latest recorded run of the referenced flow
    */
  private def dependencySatisfied(flow: FlowDef): Boolean =
    flow.dependency match
      case None =>
        true
      case Some(dep) =>
        registry match
          case None =>
            workEnv.warn(
              s"Flow ${flow
                  .name
                  .name} has a cross-flow dependency, but no run registry is available; skipping"
            )
            false
          case Some(reg) =>
            dep match
              case DependsOnFlow(flowName, _) =>
                reg.latestRunOf(flowName.fullName).exists(_.state == FlowRunRecord.STATE_SUCCESS)
              case FlowStatePredicate(flowName, "failed", _) =>
                reg.latestRunOf(flowName.fullName).exists(_.state == FlowRunRecord.STATE_FAILED)
              case FlowStatePredicate(flowName, "done", _) =>
                reg.latestRunOf(flowName.fullName).exists(_.isTerminal)
              case FlowStatePredicate(_, _, _) =>
                false

  /**
    * Execute the given flow. When `resumeFrom` holds the record of a previous failed or cancelled
    * run, the run is resumed under the same run id: stages recorded as successful keep their
    * materialized run-scoped tables and are not re-executed, and the remaining stages run with a
    * fresh retry budget
    */
  def execute(flow: FlowDef, resumeFrom: Option[FlowRunRecord] = None)(using
      ctx: Context
  ): FlowExecutionResult = executeInternal(flow, resumeFrom, jumpDepth = 0)

  /** Resolve a flow referenced by a `-> Flow` jump through the compilation context */
  private def resolveJumpTarget(name: String)(using ctx: Context): FlowDef =
    ctx.findTermSymbolByName(name).map(_.tree) match
      case Some(f: FlowDef) =>
        f
      case _ =>
        throw StatusCode
          .FLOW_NOT_FOUND
          .newException(s"Flow '${name}' referenced by a -> jump is not found")

  private def executeInternal(flow: FlowDef, resumeFrom: Option[FlowRunRecord], jumpDepth: Int)(
      using ctx: Context
  ): FlowExecutionResult =
    val runId     = resumeFrom.map(_.runId).getOrElse(ULID.newULIDString)
    val startedAt = System.currentTimeMillis()
    workEnv.info(s"Executing flow ${flow.name.name} (run: ${runId})")

    // Evaluate cross-flow dependencies against the run registry before scheduling any stage.
    // A resumed run already passed this gate when it originally started
    if resumeFrom.isEmpty && !dependencySatisfied(flow) then
      workEnv.info(s"Flow ${flow.name.name} dependency is not satisfied; skipping all stages")
      val skipped = FlowExecutionResult(
        flow.name.name,
        runId,
        flow.stages.map(s => StageResult(s.name.name, StageState.Skipped, 0))
      )
      registry.foreach {
        _.save(
          FlowRunRecord(
            runId,
            flow.name.name,
            FlowRunRecord.STATE_SKIPPED,
            startedAt,
            Some(System.currentTimeMillis()),
            skipped.stageResults.map(r => StageRunRecord(r.name, r.state.stateName, r.attempts))
          )
        )
      }
      return skipped

    // Lower flow operators (fork/route/wait/activate/end) into SQL-expressible stage bodies
    // plus orchestration metadata
    val lowered                                         = FlowLowering.lower(flow)
    val flowStages                                      = lowered.stages
    val stageNames                                      = lowered.stageNames
    val routeFilters                                    = lowered.routeFilters
    val stageConfigs: Map[String, StageExecutionConfig] =
      flowStages.map(ls => ls.name -> StageExecutionConfig.fromConfigItems(ls.stage.config)).toMap

    // Resolve jump targets eagerly so that a reference to an undefined flow fails the run
    // before any stage executes
    val jumpTargetFlows: Map[String, FlowDef] =
      flowStages.flatMap(_.jumpTargets).distinct.map(name => name -> resolveJumpTarget(name)).toMap

    // Stages materialize into run-scoped tables so that concurrent runs and real tables with the
    // same name never collide
    def tableFor(stageName: String): String = FlowExecutor.stageTableName(runId, stageName)

    // In-flight attempt tracking, keyed by (stage, attempt). Worker threads register their
    // active SQL statement while it executes; the scheduler thread cancels statements and
    // interrupts workers on timeout or flow cancellation
    val activeStatements = ConcurrentHashMap[(String, Int), java.sql.Statement]()
    val activeFutures    = ConcurrentHashMap[(String, Int), java.util.concurrent.Future[?]]()

    // Stop an in-flight attempt: cancel its SQL statement server-side (best-effort) and
    // interrupt its worker thread so that the worker slot is freed immediately
    def cancelAttempt(key: (String, Int)): Unit =
      Option(activeStatements.remove(key)).foreach { stmt =>
        try
          stmt.cancel()
        catch
          case NonFatal(e) =>
            debug(
              s"Failed to cancel the statement of stage ${key._1} (attempt ${key._2}): ${e
                  .getMessage}"
            )
      }
      Option(activeFutures.remove(key)).foreach(_.cancel(true))

    def clearAttempt(key: (String, Int)): Unit =
      activeStatements.remove(key)
      activeFutures.remove(key)

    val runBody: (FlowLowering.LoweredStage, String, (String, Int)) => Unit =
      stageRunner match
        case Some(r) =>
          (ls, table, _) => r.run(ls.stage, table)
        case None =>
          (ls, table, attemptKey) =>
            // A wait operator delays the materialization of this stage. The sleep runs on a
            // worker thread and is interruptible for cancellation
            ls.waitMillis
              .foreach { delay =>
                workEnv.info(s"Stage ${ls.name} waits for ${delay}ms")
                Thread.sleep(delay)
              }
            materializeStage(
              ls,
              stageNames,
              tableFor,
              routeFilters,
              registerStatement = stmt => activeStatements.put(attemptKey, stmt),
              deregisterStatement = () => activeStatements.remove(attemptKey)
            )
            // Activation is a local stub until external sink connectors are available
            ls.activateTargets
              .foreach { target =>
                workEnv.info(
                  s"[activate] stage ${ls
                      .name}: sending materialized output ${table} to '${target}'"
                )
              }

    // Stages recorded as successful in the resumed run keep their materialized tables and are
    // not re-executed. Stage records that no longer match a stage of the flow are ignored
    val resumedStages: Map[String, StageRunRecord] = resumeFrom
      .map {
        _.stages
          .filter { s =>
            s.state == StageState.Success.stateName && s.table.isDefined &&
            stageNames.contains(s.name)
          }
          .map(s => s.name -> s)
          .toMap
      }
      .getOrElse(Map.empty)
    if resumedStages.nonEmpty then
      workEnv.info(
        s"Resuming run ${runId}: reusing ${resumedStages.size} successful stage(s): ${resumedStages
            .keys
            .mkString(", ")}"
      )

    // All mutable scheduler state below is owned by this (caller) thread; worker threads only
    // post events to the eventQueue
    val states = mutable
      .Map
      .from(
        flowStages.map(ls =>
          ls.name -> (
            if resumedStages.contains(ls.name) then
              StageState.Success
            else
              StageState.Pending
          )
        )
      )
    // Re-executed stages get a fresh retry budget; reused stages keep their recorded attempts
    val attempts = mutable.Map.empty[String, Int].withDefaultValue(0)
    resumedStages.foreach((name, s) => attempts(name) = s.attempts)
    val errors = mutable.Map.empty[String, Throwable]
    // (stage, attempt) pairs whose outcome has been decided (guards against duplicate events
    // from a timed-out attempt completing later)
    val handledAttempts  = mutable.Set.empty[(String, Int)]
    var scheduledRetries = 0
    var runningCount     = 0
    var cancellationDone = false
    // Jump targets of successfully completed stages, triggered after this flow completes
    val pendingJumps = mutable.ListBuffer.empty[String]

    val threadFactory =
      new ThreadFactory:
        override def newThread(r: Runnable): Thread =
          val t = Thread(r, s"wvlet-flow-${flow.name.name}")
          t.setDaemon(true)
          t

    val workerPool = Executors.newFixedThreadPool(config.maxParallelism.max(1), threadFactory)
    val timer      = Executors.newSingleThreadScheduledExecutor(threadFactory)

    // Poll the registry for a cross-process cancellation request (wvlet flow session cancel)
    registry.foreach { reg =>
      val interval = config.cancelPollIntervalMillis.max(1L)
      timer.scheduleAtFixedRate(
        new Runnable:
          override def run(): Unit =
            if !cancelled && reg.cancelRequested(runId) then
              workEnv.info(s"Cancellation of run ${runId} was requested; cancelling")
              cancel()
        ,
        interval,
        interval,
        TimeUnit.MILLISECONDS
      )
    }
    val scheduleRetry: (Long, () => Unit) => Unit = retryScheduler.getOrElse { (delay, action) =>
      timer.schedule(
        new Runnable:
          def run(): Unit = action()
        ,
        delay,
        TimeUnit.MILLISECONDS
      )
    }

    // Persist a snapshot of the run so that other processes (wvlet flow session) can observe it
    def persistSnapshot(finished: Boolean = false): Unit = registry.foreach { reg =>
      val stageRecords = flowStages.map { ls =>
        val name  = ls.name
        val state = states(name)
        StageRunRecord(
          name,
          state.stateName,
          attempts(name),
          errors.get(name).map(_.getMessage),
          if state == StageState.Success then
            Some(tableFor(name))
          else
            None
        )
      }
      val flowState =
        if finished then
          FlowRunRecord.flowStateOf(states.values)
        else
          FlowRunRecord.STATE_RUNNING
      reg.save(
        FlowRunRecord(
          runId,
          flow.name.name,
          flowState,
          startedAt,
          if finished then
            Some(System.currentTimeMillis())
          else
            None
          ,
          stageRecords
        )
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
    def upstreamStagesOf(ls: FlowLowering.LoweredStage): List[String] =
      val refs = List.newBuilder[String]
      ls.stage.inputRefs.foreach(r => refs += r.fullName)
      ls.stage.dependsOn.foreach(r => refs += r.fullName)
      ls.body
        .foreach {
          _.traverse { case m: FlowMerge =>
            m.sources.foreach(src => refs += src.fullName)
          }
        }
      refs.result().distinct.filter(stageNames.contains)

    // Stages whose terminal states this stage's scheduling decision depends on
    def schedulingRefsOf(ls: FlowLowering.LoweredStage): List[String] =
      ls.stage.trigger match
        case Some(t) =>
          // An explicit trigger overrides the implicit success dependency
          triggerRefsOf(t).distinct.filter(stageNames.contains)
        case None =>
          upstreamStagesOf(ls)

    def isDecidable(ls: FlowLowering.LoweredStage): Boolean = schedulingRefsOf(ls).forall(n =>
      states(n).isTerminal
    )

    def isReady(ls: FlowLowering.LoweredStage): Boolean =
      ls.stage.trigger match
        case Some(t) =>
          evalTrigger(t)
        case None =>
          upstreamStagesOf(ls).forall(up => states(up) == StageState.Success)

    def submitAttempt(ls: FlowLowering.LoweredStage, attempt: Int): Unit =
      val name        = ls.name
      val stageConfig = stageConfigs(name)
      states(name) = StageState.Running
      runningCount += 1
      workEnv.info(s"Running stage ${name} (attempt ${attempt}/${stageConfig.retries + 1})")
      val future = workerPool.submit(
        new Runnable:
          override def run(): Unit =
            val error =
              try
                runBody(ls, tableFor(name), (name, attempt))
                None
              catch
                case NonFatal(e) =>
                  Some(e)
            eventQueue.put(AttemptResult(ls, attempt, error))
      )
      activeFutures.put((name, attempt), future)
      // Bound the attempt duration with the configured timeout. The timed-out attempt is
      // reported as a retryable failure; a late completion event is ignored via handledAttempts
      stageConfig
        .timeoutMillis
        .foreach { timeout =>
          timer.schedule(
            new Runnable:
              override def run(): Unit = eventQueue.put(
                AttemptResult(
                  ls,
                  attempt,
                  Some(
                    StatusCode
                      .OPERATION_TIMED_OUT
                      .newException(s"Stage ${name} timed out after ${timeout}ms")
                  ),
                  timedOut = true
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
        flowStages.foreach { ls =>
          val name = ls.name
          if states(name) == StageState.Pending && isDecidable(ls) then
            if isReady(ls) then
              submitAttempt(ls, attempts(name) + 1)
            else
              workEnv.info(s"Stage ${name} is skipped")
              states(name) = StageState.Skipped
              progress = true
        }

    def handleCancellation(): Unit =
      if !cancellationDone then
        cancellationDone = true
        // Stop all in-flight attempts before marking the remaining stages cancelled
        (activeStatements.keySet().asScala ++ activeFutures.keySet().asScala)
          .toSet
          .foreach(cancelAttempt)
        flowStages.foreach { ls =>
          if !states(ls.name).isTerminal then
            states(ls.name) = StageState.Cancelled
        }

    def allTerminal: Boolean = states.values.forall(_.isTerminal)

    persistSnapshot()
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
          flowStages
            .filter(ls => !states(ls.name).isTerminal)
            .foreach { ls =>
              workEnv.warn(s"Stage ${ls.name} is unschedulable; marking as skipped")
              states(ls.name) = StageState.Skipped
            }
          done = true
        else
          eventQueue.take() match
            case CancelRequested =>
            // handled at the top of the loop via the cancelled flag
            case RetryDue(s, attempt) =>
              scheduledRetries -= 1
              if states(s.name) == StageState.Retrying then
                submitAttempt(s, attempt)
            case AttemptResult(s, attempt, error, timedOut) =>
              val name = s.name
              if !handledAttempts.contains((name, attempt)) then
                handledAttempts += name -> attempt
                // A timed-out attempt is still running: stop it so its worker slot is freed.
                // A completed attempt only needs its tracking entries cleared
                if timedOut then
                  cancelAttempt((name, attempt))
                else
                  clearAttempt((name, attempt))
                runningCount -= 1
                attempts(name) = attempt
                if states(name) == StageState.Running then
                  error match
                    case None =>
                      states(name) = StageState.Success
                      pendingJumps ++= s.jumpTargets
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
        persistSnapshot()
      end while
    finally
      workerPool.shutdownNow()
      timer.shutdownNow()
    end try

    persistSnapshot(finished = true)
    // The run reached a terminal state; a pending cancellation request is no longer relevant
    registry.foreach(_.clearCancelRequest(runId))

    // Report results in stage definition order for deterministic summaries
    val stageResults = flowStages.map { ls =>
      val name  = ls.name
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

    // Trigger the control-only jumps recorded by successful stages, each as a new run of the
    // target flow. Jump chains are bounded by maxJumpDepth to terminate cycles (A -> B -> A)
    if pendingJumps.nonEmpty && !cancelled then
      pendingJumps
        .toList
        .distinct
        .foreach { target =>
          if jumpDepth + 1 > config.maxJumpDepth then
            workEnv.warn(
              s"Skipping jump to flow ${target}: max jump depth (${config.maxJumpDepth}) reached"
            )
          else
            workEnv.info(s"Jumping from flow ${flow.name.name} to flow ${target}")
            executeInternal(jumpTargetFlows(target), resumeFrom = None, jumpDepth = jumpDepth + 1)
        }

    result

  end executeInternal

  /**
    * Execute the lowered stage body and materialize the result as a run-scoped table. References to
    * other stages in the body are rewritten to their run-scoped table names (with routing
    * predicates applied when this stage is a route target). Materialization runs on a dedicated
    * session so that parallel stages do not contend on a single connection
    */
  private def materializeStage(
      ls: FlowLowering.LoweredStage,
      stageNames: Set[String],
      tableFor: String => String,
      routeFilters: Map[(String, String), Expression],
      registerStatement: java.sql.Statement => Unit,
      deregisterStatement: () => Unit
  )(using ctx: Context): Unit =
    val body = ls
      .body
      .getOrElse(
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Stage ${ls.name} has no executable body")
      )

    // Reference a source stage's run-scoped table, filtered with the routing predicate when
    // this stage is a route target of the source
    def stageTableRef(stageName: String, span: Span): Relation =
      val ref = TableRef(DoubleQuotedIdentifier(tableFor(stageName), span), span)
      routeFilters.get((ls.name, stageName)) match
        case Some(pred) =>
          Filter(ref, pred, span)
        case None =>
          ref

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
    val ddl = s"""create or replace table "${tableFor(ls.name)}" as\n${sql}"""
    debug(s"Materializing stage ${ls.name}:\n${ddl}")
    connector.withSession { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        // Expose the statement to the scheduler so that a timeout or cancellation can stop
        // it server-side while it is executing
        registerStatement(stmt)
        try stmt.execute(ddl)
        finally deregisterStatement()
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
    case AttemptResult(
        stage: FlowLowering.LoweredStage,
        attempt: Int,
        error: Option[Throwable],
        timedOut: Boolean = false
    )

    case RetryDue(stage: FlowLowering.LoweredStage, attempt: Int)
    case CancelRequested

end FlowExecutor
