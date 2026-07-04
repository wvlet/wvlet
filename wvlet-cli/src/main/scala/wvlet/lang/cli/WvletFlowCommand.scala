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

import wvlet.uni.cli.launcher.argument
import wvlet.uni.cli.launcher.command
import wvlet.uni.cli.launcher.option
import wvlet.uni.control.Control
import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.CompileResult
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.model.expr.FunctionArg
import wvlet.lang.model.plan.DependsOnFlow
import wvlet.lang.model.plan.FlowDef
import wvlet.lang.model.plan.FlowStatePredicate
import wvlet.lang.model.plan.Query
import wvlet.lang.model.plan.RunFlow
import wvlet.lang.runner.CronSchedule
import wvlet.lang.runner.FlowExecutor
import wvlet.lang.runner.FlowRunRecord
import wvlet.lang.runner.FlowRunRetention
import wvlet.lang.runner.FlowRunStore
import wvlet.lang.runner.FlowScheduleConfig
import wvlet.lang.runner.FlowScheduler
import wvlet.lang.runner.ScheduledFlow
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.uni.log.LogSupport

case class WvletFlowOption(
    @option(prefix = "-w", description = "Working folder containing .wv files")
    workFolder: String = ".",
    @option(prefix = "--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(prefix = "--catalog", description = "Context database catalog to use")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Context database schema to use")
    schema: Option[String] = None,
    @option(prefix = "--run-store", description = "Flow run store type: file (default) or sqlite")
    runStore: Option[String] = None
)

/**
  * `wvlet flow` subcommands for managing and running flows from the CLI
  */
class WvletFlowCommand(opts: WvletGlobalOption) extends LogSupport:

  @command(description = "Run a flow and print its stage results", isDefault = true)
  def help: Unit = info("Usage: wvlet flow <run|list|show> ...")

  @command(description = "Run a flow defined in the working folder")
  def run(
      flowOption: WvletFlowOption,
      @argument(description = "Flow to run: a name or a flow call like \"F(segment = 'a')\"")
      name: String
  ): Unit =
    val (flowName, args) = parseFlowCall(name)
    executeFlow(flowOption, flowName, resumeFrom = None, args = args)

  /**
    * Parse the flow argument of `wvlet flow run` as a flow-call expression (`F` or `F(segment =
    * 'a')`) with the regular wvlet parser, so the CLI and the `run flow` statement share one syntax
    */
  private def parseFlowCall(input: String): (String, List[FunctionArg]) =
    val unit = CompilationUnit.fromWvletString(s"run flow ${input}")
    val plan = ParserPhase.parseOnly(unit)
    var parsed: Option[(String, List[FunctionArg])] = None
    plan.traverse { case q: Query =>
      q.child match
        case r: RunFlow =>
          parsed = Some((r.flowName.fullName, r.args))
        case _ =>
    }
    parsed.getOrElse(
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(
          s"Invalid flow reference '${input}': expected a flow name or a flow call like \"F(param = 'value')\""
        )
    )

  /** Compile the flows in the working folder and execute the given flow */
  private def executeFlow(
      flowOption: WvletFlowOption,
      name: String,
      resumeFrom: Option[FlowRunRecord],
      args: List[FunctionArg] = Nil
  ): Unit =
    withFlows(flowOption) { (flows, compileResult, workEnv) =>
      val (unit, flow) = findFlow(flows, name)
      val profile = Profile.getProfile(flowOption.profile, flowOption.catalog, flowOption.schema)
      // System.exit is deferred until the resource blocks release the connector and run store
      var failed = false
      Control.withResource(DBConnectorProvider(workEnv)) { dbConnectorProvider =>
        val connector = dbConnectorProvider.getConnector(profile)

        given ctx: Context = compileResult
          .context
          .withCompilationUnit(unit)
          .newContext(Symbol.NoSymbol)

        Control.withResource(newRunStore(flowOption, workEnv)) { store =>
          val result = FlowExecutor(connector, workEnv, registry = Some(store)).execute(
            flow,
            resumeFrom,
            args
          )
          println(result.toPrettyBox())
          failed = result.hasError
        }
      }
      if failed && !WvletMain.isInSbt then
        System.exit(1)
    }

  /** Create the run store selected with --run-store (or the WVLET_FLOW_STORE environment) */
  private def newRunStore(flowOption: WvletFlowOption, workEnv: WorkEnv): FlowRunStore = flowOption
    .runStore
    .map(FlowRunStore.ofType(_, workEnv))
    .getOrElse(FlowRunStore.forWorkEnv(workEnv))

  @command(description =
    "Run a scheduled flow once per schedule window between --from and --to (sequentially)"
  )
  def backfill(
      flowOption: WvletFlowOption,
      @argument(description = "Flow to backfill: a name or a flow call like \"F(segment = 'a')\"")
      name: String,
      @option(
        prefix = "--from",
        description = "Start of the backfill range (inclusive): yyyy-MM-dd or yyyy-MM-ddTHH:mm"
      )
      from: String,
      @option(
        prefix = "--to",
        description = "End of the backfill range (inclusive); defaults to the current time"
      )
      to: Option[String] = None
  ): Unit =
    val (flowName, args) = parseFlowCall(name)
    withFlows(flowOption) { (flows, compileResult, workEnv) =>
      val (unit, flow) = findFlow(flows, flowName)
      val schedule     = FlowScheduleConfig.fromFlow(flow)
      val cron         = CronSchedule.parse(
        schedule
          .cron
          .getOrElse(
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(
                s"Flow '${flowName}' has no schedule: config. Backfill derives its run windows from the cron schedule"
              )
          )
      )
      val zone     = schedule.zoneId
      val fromTime = parseBackfillTime(from, zone, endOfDay = false)
      val toTime   = to
        .map(parseBackfillTime(_, zone, endOfDay = true))
        .getOrElse(java.time.ZonedDateTime.now(zone))
      if fromTime.isAfter(toTime) then
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"--from (${fromTime}) is after --to (${toTime})")

      // Schedule fire times within [fromTime, toTime]. The truncated start only counts when it
      // does not precede --from (e.g. --from 00:00:05 must not include the 00:00:00 fire)
      val windows = List.newBuilder[java.time.ZonedDateTime]
      var t       =
        val start = fromTime.truncatedTo(java.time.temporal.ChronoUnit.MINUTES)
        if cron.matches(start) && !start.isBefore(fromTime) then
          start
        else
          cron.nextAfter(fromTime)
      while !t.isAfter(toTime) do
        windows += t
        t = cron.nextAfter(t)
      val runTimes = windows.result()

      if runTimes.isEmpty then
        println(s"No schedule windows of ${flowName} between ${fromTime} and ${toTime}")
      else
        println(
          s"Backfilling ${flowName}: ${runTimes.size} run(s) from ${runTimes.head} to ${runTimes
              .last}"
        )
        val profile = Profile.getProfile(flowOption.profile, flowOption.catalog, flowOption.schema)
        // System.exit is deferred until the resource blocks release the connector and run store
        var failed = false
        Control.withResource(DBConnectorProvider(workEnv)) { dbConnectorProvider =>
          val connector = dbConnectorProvider.getConnector(profile)

          given ctx: Context = compileResult
            .context
            .withCompilationUnit(unit)
            .newContext(Symbol.NoSymbol)

          Control.withResource(newRunStore(flowOption, workEnv)) { store =>
            val executor = FlowExecutor(connector, workEnv, registry = Some(store))
            val it       = runTimes.iterator
            while !failed && it.hasNext do
              val runTime = it.next()
              println(s"=== ${flowName} @ ${runTime}")
              val result = executor.execute(flow, args = args, runTime = Some(runTime))
              println(result.toPrettyBox())
              if result.hasError then
                // Later windows often depend on earlier ones, so stop at the first failure
                failed = true
                println(
                  s"Backfill of ${flowName} aborted at window ${runTime}; resolve the failure and re-run with --from ${runTime
                      .toLocalDate}"
                )
          }
        }
        if failed && !WvletMain.isInSbt then
          System.exit(1)
      end if
    }

  end backfill

  /** Parse a --from/--to argument as a date (yyyy-MM-dd) or local date-time in the flow's zone */
  private def parseBackfillTime(
      s: String,
      zone: java.time.ZoneId,
      endOfDay: Boolean
  ): java.time.ZonedDateTime =
    try
      val date = java.time.LocalDate.parse(s)
      if endOfDay then
        date.plusDays(1).atStartOfDay(zone).minusSeconds(1)
      else
        date.atStartOfDay(zone)
    catch
      case _: java.time.format.DateTimeParseException =>
        try
          java.time.LocalDateTime.parse(s).atZone(zone)
        catch
          case _: java.time.format.DateTimeParseException =>
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(s"Cannot parse time '${s}'. Use yyyy-MM-dd or yyyy-MM-ddTHH:mm[:ss]")

  @command(description = "List flows defined in the working folder")
  def list(flowOption: WvletFlowOption): Unit =
    withFlows(flowOption) { (flows, _, _) =>
      flows.foreach { (unit, f) =>
        val params =
          if f.params.isEmpty then
            ""
          else
            s"(${f.params.map(p => s"${p.name.name}").mkString(", ")})"
        val dependency =
          f.dependency match
            case Some(DependsOnFlow(flowName, _)) =>
              s" depends on ${flowName.fullName}"
            case Some(FlowStatePredicate(flowName, stateName, _)) =>
              s" if ${flowName.fullName}.${stateName}"
            case None =>
              ""
        val schedule = f
          .config
          .find(_.key.unquotedValue == "schedule")
          .map(c => s" [schedule: ${c.value}]")
          .getOrElse("")
        println(
          f"${f.name.name}%-30s ${f.stages.size}%2d stages${params}${dependency}${schedule} - ${unit
              .sourceFile
              .relativeFilePath}"
        )
      }
    }

  @command(description =
    "Manage flow run sessions: session list | show <run_id> | cancel <run_id> | resume <run_id> | clean [--stale]"
  )
  def session(
      flowOption: WvletFlowOption,
      @argument(description = "Sub command: list | show | cancel | resume | clean")
      sub: String = "list",
      @argument(description = "Run id (required for show, cancel, and resume)")
      runId: Option[String] = None,
      @option(
        prefix = "--stale",
        description =
          "With clean: also remove running records whose liveness lease expired (crashed runs)"
      )
      stale: Boolean = false
  ): Unit =
    val workEnv = WorkEnv(flowOption.workFolder, opts.logLevel)

    def fmtTime(millis: Long): String        = java.time.Instant.ofEpochMilli(millis).toString
    def fmtElapsed(r: FlowRunRecord): String = r
      .finishedAtMillis
      .map(f => s"${f - r.startedAtMillis}ms")
      .getOrElse("-")

    def requireRunId(usage: String): String = runId.getOrElse(
      throw StatusCode.INVALID_ARGUMENT.newException(s"Usage: wvlet flow session ${usage}")
    )

    Control.withResource(newRunStore(flowOption, workEnv)) { registry =>
      def recordOf(id: String): FlowRunRecord = registry
        .get(id)
        .getOrElse(throw StatusCode.INVALID_ARGUMENT.newException(s"Flow run '${id}' is not found"))

      sub match
        case "list" =>
          val now = System.currentTimeMillis()
          registry
            .list()
            .foreach { r =>
              val state =
                if r.isStaleAt(now) then
                  s"${r.state} (stale)"
                else
                  r.state
              println(
                f"${r.runId}%-28s ${r.flowName}%-24s ${state}%-10s started: ${fmtTime(
                    r.startedAtMillis
                  )} (${fmtElapsed(r)})"
              )
            }
        case "show" =>
          val r = recordOf(requireRunId("show <run_id>"))
          println(s"run:      ${r.runId}")
          println(s"flow:     ${r.flowName}")
          println(s"state:    ${r.state}")
          println(s"started:  ${fmtTime(r.startedAtMillis)}")
          r.finishedAtMillis.foreach(f => println(s"finished: ${fmtTime(f)}"))
          r.stages
            .foreach { s =>
              val err = s.error.map(e => s" - ${e}").getOrElse("")
              println(f"  stage ${s.name}%-24s ${s.state}%-14s attempts: ${s.attempts}${err}")
            }
        case "cancel" =>
          val id = requireRunId("cancel <run_id>")
          val r  = recordOf(id)
          if r.isTerminal then
            println(s"Run ${id} is already ${r.state}")
          else
            registry.requestCancel(id)
            println(s"Requested cancellation of run ${id}")
        case "resume" =>
          val id = requireRunId("resume <run_id>")
          val r  = recordOf(id)
          r.state match
            // A stale running record belongs to a crashed process and can be resumed
            case FlowRunRecord.STATE_RUNNING if !r.isStaleAt(System.currentTimeMillis()) =>
              throw StatusCode
                .INVALID_ARGUMENT
                .newException(s"Run ${id} is still running and cannot be resumed")
            case FlowRunRecord.STATE_SUCCESS =>
              println(s"Run ${id} already succeeded; nothing to resume")
            case FlowRunRecord.STATE_SKIPPED =>
              throw StatusCode
                .INVALID_ARGUMENT
                .newException(
                  s"Run ${id} was skipped (its dependency was not satisfied). Use wvlet flow run ${r
                      .flowName} to start a new run"
                )
            case _ =>
              executeFlow(flowOption, r.flowName, resumeFrom = Some(r))
        case "clean" =>
          // Remove terminal run records and drop their run-scoped tables. Running flows are kept
          // unless --stale is given, which also removes runs whose liveness lease expired
          // (their process died mid-run and left the record in the running state)
          val now         = System.currentTimeMillis()
          val cleanedRuns = registry.list().filter(r => r.isTerminal || (stale && r.isStaleAt(now)))
          val profile     = Profile.getProfile(
            flowOption.profile,
            flowOption.catalog,
            flowOption.schema
          )
          Control.withResource(DBConnectorProvider(workEnv)) { dbConnectorProvider =>
            val connector = dbConnectorProvider.getConnector(profile)
            cleanedRuns.foreach { r =>
              FlowExecutor.dropRunTables(connector, r.runId, r.stages.map(_.name))
              registry.delete(r.runId)
            }
          }
          println(s"Removed ${cleanedRuns.size} flow run record(s)")
        case other =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(
              s"Unknown session sub command: ${other}. Use list, show, cancel, resume, or clean"
            )
      end match
    }
  end session

  @command(description = "Start the scheduler daemon that runs flows on their cron schedules")
  def scheduler(
      flowOption: WvletFlowOption,
      @option(
        prefix = "--once",
        description = "Evaluate the schedules once against the current minute and exit"
      )
      once: Boolean = false,
      @option(
        prefix = "--catchup",
        description =
          "On startup, trigger flows whose most recent scheduled fire time has no recorded run"
      )
      catchup: Boolean = false,
      @option(
        prefix = "--reload-interval",
        description = "Interval (seconds) for polling .wv file changes; 0 disables reloading"
      )
      reloadInterval: Int = 5,
      @option(
        prefix = "--retention",
        description =
          "Delete terminal run records (and their run tables) older than this age, e.g. 7d, 12h, 30m"
      )
      retention: Option[String] = None
  ): Unit =
    // Validate the retention argument up front, before any flow is loaded or triggered
    val ttlMillis                       = retention.map(parseRetentionMillis)
    val (flows, compileResult, workEnv) = loadFlows(flowOption)
    val scheduled                       = scheduledFlowsOf(flows)
    if scheduled.isEmpty then
      println("No flows with a schedule: config were found")
    else
      val profile = Profile.getProfile(flowOption.profile, flowOption.catalog, flowOption.schema)
      Control.withResource(DBConnectorProvider(workEnv)) { dbConnectorProvider =>
        val connector = dbConnectorProvider.getConnector(profile)
        Control.withResource(newRunStore(flowOption, workEnv)) { store =>
          // The compile result and defining unit of each scheduled flow, replaced on reload
          val compiled = java
            .util
            .concurrent
            .atomic
            .AtomicReference((compileResult, scheduled.map((unit, sf) => sf.name -> unit).toMap))
          // Each triggered flow runs on its own thread so that a long flow does not delay
          // other schedules; the executor enforces dependencies and concurrency limits
          val pool = java
            .util
            .concurrent
            .Executors
            .newCachedThreadPool { (r: Runnable) =>
              val t = Thread(r, "wvlet-flow-scheduler-run")
              t.setDaemon(true)
              t
            }
          val flowScheduler = FlowScheduler(
            scheduled.map(_._2),
            trigger =
              (flow, fireTime) =>
                pool.submit(
                  new Runnable:
                    override def run(): Unit =
                      val (result, unitOf) = compiled.get()

                      given ctx: Context = result
                        .context
                        .withCompilationUnit(unitOf(flow.name.name))
                        .newContext(Symbol.NoSymbol)

                      // The schedule fire time is the logical run time: catch-up runs recompute
                      // the window they missed instead of the current one
                      val runResult = FlowExecutor(connector, workEnv, registry = Some(store))
                        .execute(flow, runTime = Some(fireTime))
                      println(runResult.toPrettyBox())
                )
          )
          // Retention: the per-flow keep_runs: caps plus the daemon-wide --retention TTL.
          // The sweep runs at startup (finalizing stale records frees concurrency slots
          // before any schedule fires) and periodically in daemon mode
          val keepRunsOf: Map[String, Option[Int]] =
            flows.map((_, f) => f.name.name -> FlowScheduleConfig.fromFlow(f).keepRuns).toMap
          val retentionActive  = ttlMillis.isDefined || keepRunsOf.values.exists(_.isDefined)
          def sweepNow(): Unit = FlowRunRetention.sweep(
            store,
            connector,
            name => keepRunsOf.getOrElse(name, None),
            ttlMillis
          )
          if retentionActive then
            sweepNow()

          if catchup then
            val fired = flowScheduler.catchUp(name =>
              store.latestRunOf(name).map(_.startedAtMillis)
            )
            if fired.nonEmpty then
              println(s"Catch-up triggered ${fired.size} missed flow(s): ${fired.mkString(", ")}")
          if once then
            // Evaluate the current minute once and wait for the triggered runs to finish
            val fired = flowScheduler.runOnce()
            println(s"Triggered ${fired.size} scheduled flow(s)")
            pool.shutdown()
            pool.awaitTermination(365, java.util.concurrent.TimeUnit.DAYS)
          else
            if reloadInterval > 0 then
              startReloader(flowOption, flowScheduler, compiled, reloadInterval)
            if retentionActive then
              startSweeper(sweepNow)
            Runtime
              .getRuntime
              .addShutdownHook(
                Thread { () =>
                  flowScheduler.stop()
                }
              )
            try flowScheduler.runLoop()
            finally pool.shutdownNow()
          end if
        }
      }
    end if

  end scheduler

  /** Parse a --retention argument like 7d, 12h, 30m, or 45s into milliseconds */
  private def parseRetentionMillis(s: String): Long =
    val m = "^(\\d+)([dhms])$".r
    s.trim match
      case m(n, unit) =>
        val base = n.toLong
        unit match
          case "d" =>
            base * 24L * 3600_000L
          case "h" =>
            base * 3600_000L
          case "m" =>
            base * 60_000L
          case _ =>
            base * 1000L
      case _ =>
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Cannot parse retention '${s}'. Use a number with d/h/m/s, e.g. 7d")

  /** Start a daemon thread running the retention sweep periodically (every 5 minutes) */
  private def startSweeper(sweep: () => Unit): Unit =
    val sweeper = Thread(
      () =>
        while true do
          Thread.sleep(5 * 60 * 1000L)
          try
            sweep()
          catch
            case scala.util.control.NonFatal(e) =>
              warn(s"Retention sweep failed: ${e.getMessage}")
      ,
      "wvlet-flow-retention-sweep"
    )
    sweeper.setDaemon(true)
    sweeper.start()

  /**
    * Start a daemon thread that polls the .wv sources of the working folder and reloads the
    * schedules when they change. A compile failure keeps the previously loaded schedules
    */
  private def startReloader(
      flowOption: WvletFlowOption,
      flowScheduler: FlowScheduler,
      compiled: java.util.concurrent.atomic.AtomicReference[
        (CompileResult, Map[String, CompilationUnit])
      ],
      reloadIntervalSeconds: Int
  ): Unit =
    val reloader = Thread(
      () =>
        var snapshot = sourceSnapshot(flowOption.workFolder)
        while true do
          Thread.sleep(reloadIntervalSeconds * 1000L)
          val latest = sourceSnapshot(flowOption.workFolder)
          if latest != snapshot then
            snapshot = latest
            try
              val (newFlows, newResult, _) = loadFlows(flowOption)
              val newScheduled             = scheduledFlowsOf(newFlows)
              compiled.set((newResult, newScheduled.map((unit, sf) => sf.name -> unit).toMap))
              flowScheduler.reload(newScheduled.map(_._2))
            catch
              case scala.util.control.NonFatal(e) =>
                warn(s"Failed to reload flows; keeping the current schedules: ${e.getMessage}")
      ,
      "wvlet-flow-scheduler-reload"
    )
    reloader.setDaemon(true)
    reloader.start()

  @command(description = "Show the plan of a flow")
  def show(
      flowOption: WvletFlowOption,
      @argument(description = "Name of the flow to show")
      name: String
  ): Unit =
    withFlows(flowOption) { (flows, compileResult, _) =>
      val (unit, flow) = findFlow(flows, name)

      given Context = compileResult.context.withCompilationUnit(unit)

      println(flow.pp)
    }

  /**
    * Compile all .wv files in the working folder and collect flow definitions with their defining
    * units
    */
  private def withFlows[A](flowOption: WvletFlowOption)(
      body: (List[(CompilationUnit, FlowDef)], CompileResult, WorkEnv) => A
  ): A =
    val (flows, compileResult, workEnv) = loadFlows(flowOption)
    body(flows, compileResult, workEnv)

  /** Compile all .wv files in the working folder and collect flow definitions */
  private def loadFlows(
      flowOption: WvletFlowOption
  ): (List[(CompilationUnit, FlowDef)], CompileResult, WorkEnv) =
    val workEnv  = WorkEnv(flowOption.workFolder, opts.logLevel)
    val compiler = Compiler(
      CompilerOptions(sourceFolders = List(flowOption.workFolder), workEnv = workEnv)
    )
    val compileResult = compiler.compile()
    val flows         = List.newBuilder[(CompilationUnit, FlowDef)]
    compileResult
      .units
      .foreach { unit =>
        unit
          .resolvedPlan
          .traverse { case f: FlowDef =>
            flows += unit -> f
          }
      }
    (flows.result(), compileResult, workEnv)

  /** Extract the flows that have a cron schedule, paired with their defining units */
  private def scheduledFlowsOf(
      flows: List[(CompilationUnit, FlowDef)]
  ): List[(CompilationUnit, ScheduledFlow)] = flows.flatMap { (unit, f) =>
    val config = FlowScheduleConfig.fromFlow(f)
    config
      .cron
      .map { cronExpr =>
        (unit, ScheduledFlow(f, CronSchedule.parse(cronExpr), config.zoneId))
      }
  }

  /**
    * Snapshot of the .wv sources under the folder (path -> last modified millis), used to detect
    * changes for scheduler reloading
    */
  private def sourceSnapshot(folder: String): Map[String, Long] =
    val root = java.nio.file.Path.of(folder)
    if !java.nio.file.Files.isDirectory(root) then
      Map.empty
    else
      Control.withResource(java.nio.file.Files.walk(root)) { stream =>
        import scala.jdk.CollectionConverters.*
        stream
          .iterator()
          .asScala
          .filter(p => p.toString.endsWith(".wv") && java.nio.file.Files.isRegularFile(p))
          .map(p => p.toString -> java.nio.file.Files.getLastModifiedTime(p).toMillis)
          .toMap
      }

  private def findFlow(
      flows: List[(CompilationUnit, FlowDef)],
      name: String
  ): (CompilationUnit, FlowDef) = flows
    .find(_._2.name.name == name)
    .getOrElse(
      throw StatusCode
        .FLOW_NOT_FOUND
        .newException(
          s"Flow '${name}' is not found. Available flows: ${flows
              .map(_._2.name.name)
              .mkString(", ")}"
        )
    )

end WvletFlowCommand
