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
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.FlowDef
import wvlet.uni.log.LogSupport

import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Flow-level orchestration configuration extracted from the flow's `with { ... }` block
  *
  * @param cron
  *   Cron expression of `schedule: cron('...')`
  * @param timezone
  *   Time zone in which the cron expression is evaluated (defaults to the system zone)
  * @param concurrency
  *   Maximum number of concurrent runs of this flow
  */
case class FlowScheduleConfig(
    cron: Option[String] = None,
    timezone: Option[String] = None,
    concurrency: Option[Int] = None
):
  def withCron(cron: String): FlowScheduleConfig      = copy(cron = Some(cron))
  def noCron(): FlowScheduleConfig                    = copy(cron = None)
  def withTimezone(tz: String): FlowScheduleConfig    = copy(timezone = Some(tz))
  def noTimezone(): FlowScheduleConfig                = copy(timezone = None)
  def withConcurrency(limit: Int): FlowScheduleConfig = copy(concurrency = Some(limit))
  def noConcurrency(): FlowScheduleConfig             = copy(concurrency = None)

  def zoneId: ZoneId = timezone.map(ZoneId.of).getOrElse(ZoneId.systemDefault())

object FlowScheduleConfig:
  /** Extract schedule, timezone, and concurrency from the flow's config items */
  def fromFlow(flow: FlowDef): FlowScheduleConfig =
    flow
      .config
      .foldLeft(FlowScheduleConfig()) { (config, item) =>
        item.key.unquotedValue match
          case "schedule" =>
            item.value match
              // schedule: cron('0 2 * * *')
              case f: FunctionApply =>
                val isCron =
                  f.base match
                    case n: NameExpr =>
                      n.fullName == "cron"
                    case _ =>
                      false
                f.args.map(_.value) match
                  case List(s: StringLiteral) if isCron =>
                    config.withCron(s.unquotedValue)
                  case _ =>
                    config
              // schedule: '0 2 * * *'
              case s: StringLiteral =>
                config.withCron(s.unquotedValue)
              case _ =>
                config
          case "timezone" =>
            item.value match
              case s: StringLiteral =>
                config.withTimezone(s.unquotedValue)
              case _ =>
                config
          case "concurrency" =>
            item.value match
              case l: LongLiteral =>
                config.withConcurrency(l.value.toInt)
              case _ =>
                config
          case _ =>
            config
      }

end FlowScheduleConfig

/** A flow with a parsed cron schedule, ready to be evaluated by the scheduler */
case class ScheduledFlow(flow: FlowDef, cron: CronSchedule, zone: ZoneId):
  def name: String = flow.name.name

/**
  * Evaluates cron schedules of flows and triggers due flow runs.
  *
  * The scheduler only decides *when* to trigger a run; whether the run actually executes is decided
  * by the executor, which evaluates cross-flow dependencies and enforces the flow-level
  * `concurrency:` limit through the run store. The `trigger` function is invoked on the scheduler's
  * own thread; callers who want overlapping flows should dispatch to a worker pool inside
  * `trigger`.
  *
  * @param scheduled
  *   The flows with cron schedules to evaluate
  * @param trigger
  *   Invoked for each flow whose schedule is due, with the schedule fire time of the run (bound as
  *   the `run_time` of the triggered run)
  * @param clock
  *   Time source (injectable for testing)
  */
class FlowScheduler(
    scheduled: List[ScheduledFlow],
    trigger: (FlowDef, ZonedDateTime) => Unit,
    clock: () => Instant = () => Instant.now()
) extends LogSupport:

  @volatile
  private var current: List[ScheduledFlow] = scheduled

  private val nextFire = mutable.Map.empty[String, ZonedDateTime]

  @volatile
  private var stopped = false

  def stop(): Unit = stopped = true

  /** The currently scheduled flows (replaced by [[reload]]) */
  def scheduledFlows: List[ScheduledFlow] = current

  private def fire(sf: ScheduledFlow, fireTime: ZonedDateTime, reason: String): Unit =
    info(s"Triggering scheduled flow ${sf.name} (${reason})")
    try
      trigger(sf.flow, fireTime)
    catch
      case NonFatal(e) =>
        warn(s"Failed to trigger flow ${sf.name}: ${e.getMessage}")

  /**
    * Evaluate all schedules once: trigger the flows whose fire time has passed and return the
    * milliseconds until the earliest next fire
    */
  def tick(): Long = synchronized {
    val now = clock()
    current.foreach { sf =>
      val zonedNow = now.atZone(sf.zone)
      val due      = nextFire.getOrElseUpdate(sf.name, sf.cron.nextAfter(zonedNow))
      if !due.toInstant.isAfter(now) then
        fire(sf, due, s"due: ${due}")
        nextFire(sf.name) = sf.cron.nextAfter(zonedNow)
    }
    val nowMillis = clock().toEpochMilli
    nextFire
      .values
      .map(t => (t.toInstant.toEpochMilli - nowMillis).max(0L))
      .minOption
      .getOrElse(60000L)
  }

  /**
    * Replace the scheduled flows (e.g. after the .wv files of the working folder changed). Flows
    * whose cron expression and time zone are unchanged keep their pending fire time; new or
    * rescheduled flows are evaluated from the next tick
    */
  def reload(newScheduled: List[ScheduledFlow]): Unit = synchronized {
    val oldByName = current.map(sf => sf.name -> sf).toMap
    newScheduled.foreach { sf =>
      oldByName.get(sf.name) match
        case Some(prev) if prev.cron.expression == sf.cron.expression && prev.zone == sf.zone =>
        // The schedule is unchanged: keep the pending fire time
        case _ =>
          nextFire.remove(sf.name)
    }
    (oldByName.keySet -- newScheduled.map(_.name)).foreach(nextFire.remove)
    current = newScheduled
    info(
      s"Reloaded ${newScheduled.size} scheduled flow(s): ${newScheduled
          .map(sf => s"${sf.name} [${sf.cron.expression}]")
          .mkString(", ")}"
    )
  }

  /**
    * Trigger every flow whose cron expression matches the current minute and return their names.
    * Used by `wvlet flow scheduler --once`, where an external scheduler (e.g. system cron) decides
    * when to evaluate the schedules
    */
  def runOnce(): List[String] = synchronized {
    val now   = clock()
    val fired = current.filter(sf => sf.cron.matches(now.atZone(sf.zone)))
    fired.foreach { sf =>
      fire(sf, now.atZone(sf.zone).truncatedTo(java.time.temporal.ChronoUnit.MINUTES), "once")
    }
    fired.map(_.name)
  }

  /**
    * Trigger the flows whose most recent scheduled fire time has no run recorded at or after it
    * (i.e. the fire was missed, e.g. while the scheduler daemon was down) and return their names.
    * `lastRunStartedAt` supplies the start time of the latest recorded run of a flow
    */
  def catchUp(lastRunStartedAt: String => Option[Long]): List[String] = synchronized {
    val now    = clock()
    val missed = current.flatMap { sf =>
      try
        val prev = sf.cron.prevAtOrBefore(now.atZone(sf.zone))
        if lastRunStartedAt(sf.name).forall(_ < prev.toInstant.toEpochMilli) then
          Some((sf, prev))
        else
          None
      catch
        case NonFatal(_) =>
          // The cron expression has no fire time within the scan window: nothing was missed
          None
    }
    // The missed fire time becomes the run_time of the catch-up run, so that the run computes
    // the window it originally missed instead of the current one
    missed.foreach((sf, prev) => fire(sf, prev, s"catch-up: ${prev}"))
    missed.map(_._1.name)
  }

  /** Run the scheduler loop until stop() is called */
  def runLoop(): Unit =
    if current.isEmpty then
      throw StatusCode.INVALID_ARGUMENT.newException("No scheduled flows to run")
    info(
      s"Flow scheduler started with ${current.size} scheduled flow(s): ${current
          .map(sf => s"${sf.name} [${sf.cron.expression}]")
          .mkString(", ")}"
    )
    while !stopped do
      val delay = tick()
      // Sleep in bounded slices so that stop() is honored promptly
      Thread.sleep(delay.min(1000L).max(50L))

end FlowScheduler
