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
  *   Invoked for each flow whose schedule is due
  * @param clock
  *   Time source (injectable for testing)
  */
class FlowScheduler(
    scheduled: List[ScheduledFlow],
    trigger: FlowDef => Unit,
    clock: () => Instant = () => Instant.now()
) extends LogSupport:

  private val nextFire = mutable.Map.empty[String, ZonedDateTime]

  @volatile
  private var stopped = false

  def stop(): Unit = stopped = true

  /**
    * Evaluate all schedules once: trigger the flows whose fire time has passed and return the
    * milliseconds until the earliest next fire
    */
  def tick(): Long =
    val now = clock()
    scheduled.foreach { sf =>
      val zonedNow = now.atZone(sf.zone)
      val due      = nextFire.getOrElseUpdate(sf.name, sf.cron.nextAfter(zonedNow))
      if !due.toInstant.isAfter(now) then
        info(s"Triggering scheduled flow ${sf.name} (due: ${due})")
        try
          trigger(sf.flow)
        catch
          case NonFatal(e) =>
            warn(s"Failed to trigger flow ${sf.name}: ${e.getMessage}")
        nextFire(sf.name) = sf.cron.nextAfter(zonedNow)
    }
    val nowMillis = clock().toEpochMilli
    nextFire
      .values
      .map(t => (t.toInstant.toEpochMilli - nowMillis).max(0L))
      .minOption
      .getOrElse(60000L)

  /** Run the scheduler loop until stop() is called */
  def runLoop(): Unit =
    if scheduled.isEmpty then
      throw StatusCode.INVALID_ARGUMENT.newException("No scheduled flows to run")
    info(
      s"Flow scheduler started with ${scheduled.size} scheduled flow(s): ${scheduled
          .map(sf => s"${sf.name} [${sf.cron.expression}]")
          .mkString(", ")}"
    )
    while !stopped do
      val delay = tick()
      // Sleep in bounded slices so that stop() is honored promptly
      Thread.sleep(delay.min(1000L).max(50L))

end FlowScheduler
