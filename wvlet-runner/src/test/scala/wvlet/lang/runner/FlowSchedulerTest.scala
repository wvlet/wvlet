package wvlet.lang.runner

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.plan.FlowDef
import wvlet.uni.test.UniTest

import java.time.Instant
import java.time.ZoneId
import scala.collection.mutable.ListBuffer

class FlowSchedulerTest extends UniTest:

  private def compileFlow(wv: String): FlowDef =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val unit     = CompilationUnit.fromWvletString(wv)
    compiler.compileSingleUnit(unit)
    var flow: Option[FlowDef] = None
    unit
      .resolvedPlan
      .traverse { case f: FlowDef =>
        if flow.isEmpty then
          flow = Some(f)
      }
    flow.getOrElse(fail("No FlowDef found"))

  test("extract schedule, timezone, and concurrency from the flow config") {
    val flow = compileFlow("""flow NightlyFlow with {
        |  schedule: cron('0 2 * * *')
        |  timezone: 'UTC'
        |  concurrency: 2
        |} = {
        |  stage src = from [[1]] as t(id)
        |}""".stripMargin)
    val config = FlowScheduleConfig.fromFlow(flow)
    config.cron shouldBe Some("0 2 * * *")
    config.timezone shouldBe Some("UTC")
    config.zoneId shouldBe ZoneId.of("UTC")
    config.concurrency shouldBe Some(2)
  }

  test("extract empty schedule config from an unconfigured flow") {
    val flow = compileFlow("""flow PlainFlow = {
        |  stage src = from [[1]] as t(id)
        |}""".stripMargin)
    val config = FlowScheduleConfig.fromFlow(flow)
    config.cron shouldBe None
    config.timezone shouldBe None
    config.concurrency shouldBe None
  }

  test("trigger due flows once per schedule fire") {
    val flow = compileFlow("""flow MinutelyFlow = {
        |  stage src = from [[1]] as t(id)
        |}""".stripMargin)
    val sf = ScheduledFlow(flow, CronSchedule.parse("* * * * *"), ZoneId.of("UTC"))

    var now       = Instant.parse("2026-01-01T00:00:30Z")
    val triggered = ListBuffer.empty[String]
    val scheduler = FlowScheduler(List(sf), f => triggered += f.name.name, () => now)

    // The first evaluation computes the next fire (00:01:00) without triggering
    val delay = scheduler.tick()
    triggered.toList shouldBe Nil
    delay shouldBe 30000L

    // Time passes the fire point: the flow triggers exactly once
    now = Instant.parse("2026-01-01T00:01:05Z")
    scheduler.tick()
    triggered.toList shouldBe List("MinutelyFlow")
    scheduler.tick()
    triggered.toList shouldBe List("MinutelyFlow")

    // The next minute boundary triggers again
    now = Instant.parse("2026-01-01T00:02:00Z")
    scheduler.tick()
    triggered.toList shouldBe List("MinutelyFlow", "MinutelyFlow")
  }

  test("keep the scheduler alive when a trigger fails") {
    val flow = compileFlow("""flow FailingTriggerFlow = {
        |  stage src = from [[1]] as t(id)
        |}""".stripMargin)
    val sf = ScheduledFlow(flow, CronSchedule.parse("* * * * *"), ZoneId.of("UTC"))

    var now       = Instant.parse("2026-01-01T00:00:30Z")
    var attempts  = 0
    val scheduler = FlowScheduler(
      List(sf),
      _ =>
        attempts += 1
        throw IllegalStateException("boom")
      ,
      () => now
    )
    scheduler.tick()
    attempts shouldBe 0
    now = Instant.parse("2026-01-01T00:01:00Z")
    scheduler.tick()
    attempts shouldBe 1
    // The schedule advances despite the failure
    now = Instant.parse("2026-01-01T00:02:00Z")
    scheduler.tick()
    attempts shouldBe 2
  }

end FlowSchedulerTest
