package wvlet.lang.compiler.codegen

import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context
import wvlet.uni.test.UniTest

/**
  * Tests for rendering flow/stage definitions with LogicalPlanPrinter
  */
class FlowPlanPrinterTest extends UniTest:

  private def printPlan(wv: String): String =
    val unit      = CompilationUnit.fromWvletString(wv)
    val globalCtx = Context.testGlobalContext(".")

    given ctx: Context = globalCtx.getContextOf(unit)

    globalCtx.setContextUnit(unit)
    val plan = ParserPhase.parse(unit, ctx)
    plan.pp

  test("should print a simple flow with stages") {
    val p = printPlan("""flow SimpleFlow = {
        |  stage entry = from users
        |  stage output = from entry | select name
        |}""".stripMargin)
    debug(p)
    p shouldContain "FlowDef:"
    p shouldContain "SimpleFlow"
    p shouldContain "StageDef:"
    p shouldContain "entry"
    p shouldContain "output"
  }

  test("should print flow parameters and config") {
    val p = printPlan("""flow ParamFlow(segment: string) with {
        |  schedule: cron('0 2 * * *')
        |  concurrency: 1
        |} = {
        |  stage entry = from users | where segment_id = segment
        |}""".stripMargin)
    debug(p)
    p shouldContain "ParamFlow"
    p shouldContain "segment"
    p shouldContain "schedule:"
    p shouldContain "concurrency: 1"
  }

  test("should print stage triggers and config") {
    val p = printPlan("""flow TriggerFlow = {
        |  stage primary with { retries: 3, timeout: 5m } = from source
        |  stage fallback if primary.failed = from backup
        |  stage cleanup if primary.done and fallback.done = from primary | select *
        |}""".stripMargin)
    debug(p)
    p shouldContain "if primary.failed"
    p shouldContain "if primary.done and fallback.done"
    p shouldContain "retries: 3"
    p shouldContain "timeout: 5m"
  }

  test("should print flow dependencies") {
    val p1 = printPlan("""flow Downstream depends on Upstream = {
        |  stage report = from warehouse
        |}""".stripMargin)
    debug(p1)
    p1 shouldContain "depends on Upstream"

    val p2 = printPlan("""flow Recovery if Upstream.failed = {
        |  stage alert = from source
        |}""".stripMargin)
    debug(p2)
    p2 shouldContain "if Upstream.failed"
  }

  test("should print route cases") {
    val p = printPlan("""flow RouteFlow = {
        |  stage entry = from users
        |  stage check = from entry | route {
        |    case _.age > 18 -> adult
        |    else -> minor
        |  }
        |  stage adult = from check | select name
        |  stage minor = from check | select name
        |}""".stripMargin)
    debug(p)
    p shouldContain "FlowRoute:"
    p shouldContain "-> adult"
    p shouldContain "else -> minor"
  }

end FlowPlanPrinterTest
