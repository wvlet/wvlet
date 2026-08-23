package wvlet.lang.compiler.typer

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.expr.DotRef
import wvlet.lang.model.plan.FlowDef
import wvlet.uni.test.UniTest

/**
  * Tests for typing flow definitions and their stages
  */
class FlowTypingTest extends UniTest:

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
    flow.getOrElse(fail("No FlowDef found in the resolved plan"))

  private val userType =
    """type users = {
      |  user_id: string
      |  name: string
      |  region: string
      |}
      |""".stripMargin

  test("resolve stage references across stages") {
    val f = compileFlow(s"""${userType}
        |flow SimpleFlow = {
        |  stage entry = from users
        |  stage output = from entry | select name
        |}""".stripMargin)
    val entry = f.stages.find(_.name.name == "entry").get
    entry.relationType.isResolved shouldBe true
    entry.relationType.fields.map(_.name.name) shouldContain "name"

    val output = f.stages.find(_.name.name == "output").get
    output.relationType.isResolved shouldBe true
    output.relationType.fields.map(_.name.name) shouldBe List("name")
  }

  test("resolve merge stage type from its sources") {
    val f = compileFlow(s"""${userType}
        |flow MergeFlow = {
        |  stage source_a = from users | where region = 'US'
        |  stage source_b = from users | where region = 'EU'
        |  stage merged = merge source_a, source_b
        |  stage output = from merged | select name
        |}""".stripMargin)
    val merged = f.stages.find(_.name.name == "merged").get
    merged.relationType.isResolved shouldBe true
    merged.relationType.fields.map(_.name.name) shouldContain "name"

    val output = f.stages.find(_.name.name == "output").get
    output.relationType.fields.map(_.name.name) shouldBe List("name")
  }

  test("report an error for a trigger referencing an undefined stage") {
    val e = intercept[WvletLangException] {
      compileFlow(s"""${userType}
          |flow BrokenTrigger = {
          |  stage entry = from users
          |  stage fallback if missing_stage.failed = from users
          |}""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.STAGE_NOT_FOUND
    e.getMessage shouldContain "missing_stage"
  }

  test("report an error for a merge referencing an undefined stage") {
    val e = intercept[WvletLangException] {
      compileFlow(s"""${userType}
          |flow BrokenMerge = {
          |  stage source_a = from users
          |  stage merged = merge source_a, missing_stage
          |}""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.STAGE_NOT_FOUND
    e.getMessage shouldContain "missing_stage"
  }

  test("allow route targets to reference stages defined later") {
    val f = compileFlow(s"""${userType}
        |flow ForwardRoute = {
        |  stage gate = from users | route {
        |    case _.user_id = '1' -> matched
        |    else -> unmatched
        |  }
        |  stage matched = from gate | select name
        |  stage unmatched = from gate | select name
        |}""".stripMargin)
    f.stages.size shouldBe 3
  }

  test("report an error for a route targeting an undefined stage") {
    val e = intercept[WvletLangException] {
      compileFlow(s"""${userType}
          |flow BrokenRoute = {
          |  stage gate = from users | route {
          |    case _.user_id = '1' -> missing_stage
          |  }
          |}""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.STAGE_NOT_FOUND
    e.getMessage shouldContain "missing_stage"
  }

  test("resolve aggregation shorthands inside stage bodies") {
    val f = compileFlow(s"""${userType}
        |flow AggFlow = {
        |  stage entry = from users
        |  stage counted = from entry | agg _.count as cnt
        |}""".stripMargin)
    val counted = f.stages.find(_.name.name == "counted").get
    counted.relationType.isResolved shouldBe true
    counted.relationType.fields.map(_.name.name) shouldBe List("cnt")
    // `_.count` must be inlined into count(*); no bare member reference survives in the body
    var bareCountRefs = 0
    counted
      .body
      .get
      .traverseExpressions {
        case d: DotRef if d.name.fullName == "count" =>
          bareCountRefs += 1
      }
    bareCountRefs shouldBe 0
  }

  test("resolve aggregation shorthands before an event sensor") {
    val f = compileFlow(s"""${userType}
        |flow SensorFlow = {
        |  stage entry = from users
        |  stage gate = from entry | agg _.count as cnt | wait until _.cnt > 1000
        |}""".stripMargin)
    val gate = f.stages.find(_.name.name == "gate").get
    gate.relationType.isResolved shouldBe true
    gate.relationType.fields.map(_.name.name) shouldBe List("cnt")
  }

  test("inline chained function calls inside stage bodies") {
    val f = compileFlow(s"""type events = {
        |  id: string
        |  amount: double
        |}
        |flow ChainFlow = {
        |  stage entry = from events
        |  stage rounded = from entry | select id, amount.round(1) as amt
        |}""".stripMargin)
    val rounded = f.stages.find(_.name.name == "rounded").get
    rounded.relationType.isResolved shouldBe true
    rounded.relationType.fields.map(_.name.name) shouldBe List("id", "amt")
  }

  test("report an error for a trigger referencing a stage defined later") {
    val e = intercept[WvletLangException] {
      compileFlow(s"""${userType}
          |flow ForwardTrigger = {
          |  stage alert if late.failed = from users
          |  stage late = from users
          |}""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.STAGE_NOT_FOUND
  }

end FlowTypingTest
