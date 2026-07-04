package wvlet.lang.compiler.typer

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.DataType.SchemaType
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
