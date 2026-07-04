package wvlet.lang.compiler.transform

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.model.expr.SingleQuoteString
import wvlet.lang.model.plan.FlowDef
import wvlet.lang.model.plan.RunFlow
import wvlet.lang.model.plan.TableRef
import wvlet.uni.test.UniTest

class FlowParamsTest extends UniTest:

  private def parseFlow(wv: String): FlowDef =
    val plan                  = ParserPhase.parseOnly(CompilationUnit.fromWvletString(wv))
    var flow: Option[FlowDef] = None
    plan.traverse { case f: FlowDef =>
      if flow.isEmpty then
        flow = Some(f)
    }
    flow.getOrElse(fail("No FlowDef found"))

  private def parseRunFlow(wv: String): RunFlow =
    val plan                 = ParserPhase.parseOnly(CompilationUnit.fromWvletString(wv))
    var run: Option[RunFlow] = None
    plan.traverse { case q: wvlet.lang.model.plan.Query =>
      q.child match
        case r: RunFlow =>
          run = Some(r)
        case _ =>
    }
    run.getOrElse(fail("No RunFlow found"))

  private val flow = parseFlow("""flow F(segment: string, min_id: int = 1) = {
      |  stage src = from users | where name = segment and id >= min_id
      |}""".stripMargin)

  test("bind named arguments and apply defaults") {
    val bindings = FlowParams.bind(flow, parseRunFlow("run flow F(segment = 'a')").args)
    bindings.keySet shouldBe Set("segment", "min_id")
    bindings("segment").asInstanceOf[SingleQuoteString].unquotedValue shouldBe "a"
  }

  test("bind positional arguments in declaration order") {
    val bindings = FlowParams.bind(flow, parseRunFlow("run flow F('a', 5)").args)
    bindings("segment").asInstanceOf[SingleQuoteString].unquotedValue shouldBe "a"
  }

  test("reject a duplicate named argument") {
    val e = intercept[WvletLangException] {
      FlowParams.bind(flow, parseRunFlow("run flow F(segment = 'a', segment = 'b')").args)
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    e.getMessage shouldContain "Duplicate"
  }

  test("reject a positional argument after a named argument") {
    val e = intercept[WvletLangException] {
      FlowParams.bind(flow, parseRunFlow("run flow F(segment = 'a', 5)").args)
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    e.getMessage shouldContain "Positional argument"
  }

  test("reject too many arguments") {
    val e = intercept[WvletLangException] {
      FlowParams.bind(flow, parseRunFlow("run flow F('a', 5, 'extra')").args)
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    e.getMessage shouldContain "Too many arguments"
  }

  test("substitute parameter references but never table references") {
    // The parameter shares its name with the source table: the filter reference is
    // substituted while the `from` clause keeps referencing the table
    val f = parseFlow("""flow G(users: string) = {
        |  stage src = from users | where name = users
        |}""".stripMargin)
    val bound = FlowParams.substitute(
      f,
      FlowParams.bind(f, parseRunFlow("run flow G(users = 'a')").args)
    )
    val body               = bound.stages.head.body.get
    var tableNames         = List.empty[String]
    var substitutedLiteral = false
    body.traverse { case t: TableRef =>
      tableNames = tableNames :+ t.name.fullName
    }
    body.traverseExpressions { case s: SingleQuoteString =>
      substitutedLiteral = true
    }
    tableNames shouldBe List("users")
    substitutedLiteral shouldBe true
  }

end FlowParamsTest
