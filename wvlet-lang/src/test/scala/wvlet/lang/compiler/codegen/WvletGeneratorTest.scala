package wvlet.lang.compiler.codegen

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.compiler.CompilationUnit

class WvletGeneratorTest extends UniTest:

  private def print(wv: String): String =
    val unit = CompilationUnit.fromWvletString(wv)
    val plan = ParserPhase.parseOnly(unit)
    WvletGenerator().print(plan)

  test("should preserve names of named function arguments") {
    val printed = print("""from t
        |select approx_percentile(price, percentile = 0.95)""".stripMargin)
    printed shouldContain "percentile = 0.95"

    // The printed query should parse back to the same named argument
    val reprinted = print(printed)
    reprinted shouldContain "percentile = 0.95"
  }

  test("should print positional and named arguments together") {
    val printed = print("""from t
        |select f(a, b, mode = 'fast')""".stripMargin)
    printed shouldContain "f(a, b, mode = 'fast')"
  }

  test("should keep distinct arguments without names") {
    val printed = print("""from t
        |select count(distinct user_id)""".stripMargin)
    printed shouldContain "count(distinct user_id)"
  }

end WvletGeneratorTest
