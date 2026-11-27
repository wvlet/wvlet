package wvlet.lang.compiler.codegen

import wvlet.uni.test.UniTest
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.parser.ParserPhase

/**
  * `limit` takes a constant integer expression (a val, a model argument, or a def parameter), and
  * the generated SQL always carries a literal row count
  */
class LimitExprTest extends UniTest:

  private def generateSQL(wv: String): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    GenSQL.generateSQL(unit)(using result.context)

  private def limitError(wv: String): WvletLangException =
    val e = intercept[WvletLangException] {
      generateSQL(wv)
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    e.getMessage shouldContain "limit requires a constant non-negative integer"
    e

  test("should generate a literal limit from a val") {
    val sql = generateSQL("""val lim_n = 5
        |from [[1], [2]] as t(x)
        |limit lim_n
        |""".stripMargin)
    sql shouldContain "limit 5"
    sql shouldNotContain "lim_n"
  }

  test("should generate a literal limit from a def parameter") {
    val sql = generateSQL("""def lim_top(n: int) = order by x desc | limit n
        |from [[1], [2]] as t(x)
        || lim_top(3)
        |""".stripMargin)
    sql shouldContain "limit 3"
  }

  test("should generate a literal limit from a model argument") {
    val sql = generateSQL("""model lim_model(n: int) = {
        |  from [[1], [2]] as t(x)
        |  limit n
        |}
        |from lim_model(7)
        |""".stripMargin)
    sql shouldContain "limit 7"
  }

  test("should evaluate arithmetic over constants") {
    val sql = generateSQL("""val lim_n = 4
        |from [[1], [2]] as t(x)
        |limit (lim_n + 2) * 3 / 2
        |""".stripMargin)
    sql shouldContain "limit 9"
  }

  test("should generate a literal limit per for-loop iteration") {
    val sql = generateSQL("""for lim_i in [1, 2] {
        |  from [[1], [2]] as t(x)
        |  limit lim_i
        |}
        |""".stripMargin)
    sql shouldContain "limit 1"
    sql shouldContain "limit 2"
  }

  test("should keep a literal limit as is") {
    val sql = generateSQL("""from [[1], [2]] as t(x)
        |limit 1
        |""".stripMargin)
    sql shouldContain "limit 1"
  }

  test("should report a column reference in limit") {
    val e = limitError("""from [[1], [2]] as t(x)
        |limit x
        |""".stripMargin)
    e.getMessage shouldContain "'x'"
  }

  test("should report a non-integer limit") {
    limitError("""val lim_s = 'ten'
        |from [[1], [2]] as t(x)
        |limit lim_s
        |""".stripMargin)
  }

  test("should report a negative limit") {
    val e = limitError("""def lim_neg(n: int) = limit n
        |from [[1], [2]] as t(x)
        || lim_neg(-1)
        |""".stripMargin)
    e.getMessage shouldContain "-1"
  }

  test("should report a division with a remainder") {
    val e = limitError("""val lim_odd = 7
        |from [[1], [2]] as t(x)
        |limit lim_odd / 2
        |""".stripMargin)
    e.getMessage shouldContain "is not an integer"
  }

  test("should report an overflowing limit") {
    val e = limitError("""from [[1], [2]] as t(x)
        |limit 4611686018427387904 * 4
        |""".stripMargin)
    e.getMessage shouldContain "overflows"
  }

  test("should print a limit expression back as written") {
    val unit    = CompilationUnit.fromWvletString("from t\nlimit lim_n * 2")
    val printed = WvletGenerator().print(ParserPhase.parseOnly(unit))
    printed shouldContain "limit lim_n * 2"
  }

end LimitExprTest
