package wvlet.lang.cli

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.uni.test.UniTest

class WvletMainTest extends UniTest:
  private def capture(body: => Unit): String =
    val out = java.io.ByteArrayOutputStream()
    Console.withOut(out)(body)
    out.toString

  test("help") {
    WvletMain.main("--help")
    WvletMain.main("")
    WvletMain.main("version")
  }

  test("print the version for a bare wvlet without a terminal") {
    // Tests run without a terminal, so a bare `wvlet` must not wait for REPL input
    WvletMain.isInteractiveTerminal shouldBe false
    capture(WvletMain.main(Array.empty[String])) shouldContain "wvlet "
  }

  test("exit successfully with --version") {
    capture(WvletMain.main("--version")) shouldContain "wvlet version:"
  }

  test("run REPL commands with wvlet repl") {
    capture(
      WvletMain.main(Array("repl", "-c", "from [[1, 'a']] as t(id, name) select name"))
    ) shouldContain "name"
  }

  test("print the last result as CSV with run --format csv") {
    val csv = capture(
      WvletMain.main(Array("run", "--format", "csv", "from [[1, 'a']] as t(id, name) select name"))
    )
    csv shouldContain "name"
  }

  test("fail run when a test assertion fails") {
    val e = intercept[WvletLangException] {
      WvletMain.main(Array("run", "from [[1]] as t(x) test _.size should be 2"))
    }
    e.statusCode shouldBe StatusCode.TEST_FAILED
  }

  test("start server test") {
    WvletMain.main("server --quit-immediately")
  }

end WvletMainTest
