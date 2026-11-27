package wvlet.lang.cli

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.Profile
import wvlet.uni.cli.launcher.Launcher
import wvlet.uni.test.UniTest

/** The option set shared by the npm, Native, and JVM `wvlet` commands */
class WvletCliOptionsTest extends UniTest:

  private def runCli(args: String*): String =
    val out = java.io.ByteArrayOutputStream()
    Console.withOut(out) {
      Launcher.of[WvletCli].execute(args.toArray)
    }
    out.toString

  test("use the default profile without -t or -p") {
    WvletCliCompileOption()
      .resolveProfile(Profile.defaultWvletProfile)
      .defaultEngine
      .`type` shouldBe "wvlet"
  }

  test("use the built-in profile of the -t engine") {
    val engine =
      WvletCliCompileOption(targetDBType = Some("duckdb"))
        .resolveProfile(Profile.defaultWvletProfile)
        .defaultEngine
    engine.`type` shouldBe "duckdb"
    engine.catalog shouldBe Some("memory")
  }

  test("override the default engine's catalog and schema") {
    val engine =
      WvletCliCompileOption(catalog = Some("c1"), schema = Some("s1"))
        .resolveProfile(Profile.defaultDuckDBProfile)
        .defaultEngine
    engine.catalog shouldBe Some("c1")
    engine.schema shouldBe Some("s1")
  }

  test("apply connection flags to the default engine") {
    val engine =
      WvletCliRunOption(host = Some("h"), port = Some(1234), useHttps = true)
        .applyTo(Profile.defaultWvletProfile)
        .defaultEngine
    engine.host shouldBe Some("h")
    engine.port shouldBe Some(1234)
    engine.useHttps shouldBe Some(true)
  }

  test("compile with --catalog and --strict") {
    val sql = runCli("compile", "--catalog", "c1", "--strict", "from [[1, 'a']] as t(id, name)")
    sql shouldContain "select"
  }

  test("print the last result as CSV") {
    val csv = runCli("run", "--format", "csv", "from [[1, 'a']] as t(id, name) select name")
    csv shouldContain "name"
    csv shouldContain "a"
  }

  test("reject an unknown --format") {
    val e = intercept[WvletLangException] {
      runCli("run", "--format", "json", "from [[1]] as t(id)")
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
  }

  test("run REPL commands with -c") {
    val out = runCli("repl", "-c", "select 1 as answer", "-c", "help")
    out shouldContain "wv> select 1 as answer"
    out shouldContain "answer"
    out shouldContain "[commands]"
  }

  test("keep saved tables across REPL commands") {
    val out = runCli(
      "repl",
      "-c",
      "from [[41]] as t(x) save to repl_saved",
      "-c",
      "from repl_saved select x + 1 as y"
    )
    out shouldContain "42"
  }

  test("stop REPL commands at exit") {
    val out = runCli("repl", "-c", "exit", "-c", "select 7 as not_run")
    out.contains("not_run") shouldBe false
  }

  test("treat --version as a successful exit") {
    val e = intercept[WvletLangException] {
      WvletCliGlobalOption(displayVersion = true)
    }
    WvletCliCommands.isSuccessfulExit(e) shouldBe true
  }

end WvletCliOptionsTest
