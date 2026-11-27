package wvlet.lang.cli

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.uni.test.UniTest

/**
  * The npm and Native `wvlet` name the JVM CLI for its commands (`ui`, `flow`, `catalog`)
  */
class JvmOnlyCommandTest extends UniTest:

  test("should name the JVM CLI for ui, flow, and catalog") {
    WvletCli
      .jvmOnlyCommands
      .foreach { cmd =>
        val e = intercept[WvletLangException] {
          WvletCli.rejectJvmOnlyCommand(Array(cmd, "--help"))
        }
        e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
        e.getMessage shouldContain s"wvlet ${cmd} is in the JVM CLI"
        e.getMessage shouldContain "usage/install#jvm-cli"
      }
  }

  test("should find the command after global options") {
    WvletCli.commandName(Array("-l", "debug", "--debug", "ui")) shouldBe Some("ui")
    WvletCli.commandName(Array("-L", "wvlet.*=debug", "flow", "run")) shouldBe Some("flow")
    WvletCli.commandName(Array("--version")) shouldBe None
  }

  test("should accept shared commands, including a query text that names a JVM command") {
    WvletCli.rejectJvmOnlyCommand(Array("run", "ui"))
    WvletCli.rejectJvmOnlyCommand(Array("compile", "-f", "flow.wv"))
    WvletCli.rejectJvmOnlyCommand(Array.empty)
  }

end JvmOnlyCommandTest
