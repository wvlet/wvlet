package wvlet.lang.cli

import wvlet.lang.catalog.Profile
import wvlet.lang.cli.terminal.UniTerminal
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.PlanScriptRunner
import wvlet.uni.cli.DisplayWidth
import wvlet.uni.cli.InMemoryTerminalIO
import wvlet.uni.cli.TerminalIO
import wvlet.uni.io.IO
import wvlet.uni.rx.Rx
import wvlet.uni.test.UniTest

/**
  * Drives the REPL of the npm and Native `wvlet` (the cross-platform script runner) with replayed
  * keystrokes, on the JVM, Node.js, and Native
  */
class SharedREPLTest extends UniTest:

  private case class ReplRun(io: InMemoryTerminalIO, workFolder: String):
    def screen: String = DisplayWidth.stripAnsi(io.output)

  private def runRepl(inputs: String*): Rx[ReplRun] =
    val workFolder = IO.createTempDirectory(prefix = "shared-repl-test").path
    val workEnv    = WorkEnv(path = workFolder)
    val io         = TerminalIO.inMemory(inputs.map(_.getBytes("UTF-8")))
    val runner     = PlanScriptRunner(workEnv, Profile.defaultWvletProfile, interactive = true)
    val repl       = WvletREPL(runner, UniTerminal(workEnv, io))
    repl
      .start()
      .map { _ =>
        repl.close()
        runner.close()
        ReplRun(io, workFolder)
      }

  test("read inputs until the end of input") {
    runRepl("select 1 as answer;\r").map { r =>
      r.screen shouldContain "wv> select 1 as answer;"
      r.io.isRawMode shouldBe false
    }
  }

  test("describe the query above the cursor with Ctrl-J Ctrl-D") {
    runRepl("from [[1, 'x']] as t(id, name)\n\u0004", "\u0003").map { r =>
      r.screen shouldContain "describe"
      r.screen shouldContain "name"
    }
  }

  test("run a subquery with Ctrl-J Ctrl-T") {
    runRepl("from [[1, 'subq']] as t(id, name)\n\u0014", "\u0003").map { r =>
      r.screen shouldContain "debug"
      r.screen shouldContain "subq"
    }
  }

  test("save inputs to the history file") {
    runRepl("select 4 as saved;\r").map { r =>
      val history = IO.path(WorkEnv(path = r.workFolder).cacheFolder, UniTerminal.HistoryFileName)
      IO.readString(history) shouldContain "select 4 as saved;"
    }
  }

  test("read many inputs without growing the stack") {
    runRepl(Seq.fill(1000)("\r")*).map { r =>
      r.io.isRawMode shouldBe false
    }
  }

end SharedREPLTest
