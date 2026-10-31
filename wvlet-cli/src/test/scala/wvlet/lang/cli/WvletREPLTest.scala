/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.cli

import wvlet.lang.cli.terminal.UniTerminal
import wvlet.lang.compiler.WorkEnv
import wvlet.uni.cli.DisplayWidth
import wvlet.uni.cli.InMemoryTerminalIO
import wvlet.uni.cli.TerminalIO
import wvlet.uni.design.Design
import wvlet.uni.test.UniTest

import java.io.File
import java.nio.file.Files

/**
  * Drives the interactive REPL with replayed keystrokes through `TerminalIO.inMemory`
  */
class WvletREPLTest extends UniTest:

  // A work folder with a .wv file keeps the history file under its target folder, not the home
  private def newWorkFolder(): File =
    val dir = Files.createTempDirectory(File("target").toPath, "repl-test").toFile
    Files.writeString(File(dir, "empty.wv").toPath, "-- placeholder\n")
    dir

  private case class ReplRun(io: InMemoryTerminalIO, stdout: String, workFolder: File):
    def screen: String = DisplayWidth.stripAnsi(io.output)

  private def runRepl(inputs: String*): ReplRun = runReplIn(newWorkFolder(), inputs*)

  private def runReplIn(workFolder: File, inputs: String*): ReplRun =
    val io   = TerminalIO.inMemory(inputs.map(_.getBytes("UTF-8")))
    val main = WvletREPLMain(WvletGlobalOption(), WvletREPLOption(workFolder = workFolder.getPath))
    val stdout = captureStdout {
      (main.replDesign(isInteractive = true) + Design.newDesign.bindInstance[TerminalIO](io)).build[
        WvletREPL
      ](_.start())
    }
    ReplRun(io, stdout, workFolder)

  test("run a statement ending with a semicolon") {
    val r = runRepl("select 1 as answer;\r")
    r.stdout shouldContain "answer"
    r.io.isRawMode shouldBe false
  }

  test("continue a query on Enter until a semicolon") {
    val r = runRepl("select 1 as a\r", "select a + 1 as b;\r")
    r.screen shouldContain "│ select a + 1 as b;"
    r.stdout shouldContain "b"
  }

  test("run a statement with Ctrl-J Ctrl-R") {
    val r = runRepl("select 2 as chord\n\u0012")
    r.stdout shouldContain "chord"
  }

  test("exit on Ctrl-C at the prompt") {
    val r = runRepl("\u0003", "select 3 as not_run;\r")
    r.stdout.contains("not_run") shouldBe false
  }

  test("save inputs to the history file") {
    val r       = runRepl("select 4 as saved;\r")
    val history = File(
      WorkEnv(path = r.workFolder.getPath).cacheFolder,
      UniTerminal.HistoryFileName
    )
    history.exists() shouldBe true
    Files.readString(history.toPath) shouldContain "select 4 as saved;"
  }

  test("read many inputs without growing the stack") {
    val r = runRepl(Seq.fill(5000)("\r")*)
    r.io.isRawMode shouldBe false
  }

  test("detect complete inputs") {
    WvletREPL.isInputComplete("") shouldBe true
    WvletREPL.isInputComplete("help") shouldBe true
    WvletREPL.isInputComplete("select 1;") shouldBe true
    // A query can have more operators until `;`
    WvletREPL.isInputComplete("select 1") shouldBe false
    WvletREPL.isInputComplete("model m = {") shouldBe false
    WvletREPL.isInputComplete("select \"\"\"abc") shouldBe false
  }

  test("highlight keywords without changing the text") {
    val input = "from t where x = 'a' -- comment"
    val out   = WvletREPL.highlight(input)
    out shouldNotBe input
    DisplayWidth.stripAnsi(out) shouldBe input
  }

  test("complete local file paths inside quotes") {
    val workFolder = newWorkFolder()
    Files.createDirectory(File(workFolder, "data").toPath)
    Files.writeString(File(workFolder, "data/sales.json").toPath, "[]")
    val completer = LocalFileCompleter(WorkEnv(path = workFolder.getPath))

    val dir = completer.complete("from 'da", 8)
    dir.start shouldBe 6
    dir.candidates.map(c => (c.value, c.complete)) shouldBe Seq(("data/", false))

    val file = completer.complete("from 'data/s", 12)
    // A single file closes the quote
    file.candidates.map(c => (c.value, c.complete)) shouldBe Seq(("data/sales.json'", true))

    // Keep an existing closing quote
    val quoted = completer.complete("from 'data/s'", 12)
    quoted.candidates.map(c => (c.value, c.complete)) shouldBe Seq(("data/sales.json", false))

    // No completion outside quotes
    completer.complete("from da", 7).candidates shouldBe Seq.empty
  }

  test("complete a file path with Tab and close the quote") {
    val workFolder = newWorkFolder()
    Files.writeString(File(workFolder, "sales.json").toPath, "[]")
    // Tab completes the path, then Ctrl-C ends the REPL without running it
    val r = runReplIn(workFolder, "from 'sal\t", "\u0003")
    r.screen shouldContain "from 'sales.json' "
  }

end WvletREPLTest
