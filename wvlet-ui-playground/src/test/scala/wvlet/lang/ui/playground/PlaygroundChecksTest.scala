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
package wvlet.lang.ui.playground

import wvlet.uni.test.UniTest

/** `test` and `debug` statements run in the playground and report their source lines */
class PlaygroundChecksTest extends UniTest:
  private val engine = PlaygroundEngine(0.001)

  test("should report passing and failing tests with their lines") {
    val out = engine.run("""from nation
        |test _.size should be 25
        |test _.columns should contain 'n_name'
        |test _.size should be 10
        |""".stripMargin)
    out.error shouldBe None
    out.result.get.totalRows shouldBe 25
    val tests = out.tests
    tests.map(_.status) shouldBe Seq(TestStatus.Passed, TestStatus.Passed, TestStatus.Failed)
    tests.map(_.line) shouldBe Seq(2, 3, 4)
    tests(2).source shouldBe "test _.size should be 10"
    tests(2).message shouldContain "was not equal to"
    out.failedTests shouldBe 1
  }

  test("should check tests of every statement in the file, not only the selected one") {
    val out = engine.run("""from region
        |test _.size should be 5
        |;
        |from nation
        |limit 3
        |""".stripMargin)
    out.result.get.totalRows shouldBe 3
    out.tests.map(t => (t.line, t.status)) shouldBe Seq((2, TestStatus.Passed))
  }

  test("should show the rows of a debug block and check the tests inside it") {
    val out = engine.run("""from nation
        |debug {
        |  where n_regionkey = 0
        |  test _.size should be 5
        |}
        |where n_regionkey = 1
        |test _.size should be 5
        |""".stripMargin)
    out.error shouldBe None
    out.checks.size shouldBe 3
    out.checks.head match
      case d: Check.Debug =>
        d.line shouldBe 2
        d.source shouldBe "debug {\n  where n_regionkey = 0\n  test _.size should be 5\n}"
        d.result.map(_.totalRows) shouldBe Right(5)
      case other =>
        fail(s"Expected a debug result: ${other}")
    out.tests.map(t => (t.line, t.status)) shouldBe
      Seq((4, TestStatus.Passed), (7, TestStatus.Passed))
  }

  test("should list nested debug blocks in source order") {
    val out = engine.run("""from nation
        |debug {
        |  agg _.count as all_nations
        |}
        |where n_regionkey = 0
        |debug {
        |  agg _.count as region_nations
        |}
        |""".stripMargin)
    out.debugBlocks.map(_.line) shouldBe Seq(2, 6)
    out.debugBlocks.map(_.result.map(_.rows.head.head)) shouldBe
      Seq(Right(Some("25")), Right(Some("5")))
  }

  test("should report a debug block whose query fails without failing the run") {
    val out = engine.run("""from nation
        |debug {
        |  where no_such_column = 1
        |}
        |limit 2
        |""".stripMargin)
    out.error shouldBe None
    out.result.get.totalRows shouldBe 2
    out.failedTests shouldBe 0
    out.failedChecks shouldBe 1
    out.debugBlocks.head.result.isLeft shouldBe true
  }

  test("should run a plain query without checks") {
    val out = engine.run("from region")
    out.checks shouldBe Nil
  }

  test("should pass the tests of every bundled example except the one that fails on purpose") {
    SampleQuery
      .allFiles
      .filter((path, _) => path.endsWith(".wv"))
      .foreach { (path, text) =>
        val failed = engine.run(text).checks.filter(_.isFailure)
        if path.endsWith("06_failing_test.wv") then
          failed.size shouldBe 1
        else if failed.nonEmpty then
          fail(s"${path}: ${failed.mkString("\n")}")
      }
  }

end PlaygroundChecksTest
