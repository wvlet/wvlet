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
package wvlet.lang.compiler.codegen

import wvlet.uni.test.UniTest
import wvlet.lang.api.{StatusCode, WvletLangException}
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}

class ModelExpansionTest extends UniTest:

  private def generateSQL(wv: String): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    GenSQL.generateSQL(unit)(using result.context)

  test("report a user error for a self-recursive model reference") {
    val e = intercept[WvletLangException] {
      generateSQL("""model model_self = {
          |  from model_self
          |}
          |
          |from model_self
          |""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.RECURSIVE_MODEL_REFERENCE
    e.getMessage shouldContain "model_self -> model_self"
  }

  test("report a user error with the cycle path for mutually recursive models") {
    val e = intercept[WvletLangException] {
      generateSQL("""model model_a = {
          |  from model_b
          |}
          |
          |model model_b = {
          |  from model_a
          |}
          |
          |from model_a
          |""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.RECURSIVE_MODEL_REFERENCE
    e.getMessage shouldContain "model_a -> model_b -> model_a"
  }

  test("resolve model arguments passed through nested models") {
    val sql = generateSQL("""model nums = {
        |  from [[1], [2], [3]] as t(x)
        |}
        |
        |model filter_min(min_x: int) = {
        |  from nums
        |  where x >= min_x
        |}
        |
        |model filter_via(bound: int) = {
        |  from filter_min(min_x = bound)
        |}
        |
        |from filter_via(bound = 2)
        |""".stripMargin)
    sql shouldContain "x >= 2"
    sql shouldNotContain "min_x"
    sql shouldNotContain "bound"
  }

  test("shadow an outer val with a model parameter of the same name") {
    val sql = generateSQL("""val bound = 100
        |
        |model people = {
        |  from [[10, 'a'], [25, 'b'], [30, 'c']] as person(age, name)
        |}
        |
        |model adults(bound: int) = {
        |  from people
        |  where age >= bound
        |}
        |
        |from adults(bound = 18)
        |""".stripMargin)
    sql shouldContain "age >= 18"
    sql shouldNotContain "age >= 100"
  }

  test("report a distinct error for a deep but non-recursive model chain") {
    // Build a chain of 101 distinct models: m0 -> m1 -> ... -> m100, exceeding the
    // maxModelExpansionDepth safety net without any cycle
    val depth     = 101
    val modelDefs = (0 until depth)
      .map { i =>
        if i == 0 then
          s"model m0 = { from [[1]] as t(x) }"
        else
          s"model m${i} = { from m${i - 1} }"
      }
      .mkString("\n")
    val e = intercept[WvletLangException] {
      generateSQL(s"""${modelDefs}
          |from m${depth - 1}
          |""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.MODEL_EXPANSION_LIMIT_EXCEEDED
  }

  test("report a user error when a model is called with too many arguments") {
    val e = intercept[WvletLangException] {
      generateSQL("""model filter_min(min_x: int) = {
          |  from [[1], [2], [3]] as t(x)
          |  where x >= min_x
          |}
          |
          |from filter_min(1, 2)
          |""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    e.getMessage shouldContain "takes 1 arguments, but 2 were given"
  }

  test("expand a diamond-shaped model reference without a false cycle error") {
    val sql = generateSQL("""model base = {
        |  from [[1], [2]] as t(x)
        |}
        |
        |model left_ref = {
        |  from base
        |}
        |
        |model right_ref = {
        |  from base
        |}
        |
        |from left_ref
        |join right_ref on left_ref.x = right_ref.x
        |""".stripMargin)
    sql shouldContain "join"
  }

end ModelExpansionTest
