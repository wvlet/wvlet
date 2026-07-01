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
    sql shouldNotContain "100"
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
