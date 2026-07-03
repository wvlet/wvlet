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
package wvlet.lang.compiler.analyzer

import wvlet.uni.test.UniTest
import wvlet.lang.api.{StatusCode, WvletLangException}
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}
import wvlet.lang.compiler.codegen.GenSQL

/**
  * Tests for the cycle/depth guarded inlining of function bodies and partial queries in
  * TypeResolver
  */
class FunctionInlineTest extends UniTest:

  private def generateSQL(wv: String): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    GenSQL.generateSQL(unit)(using result.context)

  test("inline function calls nested in a function body") {
    val sql = generateSQL("""def fn_double(x: int): int = x + x
        |def fn_quad(x: int): int = fn_double(x) + fn_double(x)
        |def fn_oct(x: int): int = fn_quad(x) + fn_quad(x)
        |
        |select fn_oct(2) as v
        |""".stripMargin)
    sql shouldNotContain "fn_oct"
    sql shouldNotContain "fn_quad"
    sql shouldNotContain "fn_double"
    sql shouldContain "2 + 2"
  }

  test("bind arguments of a member function call nested in a function body") {
    // obj.method(args) inside a body: the enclosing FunctionApply must be inlined before its
    // base DotRef, or the arguments would be dropped
    val sql = generateSQL("""def fn_safe(x: int): int = x.or_else(0) + 1
        |
        |select fn_safe(10) as v
        |""".stripMargin)
    sql shouldContain "coalesce(10,0)"
    sql shouldNotContain "other"
  }

  test("report a user error for a self-recursive function reference") {
    val e = intercept[WvletLangException] {
      generateSQL("""def rec_f(x: int): int = rec_f(x)
          |
          |select rec_f(1)
          |""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.RECURSIVE_FUNCTION_REFERENCE
    e.getMessage shouldContain "rec_f -> rec_f"
  }

  test("report a user error with the cycle path for mutually recursive functions") {
    val e = intercept[WvletLangException] {
      generateSQL("""def fun_a(x: int): int = fun_b(x)
          |def fun_b(x: int): int = fun_a(x)
          |
          |select fun_a(1)
          |""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.RECURSIVE_FUNCTION_REFERENCE
    e.getMessage shouldContain "fun_a -> fun_b -> fun_a"
  }

  test("pass parameters through nested partial queries") {
    val sql = generateSQL("""def pq_older_than(min_age: int) = where age > min_age
        |def pq_names(bound: int) = where name is not null | pq_older_than(bound) | select name
        |
        |from [[10, 'a'], [25, 'b'], [30, 'c']] as person(age, name) | pq_names(18)
        |""".stripMargin)
    sql shouldContain "age > 18"
    sql shouldNotContain "min_age"
    sql shouldNotContain "bound"
  }

  test("report a user error for a self-recursive partial query reference") {
    val e = intercept[WvletLangException] {
      generateSQL("""def rec_pq = where age >= 18 | rec_pq
          |
          |from [[10], [25]] as person(age) | rec_pq
          |""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.RECURSIVE_PARTIAL_QUERY_REFERENCE
    e.getMessage shouldContain "rec_pq -> rec_pq"
  }

  test("report a user error with the cycle path for mutually recursive partial queries") {
    val e = intercept[WvletLangException] {
      generateSQL("""def pq_a = where age >= 18 | pq_b
          |def pq_b = where age <= 60 | pq_a
          |
          |from [[10], [25]] as person(age) | pq_a
          |""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.RECURSIVE_PARTIAL_QUERY_REFERENCE
    e.getMessage shouldContain "-> pq_a"
  }

end FunctionInlineTest
