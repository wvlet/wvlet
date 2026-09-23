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
package wvlet.lang.compiler.typer

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.DataType
import wvlet.lang.model.plan.ForLoop
import wvlet.uni.test.UniTest

/**
  * Tests for typing for-loop statements
  */
class ForLoopTypingTest extends UniTest:

  private def compileForLoop(wv: String): ForLoop =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val unit     = CompilationUnit.fromWvletString(wv)
    compiler.compileSingleUnit(unit)
    var loop: Option[ForLoop] = None
    unit
      .resolvedPlan
      .traverse { case f: ForLoop =>
        if loop.isEmpty then
          loop = Some(f)
      }
    loop.getOrElse(fail("No ForLoop found in the resolved plan"))

  test("report an error for a non-array iterable") {
    val e = intercept[WvletLangException] {
      compileForLoop("""for x in 1 {
          |  select x
          |}""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.INVALID_LOOP_ITERABLE
  }

  test("report an error for a string iterable") {
    val e = intercept[WvletLangException] {
      compileForLoop("""for x in 'abc' {
          |  select x
          |}""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.INVALID_LOOP_ITERABLE
  }

  test("type a single-column query iterable as an array of its values") {
    val f = compileForLoop("""for id in (from [[1, 'a']] as t(id, name) select id) {
        |  select id
        |}""".stripMargin)
    f.iterable.dataType shouldBe DataType.ArrayType(DataType.LongType)
  }

  test("type a query iterable with a run-time schema as an array of any") {
    val f = compileForLoop("""for n in (from sql"select 1 as n") {
        |  from [[n]] as t(n)
        |}""".stripMargin)
    f.iterable.dataType shouldBe DataType.ArrayType(DataType.AnyType)
    f.body.forall(_.relationType.isResolved) shouldBe true
  }

  test("type a multi-column query iterable as an array of rows with typed fields") {
    val f = compileForLoop("""for p in (from [[1, 'a']] as t(id, name) select id, name) {
        |  select p.id as id, p.name as name
        |}""".stripMargin)
    f.iterable.dataType match
      case DataType.ArrayType(row: DataType.SchemaType) =>
        row.fields.map(f => f.name.name -> f.dataType) shouldBe
          List("id" -> DataType.LongType, "name" -> DataType.StringType)
      case other =>
        fail(s"Expected an array of rows, but got ${other}")
    f.body.head.relationType.fields.map(_.dataType) shouldBe
      List(DataType.LongType, DataType.StringType)
  }

end ForLoopTypingTest
