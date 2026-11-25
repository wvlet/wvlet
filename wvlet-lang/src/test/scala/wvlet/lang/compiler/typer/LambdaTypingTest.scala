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

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.DataType
import wvlet.lang.model.Type.FunctionType
import wvlet.lang.model.expr.LambdaExpr
import wvlet.uni.test.UniTest

import scala.collection.mutable.ListBuffer

/**
  * Tests for typing lambda parameters from the element type of the list they are applied to
  */
class LambdaTypingTest extends UniTest:

  private def compileLambdas(wv: String): List[LambdaExpr] =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val unit     = CompilationUnit.fromWvletString(wv)
    compiler.compileSingleUnit(unit)
    val lambdas = ListBuffer.empty[LambdaExpr]
    unit
      .resolvedPlan
      .traverseExpressions { case l: LambdaExpr =>
        lambdas += l
      }
    lambdas.toList

  test("type the lambda parameter of a method call with the receiver's element type") {
    val List(l) = compileLambdas("select [4, 5, 6].transform(x -> x * 2) as a"): @unchecked
    val x       = l.args.head
    x.dataType.isNumeric shouldBe true
    l.body.dataType shouldBe x.dataType
    l.tpe match
      case ft: FunctionType =>
        ft.returnType shouldBe l.body.dataType
      case other =>
        fail(s"Expected a function type, but got ${other}")
  }

  test("type both parameters of reduce with the element type") {
    val List(l) = compileLambdas("select ['a', 'b'].reduce((acc, s) -> acc + s) as a"): @unchecked
    l.args.map(_.dataType) shouldBe List(DataType.StringType, DataType.StringType)
  }

  test("type the lambda parameter of a function-call form from its list argument") {
    val List(l) = compileLambdas("select list_filter(['a', 'bb'], w -> w = 'a') as a"): @unchecked
    l.args.head.dataType shouldBe DataType.StringType
    l.body.dataType shouldBe DataType.BooleanType
  }

  test("let a lambda parameter shadow a same-named input column") {
    val List(l) =
      compileLambdas("""from [[1, ['a', 'bb']]] as words_table(w, words)
        |select words.filter(w -> w.length > 1) as long_words""".stripMargin): @unchecked
    // The parameter is a string element, not the integer column w
    l.args.head.dataType shouldBe DataType.StringType
    l.body.dataType shouldBe DataType.BooleanType
  }

  test("type the accumulator of reduce with an initial value from that value") {
    val List(l) = compileLambdas("select [1, 2].reduce('', (acc, x) -> acc + 'a') as a"): @unchecked
    l.args.map(_.dataType) shouldBe List(DataType.StringType, DataType.LongType)
  }

  test("type the accumulator of DuckDB's list_reduce with an initial value") {
    val List(l) =
      compileLambdas("select list_reduce([1, 2], (acc, x) -> acc, 'a') as a"): @unchecked
    l.args.map(_.dataType) shouldBe List(DataType.StringType, DataType.LongType)
  }

  test("type the lambda parameter of exists and forall") {
    val lambdas = compileLambdas(
      "select ['a', 'bb'].exists(s -> s.length > 1) as e, [1].forall(x -> x > 0) as f"
    )
    lambdas.map(_.args.head.dataType) shouldBe List(DataType.StringType, DataType.LongType)
    lambdas.map(_.body.dataType) shouldBe List(DataType.BooleanType, DataType.BooleanType)
  }

  test("type nested lambdas with their own lists") {
    val lambdas = compileLambdas(
      "select [1, 2].transform(x -> ['a'].transform(s -> s + 'b').length + x) as a"
    )
    lambdas.size shouldBe 2
    lambdas.flatMap(_.args).forall(_.dataType.isResolved) shouldBe true
    lambdas.flatMap(_.args.map(_.leafName)).toSet shouldBe Set("x", "s")
  }

end LambdaTypingTest
