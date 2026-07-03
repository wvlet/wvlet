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

import wvlet.uni.test.UniTest
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.model.DataType.*
import wvlet.lang.model.expr.{FunctionArg, LongLiteral}

class BuiltinFunctionsTest extends UniTest:

  private val longArg = FunctionArg(None, LongLiteral(1L, "1", NoSpan), false, Nil, NoSpan)

  test("should type count and window ranking functions as long") {
    BuiltinFunctions.returnTypeOf("count", Nil) shouldBe Some(LongType)
    BuiltinFunctions.returnTypeOf("ROW_NUMBER", Nil) shouldBe Some(LongType)
  }

  test("should type string functions as string") {
    BuiltinFunctions.returnTypeOf("concat", Nil) shouldBe Some(StringType)
    BuiltinFunctions.returnTypeOf("regexp_replace", Nil) shouldBe Some(StringType)
  }

  test("should type predicates as boolean and avg as double") {
    BuiltinFunctions.returnTypeOf("regexp_matches", Nil) shouldBe Some(BooleanType)
    BuiltinFunctions.returnTypeOf("avg", Nil) shouldBe Some(DoubleType)
  }

  test("should derive min/max/coalesce types from the first argument") {
    BuiltinFunctions.returnTypeOf("max", List(longArg)) shouldBe Some(LongType)
    BuiltinFunctions.returnTypeOf("coalesce", Nil) shouldBe None
  }

  test("should return None for unknown functions") {
    BuiltinFunctions.returnTypeOf("mystery_fn", Nil) shouldBe None
  }

end BuiltinFunctionsTest
