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

import wvlet.lang.model.Type
import wvlet.lang.model.Type.NoType
import wvlet.lang.model.Type.ErrorType
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.BooleanType
import wvlet.lang.model.DataType.LongType
import wvlet.lang.model.DataType.DoubleType
import wvlet.lang.model.DataType.StringType
import wvlet.lang.model.DataType.NullType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.LogicalPlan
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Scope
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.api.Span
import wvlet.airspec.AirSpec

class TyperTest extends AirSpec:

  test("tpe field should be accessible on all SyntaxTreeNode instances"):
    val lit = LongLiteral(42, "42", Span.NoSpan)

    // Should initially have NoType
    lit.tpe.shouldBe(NoType)

    // Should be settable
    lit.tpe = LongType
    lit.tpe.shouldBe(LongType)

    // isTyped should work
    lit.isTyped.shouldBe(true)

  test("TyperRules should type literals correctly"):
    given ctx: TyperContext = TyperContext(
      owner = Symbol.NoSymbol,
      scope = Scope.newScope(0),
      compilationUnit = CompilationUnit.empty,
      context = null // We don't need a full context for this test
    )

    val longLit   = LongLiteral(42, "42", Span.NoSpan)
    val doubleLit = DoubleLiteral(3.14, "3.14", Span.NoSpan)
    val stringLit = StringLiteral.fromString("hello", Span.NoSpan)
    val boolLit   = TrueLiteral(Span.NoSpan)
    val nullLit   = NullLiteral(Span.NoSpan)

    // Apply typing rules
    val typedLong   = TyperRules.literalRules.apply(longLit)
    val typedDouble = TyperRules.literalRules.apply(doubleLit)
    val typedString = TyperRules.literalRules.apply(stringLit)
    val typedBool   = TyperRules.literalRules.apply(boolLit)
    val typedNull   = TyperRules.literalRules.apply(nullLit)

    typedLong.tpe shouldBe LongType
    typedDouble.tpe shouldBe DoubleType
    typedString.tpe shouldBe StringType
    typedBool.tpe shouldBe BooleanType
    typedNull.tpe shouldBe NullType

  test("TyperRules should type arithmetic operations"):
    given ctx: TyperContext = TyperContext(
      owner = Symbol.NoSymbol,
      scope = Scope.newScope(0),
      compilationUnit = CompilationUnit.empty,
      context = null
    )

    val left = LongLiteral(10, "10", Span.NoSpan)
    left.tpe = LongType
    val right = LongLiteral(20, "20", Span.NoSpan)
    right.tpe = LongType

    val addOp = ArithmeticBinaryExpr(BinaryExprType.Add, left, right, Span.NoSpan)

    val typed = TyperRules.binaryOpRules.apply(addOp)
    typed.tpe shouldBe LongType

  test("TyperRules should type comparison operations"):
    given ctx: TyperContext = TyperContext(
      owner = Symbol.NoSymbol,
      scope = Scope.newScope(0),
      compilationUnit = CompilationUnit.empty,
      context = null
    )

    val left = LongLiteral(10, "10", Span.NoSpan)
    left.tpe = LongType
    val right = LongLiteral(20, "20", Span.NoSpan)
    right.tpe = LongType

    val ltOp = LessThan(left, right, Span.NoSpan)

    val typed = TyperRules.binaryOpRules.apply(ltOp)
    typed.tpe shouldBe BooleanType

  test("TyperRules should type logical operations"):
    given ctx: TyperContext = TyperContext(
      owner = Symbol.NoSymbol,
      scope = Scope.newScope(0),
      compilationUnit = CompilationUnit.empty,
      context = null
    )

    val left = TrueLiteral(Span.NoSpan)
    left.tpe = BooleanType
    val right = FalseLiteral(Span.NoSpan)
    right.tpe = BooleanType

    val andOp = And(left, right, Span.NoSpan)

    val typed = TyperRules.binaryOpRules.apply(andOp)
    typed.tpe shouldBe BooleanType

  test("TyperRules should produce ErrorType for type mismatches"):
    given ctx: TyperContext = TyperContext(
      owner = Symbol.NoSymbol,
      scope = Scope.newScope(0),
      compilationUnit = CompilationUnit.empty,
      context = null
    )

    val left = LongLiteral(10, "10", Span.NoSpan)
    left.tpe = LongType
    val right = TrueLiteral(Span.NoSpan)
    right.tpe = BooleanType

    val andOp = And(left, right, Span.NoSpan)

    val typed = TyperRules.binaryOpRules.apply(andOp)
    typed.tpe.isInstanceOf[ErrorType] shouldBe true

end TyperTest
