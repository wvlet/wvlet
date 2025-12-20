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
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.expr.*
import wvlet.lang.model.expr.UnquotedIdentifier
import wvlet.lang.model.plan.LogicalPlan
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.Scope
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.api.Span
import wvlet.airspec.AirSpec

class TyperTest extends AirSpec:

  private def testContext: Context =
    val global = Context.testGlobalContext(".")
    Context(
      global = global,
      owner = Symbol.NoSymbol,
      scope = Scope.newScope(0),
      compilationUnit = CompilationUnit.empty
    )

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
    given ctx: Context = testContext

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
    given ctx: Context = testContext

    val left = LongLiteral(10, "10", Span.NoSpan)
    left.tpe = LongType
    val right = LongLiteral(20, "20", Span.NoSpan)
    right.tpe = LongType

    val addOp = ArithmeticBinaryExpr(BinaryExprType.Add, left, right, Span.NoSpan)

    val typed = TyperRules.binaryOpRules.apply(addOp)
    typed.tpe shouldBe LongType

  test("TyperRules should type comparison operations"):
    given ctx: Context = testContext

    val left = LongLiteral(10, "10", Span.NoSpan)
    left.tpe = LongType
    val right = LongLiteral(20, "20", Span.NoSpan)
    right.tpe = LongType

    val ltOp = LessThan(left, right, Span.NoSpan)

    val typed = TyperRules.binaryOpRules.apply(ltOp)
    typed.tpe shouldBe BooleanType

  test("TyperRules should type logical operations"):
    given ctx: Context = testContext

    val left = TrueLiteral(Span.NoSpan)
    left.tpe = BooleanType
    val right = FalseLiteral(Span.NoSpan)
    right.tpe = BooleanType

    val andOp = And(left, right, Span.NoSpan)

    val typed = TyperRules.binaryOpRules.apply(andOp)
    typed.tpe shouldBe BooleanType

  test("TyperRules should produce ErrorType for type mismatches"):
    given ctx: Context = testContext

    val left = LongLiteral(10, "10", Span.NoSpan)
    left.tpe = LongType
    val right = TrueLiteral(Span.NoSpan)
    right.tpe = BooleanType

    val andOp = And(left, right, Span.NoSpan)

    val typed = TyperRules.binaryOpRules.apply(andOp)
    typed.tpe.isInstanceOf[ErrorType] shouldBe true

  test("TyperRules should produce ErrorType for comparison type mismatches"):
    given ctx: Context = testContext

    // Long < String should error
    val left1 = LongLiteral(10, "10", Span.NoSpan)
    left1.tpe = LongType
    val right1 = StringLiteral.fromString("hello", Span.NoSpan)
    right1.tpe = StringType

    val ltOp   = LessThan(left1, right1, Span.NoSpan)
    val typed1 = TyperRules.binaryOpRules.apply(ltOp)
    typed1.tpe.isInstanceOf[ErrorType] shouldBe true

    // Boolean < Boolean should error (booleans are not orderable)
    val left2 = TrueLiteral(Span.NoSpan)
    left2.tpe = BooleanType
    val right2 = FalseLiteral(Span.NoSpan)
    right2.tpe = BooleanType

    val ltOp2  = LessThan(left2, right2, Span.NoSpan)
    val typed2 = TyperRules.binaryOpRules.apply(ltOp2)
    typed2.tpe.isInstanceOf[ErrorType] shouldBe true

  test("TyperRules should allow valid comparisons"):
    given ctx: Context = testContext

    // Long < Long is valid
    val left1 = LongLiteral(10, "10", Span.NoSpan)
    left1.tpe = LongType
    val right1 = LongLiteral(20, "20", Span.NoSpan)
    right1.tpe = LongType

    val ltOp1  = LessThan(left1, right1, Span.NoSpan)
    val typed1 = TyperRules.binaryOpRules.apply(ltOp1)
    typed1.tpe shouldBe BooleanType

    // String < String is valid
    val left2 = StringLiteral.fromString("abc", Span.NoSpan)
    left2.tpe = StringType
    val right2 = StringLiteral.fromString("def", Span.NoSpan)
    right2.tpe = StringType

    val ltOp2  = LessThan(left2, right2, Span.NoSpan)
    val typed2 = TyperRules.binaryOpRules.apply(ltOp2)
    typed2.tpe shouldBe BooleanType

  test("TyperRules should type Cast expressions"):
    given ctx: Context = testContext

    val expr = LongLiteral(42, "42", Span.NoSpan)
    expr.tpe = LongType

    val castExpr = Cast(expr, DoubleType, tryCast = false, Span.NoSpan)
    val typed    = TyperRules.castRules.apply(castExpr)

    typed.tpe shouldBe DoubleType

  test("TyperRules should type Case expressions with same types"):
    given ctx: Context = testContext

    val cond1 = TrueLiteral(Span.NoSpan)
    cond1.tpe = BooleanType
    val result1 = LongLiteral(1, "1", Span.NoSpan)
    result1.tpe = LongType

    val cond2 = FalseLiteral(Span.NoSpan)
    cond2.tpe = BooleanType
    val result2 = LongLiteral(2, "2", Span.NoSpan)
    result2.tpe = LongType

    val whenClause1 = WhenClause(cond1, result1, Span.NoSpan)
    val whenClause2 = WhenClause(cond2, result2, Span.NoSpan)

    val caseExpr = CaseExpr(None, List(whenClause1, whenClause2), None, Span.NoSpan)
    val typed    = TyperRules.caseExprRules.apply(caseExpr)

    typed.tpe shouldBe LongType

  test("TyperRules should type Case expressions with type promotion"):
    given ctx: Context = testContext

    val cond1 = TrueLiteral(Span.NoSpan)
    cond1.tpe = BooleanType
    val result1 = LongLiteral(1, "1", Span.NoSpan)
    result1.tpe = LongType

    val cond2 = FalseLiteral(Span.NoSpan)
    cond2.tpe = BooleanType
    val result2 = DoubleLiteral(2.5, "2.5", Span.NoSpan)
    result2.tpe = DoubleType

    val whenClause1 = WhenClause(cond1, result1, Span.NoSpan)
    val whenClause2 = WhenClause(cond2, result2, Span.NoSpan)

    val caseExpr = CaseExpr(None, List(whenClause1, whenClause2), None, Span.NoSpan)
    val typed    = TyperRules.caseExprRules.apply(caseExpr)

    // Should promote to DoubleType (Long -> Double)
    typed.tpe shouldBe DoubleType

  test("TyperRules should type Case expressions with ELSE clause"):
    given ctx: Context = testContext

    val cond1 = TrueLiteral(Span.NoSpan)
    cond1.tpe = BooleanType
    val result1 = LongLiteral(1, "1", Span.NoSpan)
    result1.tpe = LongType

    val elseResult = DoubleLiteral(3.14, "3.14", Span.NoSpan)
    elseResult.tpe = DoubleType

    val whenClause1 = WhenClause(cond1, result1, Span.NoSpan)
    val caseExpr    = CaseExpr(None, List(whenClause1), Some(elseResult), Span.NoSpan)
    val typed       = TyperRules.caseExprRules.apply(caseExpr)

    // Should promote to DoubleType
    typed.tpe shouldBe DoubleType

  test("TyperRules should type FunctionApply expressions"):
    given ctx: Context = testContext

    // Create a function type with StringType as return type
    val funcType = Type.FunctionType(
      name = Name.termName("upper"),
      args = List(NamedType(Name.termName("input"), StringType)),
      returnType = StringType,
      contextNames = Nil
    )

    // Function base is an identifier with a FunctionType
    val funcId = UnquotedIdentifier("upper", Span.NoSpan)
    funcId.tpe = funcType

    val arg = StringLiteral.fromString("hello", Span.NoSpan)
    arg.tpe = StringType
    val funcArg  = FunctionArg(None, arg, false, Nil, Span.NoSpan)
    val funcCall = FunctionApply(funcId, List(funcArg), None, None, None, Span.NoSpan)

    val typed = TyperRules.functionApplyRules.apply(funcCall)

    // Should get return type from the function type
    typed.tpe shouldBe StringType

  test("TyperRules should type DotRef with SchemaType"):
    given ctx: Context = testContext

    val schema = SchemaType(
      parent = None,
      typeName = Name.typeName("users"),
      columnTypes = List(
        NamedType(Name.termName("id"), LongType),
        NamedType(Name.termName("name"), StringType)
      )
    )

    val qualifier = UnquotedIdentifier("users", Span.NoSpan)
    qualifier.tpe = schema

    val nameExpr = UnquotedIdentifier("name", Span.NoSpan)
    val dotRef   = DotRef(qualifier, nameExpr, DataType.UnknownType, Span.NoSpan)

    val typed = TyperRules.dotRefRules.apply(dotRef)

    typed.tpe shouldBe StringType

  test("TyperRules should produce ErrorType for DotRef with non-existent field"):
    given ctx: Context = testContext

    val schema = SchemaType(
      parent = None,
      typeName = Name.typeName("users"),
      columnTypes = List(
        NamedType(Name.termName("id"), LongType),
        NamedType(Name.termName("name"), StringType)
      )
    )

    val qualifier = UnquotedIdentifier("users", Span.NoSpan)
    qualifier.tpe = schema

    val nameExpr = UnquotedIdentifier("nonexistent", Span.NoSpan)
    val dotRef   = DotRef(qualifier, nameExpr, DataType.UnknownType, Span.NoSpan)

    val typed = TyperRules.dotRefRules.apply(dotRef)

    typed.tpe.isInstanceOf[ErrorType] shouldBe true
    typed.tpe.asInstanceOf[ErrorType].msg.shouldContain("nonexistent")

  test("TyperRules should return NoType for DotRef when qualifier has NoType"):
    given ctx: Context = testContext

    val qualifier = UnquotedIdentifier("unknown", Span.NoSpan)
    // qualifier.tpe is NoType by default

    val nameExpr = UnquotedIdentifier("field", Span.NoSpan)
    val dotRef   = DotRef(qualifier, nameExpr, DataType.UnknownType, Span.NoSpan)

    val typed = TyperRules.dotRefRules.apply(dotRef)

    typed.tpe shouldBe NoType

  test("TyperRules should propagate ErrorType from DotRef qualifier"):
    given ctx: Context = testContext

    val qualifier = UnquotedIdentifier("bad", Span.NoSpan)
    qualifier.tpe = ErrorType("Unresolved identifier: bad")

    val nameExpr = UnquotedIdentifier("field", Span.NoSpan)
    val dotRef   = DotRef(qualifier, nameExpr, DataType.UnknownType, Span.NoSpan)

    val typed = TyperRules.dotRefRules.apply(dotRef)

    typed.tpe.isInstanceOf[ErrorType] shouldBe true
    typed.tpe.asInstanceOf[ErrorType].msg.shouldContain("Unresolved identifier: bad")

  test("TyperRules should produce ErrorType for DotRef on primitive type"):
    given ctx: Context = testContext

    val qualifier = LongLiteral(42, "42", Span.NoSpan)
    qualifier.tpe = LongType

    val nameExpr = UnquotedIdentifier("field", Span.NoSpan)
    val dotRef   = DotRef(qualifier, nameExpr, DataType.UnknownType, Span.NoSpan)

    val typed = TyperRules.dotRefRules.apply(dotRef)

    typed.tpe.isInstanceOf[ErrorType] shouldBe true
    typed.tpe.asInstanceOf[ErrorType].msg.shouldContain("long")
    typed.tpe.asInstanceOf[ErrorType].msg.shouldContain("does not support field access")

  test("TyperRules should produce ErrorType for Case expressions with incompatible types"):
    given ctx: Context = testContext

    val cond1 = TrueLiteral(Span.NoSpan)
    cond1.tpe = BooleanType
    val result1 = StringLiteral.fromString("hello", Span.NoSpan)
    result1.tpe = StringType

    val cond2 = FalseLiteral(Span.NoSpan)
    cond2.tpe = BooleanType
    val result2 = LongLiteral(42, "42", Span.NoSpan)
    result2.tpe = LongType

    val whenClause1 = WhenClause(cond1, result1, Span.NoSpan)
    val whenClause2 = WhenClause(cond2, result2, Span.NoSpan)

    val caseExpr = CaseExpr(None, List(whenClause1, whenClause2), None, Span.NoSpan)
    val typed    = TyperRules.caseExprRules.apply(caseExpr)

    // Should produce ErrorType for incompatible types (String vs Long)
    typed.tpe.isInstanceOf[ErrorType] shouldBe true

end TyperTest
