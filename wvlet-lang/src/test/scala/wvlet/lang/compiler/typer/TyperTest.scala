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
import wvlet.lang.model.DataType.IntType
import wvlet.lang.model.DataType.LongType
import wvlet.lang.model.DataType.FloatType
import wvlet.lang.model.DataType.DoubleType
import wvlet.lang.model.DataType.StringType
import wvlet.lang.model.DataType.NullType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.expr.*
import wvlet.lang.model.expr.UnquotedIdentifier
import wvlet.lang.model.plan.*
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.Scope
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.SymbolInfo
import wvlet.lang.compiler.SymbolType
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

  // ============================================
  // Context and TyperState tests
  // ============================================

  test("TyperState should track inputType"):
    val state = TyperState.empty
    state.inputType shouldBe DataType.EmptyRelationType

    val schema = SchemaType(
      parent = None,
      typeName = Name.typeName("test"),
      columnTypes = List(NamedType(Name.termName("id"), LongType))
    )
    val updated = state.withInputType(schema)
    updated.inputType shouldBe schema

  test("Context.withInputType should propagate to typerState"):
    val ctx = testContext
    ctx.inputType shouldBe DataType.EmptyRelationType

    val schema = SchemaType(
      parent = None,
      typeName = Name.typeName("test"),
      columnTypes = List(NamedType(Name.termName("name"), StringType))
    )
    val updated = ctx.withInputType(schema)
    updated.inputType shouldBe schema

  test("Context.newContext should create child context with inherited typerState"):
    val ctx    = testContext
    val schema = SchemaType(
      parent = None,
      typeName = Name.typeName("test"),
      columnTypes = List(NamedType(Name.termName("x"), LongType))
    )
    val ctxWithInput = ctx.withInputType(schema)

    val childCtx = ctxWithInput.newContext(Symbol.NoSymbol)
    // Child context should inherit typerState
    childCtx.inputType shouldBe schema

  test("TyperRules.identifierRules should resolve from inputType"):
    val schema = SchemaType(
      parent = None,
      typeName = Name.typeName("users"),
      columnTypes = List(
        NamedType(Name.termName("id"), LongType),
        NamedType(Name.termName("name"), StringType)
      )
    )
    given ctx: Context = testContext.withInputType(schema)

    val idExpr = UnquotedIdentifier("name", Span.NoSpan)
    val typed  = TyperRules.identifierRules.apply(idExpr)

    typed.tpe shouldBe StringType

  test("TyperRules.identifierRules should produce ErrorType for unknown column"):
    val schema = SchemaType(
      parent = None,
      typeName = Name.typeName("users"),
      columnTypes = List(NamedType(Name.termName("id"), LongType))
    )
    given ctx: Context = testContext.withInputType(schema)

    val idExpr = UnquotedIdentifier("unknown", Span.NoSpan)
    val typed  = TyperRules.identifierRules.apply(idExpr)

    typed.tpe.isInstanceOf[ErrorType] shouldBe true

  // ============================================
  // Relation typing tests
  // ============================================

  test("TyperRules.relationRules should set tpe from relationType"):
    given ctx: Context = testContext

    // Create a simple Values relation with a schema
    val schema = SchemaType(
      parent = None,
      typeName = Name.typeName("test"),
      columnTypes = List(
        NamedType(Name.termName("id"), LongType),
        NamedType(Name.termName("name"), StringType)
      )
    )
    val values = Values(Nil, schema, Span.NoSpan)

    // Before typing, tpe should be NoType
    values.tpe shouldBe NoType

    // Apply relation rules
    val typed = TyperRules.relationRules.apply(values)

    // After typing, tpe should be set to relationType
    typed.tpe shouldBe values.relationType
    typed.tpe shouldBe schema

  // ============================================
  // Statement typing tests
  // ============================================

  test("TyperRules.typeStatement should type PackageDef with PackageType"):
    given ctx: Context = testContext

    val packageDef = PackageDef(
      name = wvlet
        .lang
        .model
        .expr
        .DotRef(
          wvlet.lang.model.expr.UnquotedIdentifier("test", Span.NoSpan),
          wvlet.lang.model.expr.UnquotedIdentifier("pkg", Span.NoSpan),
          DataType.UnknownType,
          Span.NoSpan
        ),
      statements = Nil,
      span = Span.NoSpan
    )

    // Before typing, tpe should be NoType
    packageDef.tpe shouldBe NoType

    // Apply statement typing
    TyperRules.typeStatement(packageDef)

    // After typing, tpe should be PackageType
    packageDef.tpe.isInstanceOf[Type.PackageType] shouldBe true

  test("TyperRules.typeStatement should type Import with ImportType"):
    given ctx: Context = testContext

    val importDef = Import(
      importRef = wvlet.lang.model.expr.UnquotedIdentifier("some_module", Span.NoSpan),
      alias = None,
      fromSource = None,
      span = Span.NoSpan
    )

    // Before typing, tpe should be NoType
    importDef.tpe shouldBe NoType

    // Apply statement typing
    TyperRules.typeStatement(importDef)

    // After typing, tpe should be ImportType
    importDef.tpe.isInstanceOf[Type.ImportType] shouldBe true

  // ============================================
  // TypeInference tests
  // ============================================

  test("TypeInference.findCommonType should return same type when all types match"):
    TypeInference.findCommonType(Seq(IntType, IntType, IntType)) shouldBe IntType
    TypeInference.findCommonType(Seq(StringType, StringType)) shouldBe StringType

  test("TypeInference.findCommonType should promote numeric types"):
    TypeInference.findCommonType(Seq(IntType, LongType)) shouldBe LongType
    TypeInference.findCommonType(Seq(IntType, DoubleType)) shouldBe DoubleType
    TypeInference.findCommonType(Seq(FloatType, DoubleType)) shouldBe DoubleType
    TypeInference.findCommonType(Seq(IntType, LongType, FloatType, DoubleType)) shouldBe DoubleType

  test("TypeInference.findCommonType should handle NullType"):
    TypeInference.findCommonType(Seq(NullType, IntType)) shouldBe IntType
    TypeInference.findCommonType(Seq(StringType, NullType)) shouldBe StringType
    TypeInference.findCommonType(Seq(NullType, NullType)) shouldBe NullType

  test("TypeInference.findCommonType should return NoType for empty list"):
    TypeInference.findCommonType(Seq.empty) shouldBe NoType

  test("TypeInference.findCommonType should return ErrorType for incompatible types"):
    val result = TypeInference.findCommonType(Seq(IntType, BooleanType))
    result.isInstanceOf[ErrorType] shouldBe true

  test("TypeInference.canCoerce should allow NULL to any type"):
    TypeInference.canCoerce(NullType, IntType) shouldBe true
    TypeInference.canCoerce(NullType, StringType) shouldBe true

  test("TypeInference.canCoerce should allow numeric widening"):
    TypeInference.canCoerce(IntType, LongType) shouldBe true
    TypeInference.canCoerce(IntType, DoubleType) shouldBe true
    TypeInference.canCoerce(FloatType, DoubleType) shouldBe true
    // But not narrowing
    TypeInference.canCoerce(LongType, IntType) shouldBe false
    TypeInference.canCoerce(DoubleType, IntType) shouldBe false

  test("TypeInference.unify should find common type"):
    TypeInference.unify(IntType, IntType) shouldBe IntType
    TypeInference.unify(IntType, LongType) shouldBe LongType
    TypeInference.unify(NullType, StringType) shouldBe StringType

  test("TypeInference.unify should return ErrorType for incompatible types"):
    val result = TypeInference.unify(IntType, BooleanType)
    result.isInstanceOf[ErrorType] shouldBe true

  // ============================================
  // Typer integration tests
  // ============================================

  test("Typer.run should store typed plan in CompilationUnit.resolvedPlan"):
    val wvletSource = "from values(1, 2, 3) as t(x)"

    // Compile with new Typer
    val options  = CompilerOptions(workEnv = WorkEnv("."))
    val compiler = Compiler.withNewTyper(options)
    val unit     = CompilationUnit.fromWvletString(wvletSource)

    // Before compilation, resolvedPlan should be empty
    unit.resolvedPlan shouldBe LogicalPlan.empty

    // Compile
    compiler.compileSingleUnit(unit)

    // After compilation, resolvedPlan should be set (not empty)
    unit.resolvedPlan shouldNotBe LogicalPlan.empty
    unit.resolvedPlan.isInstanceOf[PackageDef] shouldBe true

  // ============================================
  // Table type resolution tests
  // ============================================

  /**
    * Helper to create a symbol with type information registered in the context.
    */
  private def registerTypeSymbol(typeName: String, schema: SchemaType)(using ctx: Context): Symbol =
    val name    = Name.typeName(typeName)
    val typeSym = Symbol(ctx.global.newSymbolId, Span.NoSpan)
    val symInfo = SymbolInfo(SymbolType.TypeDef, ctx.owner, typeSym, name, schema)
    typeSym.symbolInfo = symInfo
    ctx.enter(typeSym)
    typeSym

  test("TyperRules.tableRefRules should resolve TableRef via type definition"):
    given ctx: Context = testContext

    // Create a schema type definition for 'users'
    val usersSchema = SchemaType(
      parent = None,
      typeName = Name.typeName("users"),
      columnTypes = List(
        NamedType(Name.termName("id"), LongType),
        NamedType(Name.termName("name"), StringType),
        NamedType(Name.termName("email"), StringType)
      )
    )

    // Register the type in the context's scope
    registerTypeSymbol("users", usersSchema)

    // Create a TableRef for 'users'
    val tableRef = TableRef(name = UnquotedIdentifier("users", Span.NoSpan), span = Span.NoSpan)

    // Initially should have UnresolvedRelationType
    tableRef.relationType.isResolved shouldBe false

    // Apply tableRefRules
    val resolved = TyperRules.tableRefRules.applyOrElse(tableRef, identity[Relation])

    // Should resolve to TableScan with the schema from type definition
    resolved.isInstanceOf[TableScan] shouldBe true
    val tableScan = resolved.asInstanceOf[TableScan]
    tableScan.schema shouldBe usersSchema
    tableScan.columns.size shouldBe 3
    tableScan.columns.map(_.name.name) shouldBe List("id", "name", "email")

  test("TyperRules.tableRefRules should leave TableRef unresolved if type not found"):
    given ctx: Context = testContext

    // Create a TableRef for 'unknown_table' (no type definition)
    val tableRef = TableRef(
      name = UnquotedIdentifier("unknown_table", Span.NoSpan),
      span = Span.NoSpan
    )

    // Apply tableRefRules
    val resolved = TyperRules.tableRefRules.applyOrElse(tableRef, identity[Relation])

    // Should remain as TableRef (unresolved)
    resolved.isInstanceOf[TableRef] shouldBe true
    resolved.relationType.isResolved shouldBe false

  test("TyperRules.relationRules should include tableRefRules"):
    given ctx: Context = testContext

    // Create a schema type definition
    val ordersSchema = SchemaType(
      parent = None,
      typeName = Name.typeName("orders"),
      columnTypes = List(
        NamedType(Name.termName("order_id"), LongType),
        NamedType(Name.termName("amount"), DoubleType)
      )
    )

    // Register the type
    registerTypeSymbol("orders", ordersSchema)

    // Create a TableRef
    val tableRef = TableRef(name = UnquotedIdentifier("orders", Span.NoSpan), span = Span.NoSpan)

    // Apply relationRules (which includes tableRefRules)
    val resolved = TyperRules.relationRules.applyOrElse(tableRef, identity[Relation])

    // Should resolve to TableScan
    resolved.isInstanceOf[TableScan] shouldBe true
    val tableScan = resolved.asInstanceOf[TableScan]
    tableScan.columns.size shouldBe 2

  test("TyperRules.tableRefRules should handle qualified table names"):
    given ctx: Context = testContext

    // Create a schema type definition for 'products'
    val productsSchema = SchemaType(
      parent = None,
      typeName = Name.typeName("products"),
      columnTypes = List(
        NamedType(Name.termName("product_id"), LongType),
        NamedType(Name.termName("name"), StringType),
        NamedType(Name.termName("price"), DoubleType)
      )
    )

    // Register the type
    registerTypeSymbol("products", productsSchema)

    // Create a TableRef with qualified name (schema.products)
    val tableRef = TableRef(
      name = DotRef(
        UnquotedIdentifier("myschema", Span.NoSpan),
        UnquotedIdentifier("products", Span.NoSpan),
        DataType.UnknownType,
        Span.NoSpan
      ),
      span = Span.NoSpan
    )

    // Apply tableRefRules
    val resolved = TyperRules.tableRefRules.applyOrElse(tableRef, identity[Relation])

    // Should resolve to TableScan using the leaf name lookup
    resolved.isInstanceOf[TableScan] shouldBe true

end TyperTest
