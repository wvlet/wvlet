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

import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.analyzer.FunctionInliner
import wvlet.lang.model.plan.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.Type
import wvlet.lang.model.Type.NoType
import wvlet.lang.model.Type.ErrorType
import wvlet.lang.model.Type.ImportType
import wvlet.lang.model.Type.PackageType
import wvlet.lang.model.Type.FunctionType
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.*

/**
  * Composable typing rules using PartialFunction pattern. Each rule types a specific kind of
  * SyntaxTreeNode (LogicalPlan or Expression).
  *
  * Context carries TyperState with inputType and errors (following Scala 3 pattern).
  */
object TyperRules:

  /**
    * All typing rules for expressions
    */
  def exprRules(using ctx: Context): PartialFunction[Expression, Expression] = (
    literalRules orElse identifierRules orElse binaryOpRules orElse castRules orElse
      caseExprRules orElse interpolatedStringRules orElse parenRules orElse dotRefRules orElse
      functionApplyRules orElse materializeStructuralTypeRules
  ) andThen materializeStructuralType

  /**
    * Record a structurally computed dataType in the tpe field so that tpe becomes the
    * authoritative, per-node type record (the first step of unifying the two fields, #71 / issue
    * #392). Applies to nodes with a structural dataType override (e.g. SingleColumn delegating to
    * its expression) that no dedicated typing rule covers yet
    */
  private def materializeStructuralType(e: Expression): Expression =
    if !e.isTyped then
      val dt = e.dataType
      if dt.isResolved then
        e.tpe = dt
    e

  /**
    * Catch-all rule so that the materialization above runs for expression classes that no dedicated
    * typing rule matches
    */
  private def materializeStructuralTypeRules(using
      ctx: Context
  ): PartialFunction[Expression, Expression] = { case e: Expression =>
    e
  }

  /**
    * All typing rules for relations. Sets tpe field from relationType.
    */
  def relationRules(using ctx: Context): PartialFunction[Relation, Relation] = defaultRelationRules

  /**
    * Rules for typing literal expressions
    */
  def literalRules(using ctx: Context): PartialFunction[Expression, Expression] = {
    case lit: LongLiteral =>
      lit.tpe = LongType
      lit

    case lit: DoubleLiteral =>
      lit.tpe = DoubleType
      lit

    case lit: StringLiteral =>
      lit.tpe = StringType
      lit

    case lit: TrueLiteral =>
      lit.tpe = BooleanType
      lit

    case lit: FalseLiteral =>
      lit.tpe = BooleanType
      lit

    case lit: NullLiteral =>
      lit.tpe = NullType
      lit
  }

  /**
    * Rules for typing identifiers
    */
  def identifierRules(using ctx: Context): PartialFunction[Expression, Expression] = {
    case w: Wildcard =>
      // A wildcard (*) selects the full input row, so it carries the input relation type
      if ctx.inputType.isResolved then
        w.tpe = ctx.inputType
      w

    case id: Identifier =>
      // Look up symbol from scope, imports, and global scope
      ctx.findSymbolByName(id.toTermName) match
        case Some(sym) =>
          id.symbol = sym // Attach symbol for named reference
          id.tpe = sym.dataType
          id

        case None =>
          // Check input relation for column
          ctx.inputType.fields.find(_.name.name == id.unquotedValue) match
            case Some(field) =>
              id.tpe = field.dataType
              id
            case None =>
              // Consult RelationType.find as well, which also resolves virtual columns
              // (e.g. aggregated array columns of a GroupBy input)
              ctx.inputType.find(_.name == id.fullName) match
                case Some(attr) =>
                  id.tpe = attr.dataType
                  id
                case None =>
                  // Unresolved identifier
                  id.tpe = ErrorType(s"Unresolved identifier: ${id.unquotedValue}")
                  id
  }

  /**
    * Rules for typing binary operations
    */
  def binaryOpRules(using ctx: Context): PartialFunction[Expression, Expression] = {
    // Arithmetic binary expressions
    case op: ArithmeticBinaryExpr =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe

      op.tpe =
        (op.exprType, leftTpe, rightTpe) match
          // Integer arithmetic
          case (_, IntType, IntType) =>
            IntType
          case (_, LongType, LongType) =>
            LongType
          case (_, FloatType, FloatType) =>
            FloatType
          case (_, DoubleType, DoubleType) =>
            DoubleType

          // Mixed numeric types - promote to larger type
          case (_, IntType, LongType) | (_, LongType, IntType) =>
            LongType
          case (_, IntType | LongType, DoubleType) | (_, DoubleType, IntType | LongType) =>
            DoubleType
          case (_, IntType | LongType, FloatType) | (_, FloatType, IntType | LongType) =>
            FloatType
          case (_, FloatType, DoubleType) | (_, DoubleType, FloatType) =>
            DoubleType

          // Decimal arithmetic keeps the decimal type
          case (_, d: DecimalType, _: DecimalType | IntType | LongType | FloatType | DoubleType) =>
            d
          case (_, IntType | LongType | FloatType | DoubleType, d: DecimalType) =>
            d

          // String concatenation for Add: a string operand absorbs the other side, which is
          // coerced to string (see RewriteExpr.RewriteStringConcat)
          case (BinaryExprType.Add, StringType, _) | (BinaryExprType.Add, _, StringType) =>
            StringType

          // Operands not typed yet - defer
          case (_, NoType, _) | (_, _, NoType) =>
            NoType

          // Propagate operand errors
          case (_, e: ErrorType, _) =>
            e
          case (_, _, e: ErrorType) =>
            e

          // Type error between two resolved, incompatible operand types
          case _ =>
            ctx.addTyperError(TypeMismatch(leftTpe, rightTpe, op))
            ErrorType(s"Type error in ${op.exprType}: $leftTpe ${op.exprType} $rightTpe")

      op

    // Comparison operators - require comparable types
    case op: Eq =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe
      // Equality is allowed for any types (including null checks)
      op.tpe = BooleanType
      op

    case op: NotEq =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe
      // Inequality is allowed for any types (including null checks)
      op.tpe = BooleanType
      op

    case op: LessThan =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe
      op.tpe =
        (leftTpe, rightTpe) match
          // Numeric comparisons
          case (
                IntType | LongType | FloatType | DoubleType | _: DecimalType,
                IntType | LongType | FloatType | DoubleType | _: DecimalType
              ) =>
            BooleanType
          // String comparisons
          case (StringType, StringType) =>
            BooleanType
          // Same types (for custom types that support ordering)
          case (l, r) if l == r && l != BooleanType =>
            BooleanType
          // NoType means untyped yet - allow it
          case (NoType, _) | (_, NoType) =>
            BooleanType
          // Propagate errors
          case (e: ErrorType, _) =>
            e
          case (_, e: ErrorType) =>
            e
          // Type mismatch
          case _ =>
            ctx.addTyperError(TypeMismatch(leftTpe, rightTpe, op))
            ErrorType(s"Cannot compare $leftTpe < $rightTpe: incompatible types")
      op

    case op: LessThanOrEq =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe
      op.tpe =
        (leftTpe, rightTpe) match
          case (
                IntType | LongType | FloatType | DoubleType | _: DecimalType,
                IntType | LongType | FloatType | DoubleType | _: DecimalType
              ) =>
            BooleanType
          case (StringType, StringType) =>
            BooleanType
          case (l, r) if l == r && l != BooleanType =>
            BooleanType
          case (NoType, _) | (_, NoType) =>
            BooleanType
          case (e: ErrorType, _) =>
            e
          case (_, e: ErrorType) =>
            e
          case _ =>
            ctx.addTyperError(TypeMismatch(leftTpe, rightTpe, op))
            ErrorType(s"Cannot compare $leftTpe <= $rightTpe: incompatible types")
      op

    case op: GreaterThan =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe
      op.tpe =
        (leftTpe, rightTpe) match
          case (
                IntType | LongType | FloatType | DoubleType | _: DecimalType,
                IntType | LongType | FloatType | DoubleType | _: DecimalType
              ) =>
            BooleanType
          case (StringType, StringType) =>
            BooleanType
          case (l, r) if l == r && l != BooleanType =>
            BooleanType
          case (NoType, _) | (_, NoType) =>
            BooleanType
          case (e: ErrorType, _) =>
            e
          case (_, e: ErrorType) =>
            e
          case _ =>
            ctx.addTyperError(TypeMismatch(leftTpe, rightTpe, op))
            ErrorType(s"Cannot compare $leftTpe > $rightTpe: incompatible types")
      op

    case op: GreaterThanOrEq =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe
      op.tpe =
        (leftTpe, rightTpe) match
          case (
                IntType | LongType | FloatType | DoubleType | _: DecimalType,
                IntType | LongType | FloatType | DoubleType | _: DecimalType
              ) =>
            BooleanType
          case (StringType, StringType) =>
            BooleanType
          case (l, r) if l == r && l != BooleanType =>
            BooleanType
          case (NoType, _) | (_, NoType) =>
            BooleanType
          case (e: ErrorType, _) =>
            e
          case (_, e: ErrorType) =>
            e
          case _ =>
            ctx.addTyperError(TypeMismatch(leftTpe, rightTpe, op))
            ErrorType(s"Cannot compare $leftTpe >= $rightTpe: incompatible types")
      op

    // Logical operators
    case op: And =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe

      op.tpe =
        (leftTpe, rightTpe) match
          case (BooleanType, BooleanType) =>
            BooleanType
          case (NoType, _) | (_, NoType) =>
            // Operands not typed yet - defer
            NoType
          case (e: ErrorType, _) =>
            e
          case (_, e: ErrorType) =>
            e
          case _ =>
            ctx.addTyperError(TypeMismatch(BooleanType, leftTpe, op))
            ErrorType(s"Type error in AND: expected boolean operands, got $leftTpe AND $rightTpe")

      op

    case op: Or =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe

      op.tpe =
        (leftTpe, rightTpe) match
          case (BooleanType, BooleanType) =>
            BooleanType
          case (NoType, _) | (_, NoType) =>
            // Operands not typed yet - defer
            NoType
          case (e: ErrorType, _) =>
            e
          case (_, e: ErrorType) =>
            e
          case _ =>
            ctx.addTyperError(TypeMismatch(BooleanType, leftTpe, op))
            ErrorType(s"Type error in OR: expected boolean operands, got $leftTpe OR $rightTpe")

      op
  }

  /**
    * Rules for typing Cast expressions
    *
    * TODO: When castType is an UnresolvedType (e.g., user-defined type alias), we need to resolve
    * it via symbol table lookup. Currently assumes castType is already resolved (works for
    * primitives).
    */
  def castRules(using ctx: Context): PartialFunction[Expression, Expression] = { case cast: Cast =>
    cast.tpe = cast.castType
    cast
  }

  /**
    * Rules for typing Case/When expressions
    */
  def caseExprRules(using ctx: Context): PartialFunction[Expression, Expression] = {
    case caseExpr: CaseExpr =>
      // Find common type among all WHEN result clauses and ELSE clause
      val resultTypes = caseExpr.whenClauses.map(_.result.tpe) ++ caseExpr.elseClause.map(_.tpe)

      // Filter out NoType (untyped)
      val typedResults = resultTypes.filter(_ != NoType)

      if typedResults.isEmpty then
        // No typed results yet, keep as NoType
        caseExpr.tpe = NoType
      else
        // Find common type
        val commonType = findCommonType(typedResults)
        caseExpr.tpe = commonType

      caseExpr
  }

  /**
    * Rule for typing string interpolations. An s"..." interpolation always yields a string, while
    * sql"..." fragments have no statically known type
    */
  def interpolatedStringRules(using ctx: Context): PartialFunction[Expression, Expression] = {
    case s: InterpolatedString if s.prefix.fullName == "s" =>
      s.tpe = StringType
      s
  }

  /**
    * Rule for propagating the child type through parenthesized expressions
    */
  def parenRules(using ctx: Context): PartialFunction[Expression, Expression] = {
    case p: ParenthesizedExpression =>
      if p.child.dataType.isResolved then
        p.tpe = p.child.dataType
      else if p.child.isTyped then
        p.tpe = p.child.tpe
      p
  }

  /**
    * Rules for typing DotRef expressions: field access on a schema-typed qualifier, or a method
    * reference (e.g. {{{x.to_double}}}) typed by the method's declared return type. Typing a method
    * reference lets an enclosing call resolve its qualifier type without inlining the reference
    */
  def dotRefRules(using ctx: Context): PartialFunction[Expression, Expression] = {
    case dotRef: DotRef =>
      val qualifierType = dotRef.qualifier.tpe
      val fieldName     = dotRef.name.leafName

      // Typing is best-effort: an ambiguous method reference (e.g. same-named defs in multiple
      // types with a still-unresolved qualifier) is not a type, not a user error. The inlining
      // path reports real ambiguity with a proper source location
      def methodReturnType: Option[DataType] =
        try
          FunctionInliner.findFunctionDef(dotRef).map(_.ft.returnType).filter(_.isResolved)
        catch
          case _: WvletLangException =>
            None

      qualifierType match
        case schema: SchemaType =>
          // Look up field in schema
          schema.fields.find(_.name.name == fieldName) match
            case Some(field) =>
              dotRef.tpe = field.dataType
            case None =>
              dotRef.tpe = methodReturnType.getOrElse(
                ErrorType(s"Field $fieldName not found in schema")
              )

        case NoType =>
          // Qualifier not typed yet; a method reference may still resolve from the legacy
          // dataType of the qualifier
          methodReturnType.foreach { t =>
            dotRef.tpe = t
          }

        case _: ErrorType =>
          // Propagate error from qualifier unless this is a method reference
          dotRef.tpe = methodReturnType.getOrElse(qualifierType)

        case _ =>
          // A non-schema qualifier supports method references (e.g. primitive methods)
          dotRef.tpe = methodReturnType.getOrElse(
            ErrorType(s"Type ${qualifierType} does not support field access via '.'")
          )

      dotRef
  }

  /**
    * Rules for typing FunctionApply expressions
    */
  def functionApplyRules(using ctx: Context): PartialFunction[Expression, Expression] = {
    case funcApply: FunctionApply =>
      // A call whose base is a plain function name passed through to the database engine
      // (e.g. count(*), regexp_matches(...)) is typed from the builtin signature table. The
      // base name itself is typed as a function type of the builtin signature. Method-style
      // calls of builtins (e.g. x.lower()) have a DotRef base and are looked up by the member
      // name
      def builtinReturnType: Option[DataType] =
        funcApply.base match
          case id: Identifier =>
            BuiltinFunctions
              .returnTypeOf(id.fullName, funcApply.args)
              .map { ret =>
                id.tpe = FunctionType(id.toTermName, Nil, ret, Nil)
                ret
              }
          case d: DotRef =>
            BuiltinFunctions.returnTypeOf(d.name.leafName, funcApply.args)
          case _ =>
            None

      // The type of a function application is the function's return type
      funcApply.base.tpe match
        case ft: Type.FunctionType =>
          funcApply.tpe = ft.returnType
        case dt: DataType if dt.isResolved && funcApply.base.isInstanceOf[DotRef] =>
          // A method reference typed by dotRefRules carries its declared return type, which
          // is also the type of applying it
          funcApply.tpe = dt
        case NoType =>
          // Base not typed yet
          builtinReturnType.foreach { t =>
            funcApply.tpe = t
          }
        case et: ErrorType =>
          // Propagate error from base unless this is a known builtin function
          funcApply.tpe = builtinReturnType.getOrElse(et)
        case other =>
          // Not a function type
          funcApply.tpe = builtinReturnType.getOrElse(
            ErrorType(s"Cannot apply non-function type ${other}")
          )
      funcApply
  }

  /**
    * Find common type among a list of types. Delegates to TypeInference.
    */
  private def findCommonType(types: Seq[Type]): Type = TypeInference.findCommonType(types)

  // ============================================
  // Relation Typing Rules
  // ============================================

  /**
    * Default rule for all relations. Sets tpe from relationType. The existing relationType methods
    * in the Relation type hierarchy handle schema computation, so this rule just bridges to the tpe
    * field.
    */
  def defaultRelationRules(using ctx: Context): PartialFunction[Relation, Relation] = {
    case r: Relation =>
      r.tpe = r.relationType
      r
  }

  // ============================================
  // Statement Typing Rules
  // ============================================

  /**
    * Typing rules for statements. Sets tpe field on all statement types to ensure all nodes are
    * typed after the typing phase.
    *
    * Note: FunctionDef and FieldDef are TypeElem (extend Expression, not LogicalPlan) and are
    * handled directly in Typer.typeTypeElem.
    */
  def typeStatement(plan: LogicalPlan)(using ctx: Context): Unit =
    plan match
      case p: PackageDef =>
        p.tpe = PackageType(wvlet.lang.compiler.Name.termName(p.name.fullName))
      case t: TypeDef =>
        t.tpe = t.symbol.dataType
      case m: ModelDef =>
        m.tpe = m.child.tpe
      case i: Import =>
        i.tpe = ImportType(i)
      case v: ValDef =>
        v.tpe = v.dataType
      case t: TopLevelFunctionDef =>
        t.tpe = t.functionDef.tpe
      case _ =>
        () // Other statements don't need typing

end TyperRules
