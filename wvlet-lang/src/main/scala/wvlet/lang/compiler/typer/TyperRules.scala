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

import wvlet.lang.model.plan.LogicalPlan
import wvlet.lang.model.expr.*
import wvlet.lang.model.Type
import wvlet.lang.model.Type.NoType
import wvlet.lang.model.Type.ErrorType
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.*

/**
  * Composable typing rules using PartialFunction pattern. Each rule types a specific kind of
  * SyntaxTreeNode (LogicalPlan or Expression).
  */
object TyperRules:

  /**
    * All typing rules for expressions
    */
  def exprRules(using ctx: TyperContext): PartialFunction[Expression, Expression] =
    literalRules orElse identifierRules orElse binaryOpRules orElse castRules orElse
      caseExprRules orElse dotRefRules orElse functionApplyRules

  /**
    * All typing rules composed together for LogicalPlan
    */
  def allRules(using ctx: TyperContext): PartialFunction[LogicalPlan, LogicalPlan] = {
    case e: Expression if exprRules.isDefinedAt(e) =>
      exprRules(e).asInstanceOf[LogicalPlan]
  }
  // More rules will be added here as we implement them:
  // orElse functionApplyRules
  // orElse dotRefRules
  // orElse projectRules
  // orElse filterRules
  // orElse joinRules
  // orElse modelDefRules
  // orElse packageDefRules

  /**
    * Rules for typing literal expressions
    */
  def literalRules(using ctx: TyperContext): PartialFunction[Expression, Expression] = {
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
  def identifierRules(using ctx: TyperContext): PartialFunction[Expression, Expression] = {
    case id: Identifier =>
      // First check if it's a named symbol in scope
      ctx.findSymbol(id.toTermName) match
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
              // Unresolved identifier
              id.tpe = ErrorType(s"Unresolved identifier: ${id.unquotedValue}")
              id
  }

  /**
    * Rules for typing binary operations
    */
  def binaryOpRules(using ctx: TyperContext): PartialFunction[Expression, Expression] = {
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

          // String concatenation for Add
          case (BinaryExprType.Add, StringType, StringType) =>
            StringType

          // Type error
          case _ =>
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
                IntType | LongType | FloatType | DoubleType,
                IntType | LongType | FloatType | DoubleType
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
            ErrorType(s"Cannot compare $leftTpe < $rightTpe: incompatible types")
      op

    case op: LessThanOrEq =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe
      op.tpe =
        (leftTpe, rightTpe) match
          case (
                IntType | LongType | FloatType | DoubleType,
                IntType | LongType | FloatType | DoubleType
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
            ErrorType(s"Cannot compare $leftTpe <= $rightTpe: incompatible types")
      op

    case op: GreaterThan =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe
      op.tpe =
        (leftTpe, rightTpe) match
          case (
                IntType | LongType | FloatType | DoubleType,
                IntType | LongType | FloatType | DoubleType
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
            ErrorType(s"Cannot compare $leftTpe > $rightTpe: incompatible types")
      op

    case op: GreaterThanOrEq =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe
      op.tpe =
        (leftTpe, rightTpe) match
          case (
                IntType | LongType | FloatType | DoubleType,
                IntType | LongType | FloatType | DoubleType
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
          case _ =>
            ErrorType(s"Type error in AND: expected boolean operands, got $leftTpe AND $rightTpe")

      op

    case op: Or =>
      val leftTpe  = op.left.tpe
      val rightTpe = op.right.tpe

      op.tpe =
        (leftTpe, rightTpe) match
          case (BooleanType, BooleanType) =>
            BooleanType
          case _ =>
            ErrorType(s"Type error in OR: expected boolean operands, got $leftTpe OR $rightTpe")

      op
  }

  /**
    * Rules for typing Cast expressions
    */
  def castRules(using ctx: TyperContext): PartialFunction[Expression, Expression] = {
    case cast: Cast =>
      // Cast expression type is simply the target cast type
      cast.tpe = cast.castType
      cast
  }

  /**
    * Rules for typing Case/When expressions
    */
  def caseExprRules(using ctx: TyperContext): PartialFunction[Expression, Expression] = {
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
    * Rules for typing DotRef expressions (field access)
    */
  def dotRefRules(using ctx: TyperContext): PartialFunction[Expression, Expression] = {
    case dotRef: DotRef =>
      val qualifierType = dotRef.qualifier.tpe
      val fieldName     = dotRef.name.leafName

      qualifierType match
        case schema: SchemaType =>
          // Look up field in schema
          schema.fields.find(_.name.name == fieldName) match
            case Some(field) =>
              dotRef.tpe = field.dataType
            case None =>
              dotRef.tpe = ErrorType(s"Field $fieldName not found in schema")

        case NoType =>
          // Qualifier not typed yet, keep as NoType
          dotRef.tpe = NoType

        case _: ErrorType =>
          // Propagate error from qualifier
          dotRef.tpe = qualifierType

        case _ =>
          // If qualifier has been typed and is not a SchemaType, it's a type error
          dotRef.tpe = ErrorType(s"Type ${qualifierType} does not support field access via '.'")

      dotRef
  }

  /**
    * Rules for typing FunctionApply expressions
    */
  def functionApplyRules(using ctx: TyperContext): PartialFunction[Expression, Expression] = {
    case funcApply: FunctionApply =>
      // The type of a function application is the function's return type
      funcApply.base.tpe match
        case ft: Type.FunctionType =>
          funcApply.tpe = ft.returnType
        case NoType =>
          // Base not typed yet
          funcApply.tpe = NoType
        case et: ErrorType =>
          // Propagate error from base
          funcApply.tpe = et
        case other =>
          // Not a function type
          funcApply.tpe = ErrorType(s"Cannot apply non-function type ${other}")
      funcApply
  }

  /**
    * Find common type among a list of types (simple type coercion)
    */
  private def findCommonType(types: Seq[Type]): Type =
    if types.isEmpty then
      NoType
    else if types.forall(_ == types.head) then
      // All types are the same
      types.head
    else
      // Check for numeric type promotion
      val allNumeric = types.forall {
        case IntType | LongType | FloatType | DoubleType =>
          true
        case _ =>
          false
      }

      if allNumeric then
        // Promote to widest numeric type
        val promotionOrder = Seq(DoubleType, FloatType, LongType, IntType)
        promotionOrder.find(t => types.exists(_ == t)).getOrElse(IntType)
      else
        // Check if any are error types
        types.collectFirst { case e: ErrorType =>
          e
        } match
          case Some(error) =>
            error
          case None =>
            // No common type found
            ErrorType(s"No common type found among: ${types.mkString(", ")}")

end TyperRules
