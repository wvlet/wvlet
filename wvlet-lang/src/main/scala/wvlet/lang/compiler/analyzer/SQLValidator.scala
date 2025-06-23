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

import wvlet.lang.compiler.{CompilationUnit, Context, Phase}
import wvlet.lang.model.expr.{Eq, Expression, NotEq, NullLiteral}
import wvlet.lang.model.plan.LogicalPlan

/**
  * Validates SQL-specific patterns and emits warnings for potentially confusing SQL behaviors
  */
class SQLValidator extends Phase("sql-validator"):

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    // Only validate SQL files
    if unit.sourceFile.isSQL then
      val plan = unit.resolvedPlan
      validatePlan(plan, context)
    unit

  private def validatePlan(plan: LogicalPlan, context: Context): Unit =
    // Walk through all expressions in the plan
    plan.transformUpExpressions { expr =>
      validateExpression(expr, context)
      expr
    }

  private def validateExpression(expr: Expression, context: Context): Unit =
    expr match
      case eq @ Eq(left, right, span) =>
        checkNullComparison(eq, left, right, "=", context)
      case neq @ NotEq(left, right, span) =>
        checkNullComparison(neq, left, right, "!=", context)
      case _ =>
      // No validation needed

  private def checkNullComparison(
      expr: Expression,
      left: Expression,
      right: Expression,
      op: String,
      context: Context
  ): Unit =
    (left, right) match
      case (_: NullLiteral, _) =>
        val suggestion =
          s"${right.pp} is ${
              if op == "!=" then
                "not "
              else
                ""
            }null"
        val loc = context.sourceLocationAt(expr.span)
        context
          .workEnv
          .warn(
            s"${loc}: Comparison with null using '${op}' may yield undefined results. Consider using: ${suggestion}"
          )
      case (_, _: NullLiteral) =>
        val suggestion =
          s"${left.pp} is ${
              if op == "!=" then
                "not "
              else
                ""
            }null"
        val loc = context.sourceLocationAt(expr.span)
        context
          .workEnv
          .warn(
            s"${loc}: Comparison with null using '${op}' may yield undefined results. Consider using: ${suggestion}"
          )
      case _ =>
      // No warning needed

end SQLValidator
