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
package wvlet.lang.compiler.transform

import wvlet.lang.compiler.RewriteRule.PlanRewriter
import wvlet.lang.compiler.*
import wvlet.lang.compiler.DBType.Trino
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * Rewrite pivot relations to group by queries for engines not supporting pivot function (e.g.,
  * Trino)
  */
object TrinoRewritePivot extends Phase("rewrite-pivot"):
  private def rewriteRules: List[RewriteRule] = List(rewritePivotAggForTrino, rewritePivotForTrino)

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    val resolvedPlan = unit.resolvedPlan
    val newPlan =
      if context.dbType != Trino then
        resolvedPlan
      else
        resolvedPlan.transform { case q: Query =>
          RewriteRule.rewrite(q, rewriteRules, context)
        }

    unit.resolvedPlan = newPlan
    unit

  private object rewritePivotAggForTrino extends RewriteRule:
    override def apply(context: Context): PlanRewriter =
      case a: Agg if a.child.isPivot && context.dbType == DBType.Trino =>
        rewritePivot(a.child.asInstanceOf[Pivot], a.selectItems)

  private object rewritePivotForTrino extends RewriteRule:
    override def apply(context: Context): PlanRewriter =
      case p: Pivot if context.dbType == DBType.Trino =>
        rewritePivot(p, Nil)
    end apply

  end rewritePivotForTrino

  private def rewritePivot(p: Pivot, aggExprs: List[Attribute]): Relation =
    // Rewrite pivot function for Trino
    val pivotKeyNames = p.pivotKeys.map(_.name.fullName)
    val pivotGroupingKeys: List[GroupingKey] =
      if p.groupingKeys.isEmpty then
        // If no grouping key is given, use all columns in the relation
        p.child
          .relationType
          .fields
          .filterNot(f => pivotKeyNames.contains(f.name.name))
          .map { f =>
            UnresolvedGroupingKey(NameExpr.EmptyName, UnquotedIdentifier(f.name.name, None), None)
          }
          .toList
      else
        p.groupingKeys

    // Wrap with group by for specifying aggregation keys
    val g = GroupBy(p.child, pivotGroupingKeys, p.nodeLocation)

    // Pivot keys are used as grouping keys
    val pivotKeys: List[Attribute] = p.groupingKeys.map(k => SingleColumn(k.name, k.name, None))
    // Pivot aggregation expressions
    val pivotAggExprs: List[Attribute] = p
      .pivotKeys
      .flatMap { pivotKey =>
        val targetColumn = pivotKey.name
        val pivotExprs = pivotKey
          .values
          .flatMap { v =>
            val exprs = List.newBuilder[SingleColumn]
            if aggExprs.isEmpty then
              // count_if(pivot_column = value) as "value"
              exprs +=
                SingleColumn(
                  DoubleQuotedIdentifier(v.stringValue, None),
                  FunctionApply(
                    UnquotedIdentifier("count_if", None),
                    List(FunctionArg(None, Eq(targetColumn, v, None), None)),
                    None,
                    None
                  ),
                  None
                )
            else
              val fieldNames = p.inputRelationType.fields.map(_.name).toSet
              aggExprs.foreach { aggExpr =>
                // Rewrite agg expr to conditional aggregation. For example:
                // sum(price) => sum(if(pivot_column = value, price, null)) as "value"
                val pivotAggExpr = aggExpr.transformUpExpression {
                  // Replace input relation field access to conditional access
                  case id: Identifier if fieldNames.contains(id.toTermName) =>
                    FunctionApply(
                      UnquotedIdentifier("if", None),
                      List(
                        FunctionArg(None, Eq(targetColumn, v, None), None),
                        FunctionArg(None, id, None),
                        FunctionArg(None, NullLiteral(None), None)
                      ),
                      None,
                      None
                    )
                }
                exprs +=
                  SingleColumn(DoubleQuotedIdentifier(v.stringValue, None), pivotAggExpr, None)
              }
            end if
            exprs.result
          }
        pivotExprs
      }
    Project(g, pivotKeys ++ pivotAggExprs, p.nodeLocation)

  end rewritePivot

end TrinoRewritePivot
