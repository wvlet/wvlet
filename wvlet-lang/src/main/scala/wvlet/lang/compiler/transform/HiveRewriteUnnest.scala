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
import wvlet.lang.compiler.DBType.Hive
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.lang.model.plan.JoinType.*

/**
  * Rewrite UNNEST to LATERAL VIEW for Hive
  */
object HiveRewriteUnnest extends Phase("hive-rewrite-unnest"):
  private def rewriteRules: List[RewriteRule] = List(rewriteUnnestToLateralView)

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    val resolvedPlan = unit.resolvedPlan
    val newPlan =
      if context.dbType != Hive then
        resolvedPlan
      else
        resolvedPlan.transformUp {
          case m: ModelDef =>
            m.symbol.tree = m
            m
          case q: Query =>
            RewriteRule.rewrite(q, rewriteRules, context)
        }

    unit.resolvedPlan = newPlan
    unit

  private object rewriteUnnestToLateralView extends RewriteRule:
    override def apply(context: Context): RewriteRule.PlanRewriter =
      // Handle aliased UNNEST
      case j @ Join(
            joinType,
            left,
            ar @ AliasedRelation(u: Unnest, alias, columnNamesOpt, _),
            cond,
            asof,
            span
          ) if joinType == CrossJoin =>
        // Transform CROSS JOIN UNNEST to LATERAL VIEW
        // For now, we support single column unnest
        if u.columns.size == 1 && columnNamesOpt.exists(_.size == 1) then
          val expr          = u.columns.head
          val columnAliases = columnNamesOpt.get.map(nt => NameExpr.fromString(nt.name.toString))
          LateralView(
            child = left,
            exprs = Seq(
              FunctionApply(
                NameExpr.fromString("explode"),
                List(FunctionArg(None, expr, false, Nil, NoSpan)),
                None,
                None,
                NoSpan
              )
            ),
            tableAlias = alias,
            columnAliases = columnAliases,
            span = span
          )
        else
          // Multi-column unnest or mismatched column alias count not supported yet
          j

      // Handle non-aliased UNNEST (fallback for compatibility)
      case j @ Join(joinType, left, u: Unnest, cond, asof, span) if joinType == CrossJoin =>
        // Transform CROSS JOIN UNNEST to LATERAL VIEW with default aliases
        if u.columns.size == 1 then
          val expr        = u.columns.head
          val tableAlias  = NameExpr.fromString("unnest_table")
          val columnAlias = NameExpr.fromString("unnest_col")
          LateralView(
            child = left,
            exprs = Seq(
              FunctionApply(
                NameExpr.fromString("explode"),
                List(FunctionArg(None, expr, false, Nil, NoSpan)),
                None,
                None,
                NoSpan
              )
            ),
            tableAlias = tableAlias,
            columnAliases = Seq(columnAlias),
            span = span
          )
        else
          // Multi-column unnest not supported yet
          j

    end apply

  end rewriteUnnestToLateralView

end HiveRewriteUnnest
