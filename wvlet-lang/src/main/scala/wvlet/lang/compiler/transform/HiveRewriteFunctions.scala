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
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * Rewrite functions for Hive compatibility
  */
object HiveRewriteFunctions extends Phase("hive-rewrite-functions"):
  private def rewriteRules: List[RewriteRule] = List(rewriteFunctions)

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

  private object rewriteFunctions extends RewriteRule:
    override def apply(context: Context): RewriteRule.PlanRewriter =
      plan =>
        plan.transformExpressions {
          case f @ FunctionApply(n: NameExpr, args, window, span) =>
            n.leafName match
              case "array_agg" =>
                FunctionApply(NameExpr.fromString("collect_list"), args, window, span)
              case "array_distinct" =>
                FunctionApply(NameExpr.fromString("collect_set"), args, window, span)
              case "regexp_like" =>
                FunctionApply(NameExpr.fromString("regexp"), args, window, span)
              case _ =>
                f
          case other =>
            other
        }

end HiveRewriteFunctions
