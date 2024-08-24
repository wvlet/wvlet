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

import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.RewriteRule.PlanRewriter
import wvlet.lang.compiler.{CompilationUnit, Context, Phase, RewriteRule}
import wvlet.lang.model.plan.*

/**
  * Generate incremental query plans corresponding for Subscription nodes
  */
object Incrementalize extends Phase("incrementalize"):

  private val rewriteRules: List[RewriteRule] =
    ResolveRef ::
      IncrementalizeSimpleScan // Incrementalize simple table scan queries without aggregation and joins
      :: Nil

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    val subscriptionPlans = List.newBuilder[LogicalPlan]

    unit
      .resolvedPlan
      .traverse:
        case s: Subscribe =>
          val incrementalPlan = RewriteRule.rewriteRelation(s.child, rewriteRules, context)
          subscriptionPlans += s.copy(child = incrementalPlan)

    unit.subscriptionPlans = subscriptionPlans.result()
    unit

  object ResolveRef extends RewriteRule:
    override def apply(context: Context): PlanRewriter =
      case r: ModelScan =>
        context.compilationUnit.findRelationRef(r.name.name) match
          case Some(relation) =>
            relation
          case None =>
            warn(s"Relation ${r.name} not found in the context")
            r

  /**
    * Incrementalize simple table scan queries without aggregation and joins
    */
  object IncrementalizeSimpleScan extends RewriteRule:

    private def isSimpleScan(plan: LogicalPlan, context: Context): Boolean =
      plan match
        case _: TableScan =>
          true
        case _: Filter =>
          // TODO Exclude filter with correlated sub-queries
          true
        case _: Project =>
          // TODO Exclude projection with window functions or sub que`ries
          true
        case _: Transform =>
          true
        case n: NamedRelation =>
          isSimpleScan(n.child, context)
        case _: TableRef =>
          true
        case _: Values =>
          true
        case p: ParenthesizedRelation =>
          isSimpleScan(p.child, context)
        case a: AliasedRelation =>
          isSimpleScan(a.child, context)
        case r: ModelScan =>
          context.compilationUnit.findRelationRef(r.name.name) match
            case Some(rel) =>
              isSimpleScan(rel, context)
            case _ =>
              false
        case _ =>
          false

    override def isTargetPlan(plan: LogicalPlan, context: Context): Boolean = isSimpleScan(
      plan,
      context
    )

    override def apply(context: Context): PlanRewriter =
      case t: TableScan =>
        IncrementalTableScan(TableName.parse(t.name.fullName), t.schema, t.columns, t.nodeLocation)

    override def postProcess(plan: LogicalPlan, context: Context): LogicalPlan = IncrementalAppend(
      plan.asInstanceOf[Relation],
      plan.nodeLocation
    )

  end IncrementalizeSimpleScan

end Incrementalize
