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

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.ContextLogSupport
import wvlet.lang.compiler.MethodSymbolInfo
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.TypeSymbolInfo
import wvlet.lang.compiler.ContextUtil.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * Resolution of aggregation expressions and grouping-key indexes.
  */
object AggregationResolver extends ContextLogSupport:

  /**
    * Aggregation functions (members of the array type) that can be applied to a column or
    * expression via dot syntax, e.g. {{{(l_extendedprice * l_discount).sum}}}
    */
  private def aggregationFunctions(ctx: Context): List[Symbol] = ctx
    .findSymbolByName(Name.typeName("array"))
    .map(_.symbolInfo)
    .collect { case t: TypeSymbolInfo =>
      t.members
    }
    .getOrElse(Nil)

  /**
    * A single-node rewrite rule applied through a bottom-up traversal (transformUpExpression), so
    * child expressions are already resolved when the rule fires. The rule must not re-traverse its
    * subtree: doing so doubles the work at every tree level, which is exponential on deeply nested
    * expressions (e.g. TPC-DS q4 never finishes)
    */
  private def resolveAggregationExpr(aggFunctions: List[Symbol])(using
      ctx: Context
  ): PartialFunction[Expression, Expression] =
    case d: DotRef =>
      val nme = d.name.toTermName

      // A member of the qualifier's own type (e.g. map.size, string.split) resolves through
      // the typed member path with dialect selection, so the aggregation shorthand must not
      // shadow it
      def hasOwnMember: Boolean =
        val qualType = d.qualifier.dataType
        qualType.isResolved &&
        ctx
          .findSymbolByName(qualType.typeName)
          .exists(sym => !sym.symbolInfo.findMember(nme).isNoSymbol)

      aggFunctions
        .find(_.name == nme)
        .filterNot(_ => hasOwnMember)
        .flatMap(sym => FunctionInliner.selectMethodVariant(List(sym.symbolInfo), Nil, ctx))
        // Members with parameters (e.g. count_if, min_by) are bound by their enclosing
        // FunctionApply; inlining them here would drop the arguments
        .filter(_.ft.args.isEmpty)
        .map(m => FunctionInliner.inlineFunctionBody(d, m, Nil))
        .getOrElse(d)

  /**
    * Resolve aggregation expressions in a single traversal: inline aggregation functions applied
    * without an explicit GroupBy node (e.g. {{{expr.sum}}}) in selection expressions, and expand
    * grouping-key indexes (_1, _2, ...) in select clauses
    */
  def resolveAggregations(q: Relation)(using ctx: Context): Relation =
    val aggFunctions = aggregationFunctions(ctx)
    q.transformUp { case r: Relation =>
        // A node can be both a GeneralSelection and an AggSelect (e.g. Agg), so apply both
        // rewrites in sequence
        val withAgg =
          r match
            case s: GeneralSelection =>
              // Resolve within each direct expression of this node only; nested relations are
              // rewritten by their own transformUp visit, and test expressions stay as written
              // because TestRelation is not a GeneralSelection
              s.transformChildExpressions { case e: Expression =>
                  e.transformUpExpression(resolveAggregationExpr(aggFunctions))
                }
                .asInstanceOf[Relation]
            case _ =>
              r
        withAgg match
          case p: AggSelect =>
            resolveGroupingKeyIndexes(p)
          case _ =>
            withAgg
      }
      .asInstanceOf[Relation]

  // Find the first Aggregate node
  private def findAggregate(r: Relation): Option[GroupBy] =
    r match
      case a: GroupBy =>
        Some(a)
      case f: FilteringRelation =>
        findAggregate(f.child)
      case p: Project =>
        findAggregate(p.child)
      case _ =>
        None

  /**
    * Replace grouping key indexes (_1, _2, ...) in select clauses with the referenced grouping keys
    */
  def resolveGroupingKeyIndexes(p: AggSelect)(using ctx: Context): Relation =
    if !p.selectItems.exists(_.nameExpr.isGroupingKeyIndex) then
      p
    else
      findAggregate(p.child) match
        case Some(agg) =>
          val resolved = p.transformChildExpressions {
            case attr: SingleColumn if attr.nameExpr.isGroupingKeyIndex =>
              val index = attr.nameExpr.fullName.stripPrefix("_").toInt - 1
              if index >= agg.groupingKeys.length then
                throw StatusCode
                  .SYNTAX_ERROR
                  .newException(
                    s"Invalid grouping key index: ${attr.nameExpr}",
                    ctx.sourceLocationAt(attr.span)
                  )

              val referencedGroupingKey = agg.groupingKeys(index)
              SingleColumn(referencedGroupingKey.name, expr = referencedGroupingKey, attr.span)
          }
          resolved.asInstanceOf[Relation]
        case None =>
          p

end AggregationResolver
