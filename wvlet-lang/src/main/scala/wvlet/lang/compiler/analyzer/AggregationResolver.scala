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
  * Resolution of aggregation expressions and grouping-key indexes, shared between the current
  * TypeResolver and the new Typer.
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

  private def resolveAggregationExpr(aggFunctions: List[Symbol])(using
      ctx: Context
  ): PartialFunction[Expression, Expression] =
    case e: ShouldExpr =>
      // do not resolve aggregation expr in test expressions
      e
    case d: DotRef =>
      val dd  = d.transformChildExpressions(resolveAggregationExpr(aggFunctions))
      val nme = dd.name.toTermName
      aggFunctions
        .find(_.name == nme)
        .map(_.symbolInfo)
        .collect { case m: MethodSymbolInfo =>
          FunctionInliner.inlineFunctionBody(d, m, Nil)
        }
        .getOrElse(dd)
    case other =>
      other.transformChildExpressions(resolveAggregationExpr(aggFunctions))

  /**
    * Inline aggregation functions (e.g. {{{expr.sum}}}) applied without an explicit GroupBy node in
    * selection expressions
    */
  def resolveNoGroupByAggregations(q: Relation)(using ctx: Context): Relation =
    val aggFunctions = aggregationFunctions(ctx)
    q.transformUp { case s: GeneralSelection =>
        s.transformChildExpressions(resolveAggregationExpr(aggFunctions))
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
