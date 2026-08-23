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
import wvlet.lang.model.DataType.VarArgType
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
    * Select an aggregation-shorthand definition for the member reference `d` with the given call
    * arguments: the name must be a member of the array type, and must not be a member of the
    * qualifier's own type (e.g. map.size, string.split), which resolves through the typed member
    * path with dialect selection instead.
    */
  private def findAggShorthand(aggFunctions: List[Symbol], d: DotRef, knownArgs: List[FunctionArg])(
      using ctx: Context
  ): Option[MethodSymbolInfo] =
    val nme = d.name.toTermName

    def hasOwnMember: Boolean =
      val qualType = d.qualifier.dataType
      qualType.isResolved &&
      ctx
        .findSymbolByName(qualType.typeName)
        .exists(sym => !sym.symbolInfo.findMember(nme).isNoSymbol)

    aggFunctions
      .find(_.name == nme)
      .filterNot(_ => hasOwnMember)
      .flatMap(sym => FunctionInliner.selectMethodVariant(List(sym.symbolInfo), knownArgs, ctx))

  /**
    * Resolve aggregation shorthands in a single-visit recursion. A FunctionApply and its base
    * DotRef are handled as one unit so that a no-arg variant never shadows an arg-taking overload
    * of the same member (e.g. lag and lag(offset)); parameterized members (count_if, min_by,
    * lag(offset)) bind their arguments here. Each node is visited exactly once — re-traversing
    * subtrees would be exponential on deeply nested expressions (e.g. TPC-DS q4 never finishes)
    */
  private def resolveAggregationExpr(aggFunctions: List[Symbol], e: Expression)(using
      ctx: Context
  ): Expression =
    e match
      case f @ FunctionApply(d: DotRef, _, _, _, _, _) =>
        // Resolve the apply and its base member as a unit; only the qualifier is a free
        // expression
        val q       = resolveAggregationExpr(aggFunctions, d.qualifier)
        val newBase =
          if q eq d.qualifier then
            d
          else
            d.copy(qualifier = q)
        val newArgs = f
          .args
          .map { a =>
            resolveAggregationExpr(aggFunctions, a) match
              case fa: FunctionArg =>
                fa
              case _ =>
                a
          }
        val newWindow = f
          .window
          .map { w =>
            resolveAggregationExpr(aggFunctions, w) match
              case nw: Window =>
                nw
              case _ =>
                w
          }
        val changed =
          !(newBase eq d) || newArgs.lazyZip(f.args).exists(_ ne _) ||
            newWindow.lazyZip(f.window).exists(_ ne _)
        val nf =
          if changed then
            f.copy(base = newBase, args = newArgs, window = newWindow)
          else
            f
        findAggShorthand(aggFunctions, newBase, nf.args) match
          case Some(m)
              if nf.args.size <= m.ft.args.size ||
                m.ft.args.exists(_.dataType.isInstanceOf[VarArgType]) =>
            FunctionInliner.inlineFunctionApply(nf, m)
          case _ =>
            nf
      case d: DotRef =>
        val q  = resolveAggregationExpr(aggFunctions, d.qualifier)
        val nd =
          if q eq d.qualifier then
            d
          else
            d.copy(qualifier = q)
        findAggShorthand(aggFunctions, nd, Nil)
          // A genuinely bare member resolves only to a no-arg variant
          .filter(_.ft.args.isEmpty)
          .map(m => FunctionInliner.inlineFunctionBody(nd, m, Nil))
          .getOrElse(nd)
      case other =>
        other.mapChildExpressions(e => resolveAggregationExpr(aggFunctions, e))
    end match
  end resolveAggregationExpr

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
                  resolveAggregationExpr(aggFunctions, e)
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
