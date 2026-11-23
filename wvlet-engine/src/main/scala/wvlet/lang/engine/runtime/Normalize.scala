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
package wvlet.lang.engine.runtime

import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.OrExpr
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.Optimization

/**
  * Rewrites conditions into the shape the later passes key on. It runs first.
  *
  * The planner still factors the conditions that legacy join planning reads (conditions with
  * subqueries, and joins the binder leaves to the planner) on the compiler's expressions, before
  * binding.
  */
object Normalize extends RuleBatch("normalize", Seq(FactorOr))

/**
  * `(a and b) or (a and c)` => `a and (b or c)`, and `a or (a and b)` => `a`, over the conjuncts of
  * a filter, an outer join's ON condition, and the conditions of an inner join tree, before
  * [[PushDownFilters]] and [[ReorderJoins]] read them (a common equality becomes a join key).
  * Conjuncts are matched by their bound signature, which names columns by id. Both rewrites keep
  * the value under three-valued logic (and distributes over or, and absorption holds, with NULL as
  * unknown)
  */
object FactorOr extends Rule:
  override def optimization: Optimization = Optimization.FactorOr

  override def apply(p: RuntimePlan): Option[RuntimePlan] =
    p match
      case f: RuntimePlan.Filter =>
        factorConjuncts(f.cond).map(c => f.copy(cond = c))
      case j: RuntimePlan.Join if j.residual.nonEmpty =>
        // An outer join's ON condition, before PushDownFilters turns its equalities into keys
        factorConjuncts(j.residual.get).map(c => j.copy(residual = Some(c)))
      case j: RuntimePlan.InnerJoins =>
        // Each condition is one conjunct: it is replaced in place by its factored conjuncts
        val factored = j.conds.map(c => factor(c.cond.expr))
        if factored.forall(_.isEmpty) then
          None
        else
          Some(
            j.copy(conds =
              j.conds.zip(factored).flatMap((c, fc) => fc.fold(List(c))(_.map(c.withCond)))
            )
          )
      case _ =>
        None

  /**
    * The conjuncts of a condition, as the planner splits compiler expressions: every `and` is split
    * except a bound `x between a and b` (`x >= a and x <= b`), which stays one conjunct. The binder
    * reduces a filter's bound conjuncts with `and` from the left, so reducing them again gives the
    * same expression
    */
  private[runtime] def conjuncts(e: VExpr): List[VExpr] =
    e match
      case AndExpr(Compare(CmpOp.Ge, x, _), Compare(CmpOp.Le, y, _)) if x.sig == y.sig =>
        List(e)
      case AndExpr(l, r) =>
        conjuncts(l) ++ conjuncts(r)
      case other =>
        List(other)

  /** The condition with each of its conjuncts factored, or None when none is */
  private def factorConjuncts(cond: BExpr): Option[BExpr] =
    val conds    = conjuncts(cond.expr)
    val factored = conds.map(factor)
    if factored.forall(_.isEmpty) then
      None
    else
      Some(BExpr(and(conds.zip(factored).flatMap((c, fc) => fc.getOrElse(List(c))))))

  private def disjuncts(e: VExpr): List[VExpr] =
    e match
      case OrExpr(l, r) =>
        disjuncts(l) ++ disjuncts(r)
      case other =>
        List(other)

  private def and(es: List[VExpr]): VExpr = es.reduce((a, b) => AndExpr(a, b))

  /** The factored conjuncts of an `or`, or None when no conjunct is common to every branch */
  private def factor(e: VExpr): Option[List[VExpr]] =
    e match
      case o: OrExpr =>
        val branches = disjuncts(o).map(conjuncts)
        val common   = branches.map(_.map(_.sig).toSet).reduce(_ intersect _)
        if common.isEmpty then
          None
        else
          val commonExprs = branches.head.filter(c => common.contains(c.sig))
          val rest        = branches.map(_.filterNot(c => common.contains(c.sig)))
          if rest.exists(_.isEmpty) then
            Some(commonExprs)
          else
            Some(commonExprs :+ rest.map(and).reduce((a, b) => OrExpr(a, b)))
      case _ =>
        None

end FactorOr
