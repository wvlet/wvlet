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
import wvlet.lang.engine.expr.OrExpr
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.Optimization

/**
  * Rewrites conditions into the shape the later passes key on. It runs first.
  *
  * The planner still factors the conditions that join planning reads (inner join trees, ON
  * conditions, conditions pushed below a left join) on the compiler's expressions, before binding,
  * because join order is chosen while binding. Those rules move here with join ordering.
  */
object Normalize extends RuleBatch("normalize", Seq(FactorOr))

/**
  * `(a and b) or (a and c)` => `a and (b or c)`, and `a or (a and b)` => `a`, over the conjuncts of
  * a filter. Conjuncts are matched by their bound signature, which names columns by id. Both
  * rewrites keep the value under three-valued logic (and distributes over or, and absorption holds,
  * with NULL as unknown)
  */
object FactorOr extends Rule:
  override def optimization: Optimization = Optimization.FactorOr

  override def apply(p: RuntimePlan): Option[RuntimePlan] =
    p match
      case f: RuntimePlan.Filter =>
        val conds    = leftConjuncts(f.cond.expr)
        val factored = conds.map(factor)
        if factored.forall(_.isEmpty) then
          None
        else
          val all = conds.zip(factored).flatMap((c, fc) => fc.getOrElse(List(c)))
          Some(f.copy(cond = BExpr(and(all))))
      case _ =>
        None

  /**
    * The conjuncts of a condition along its left spine: the binder reduces a filter's conjuncts
    * with `and` from the left, and parsed `a and b and c` nests the same way, so reducing them
    * again gives the same expression. A conjunct on the right stays whole, as `x between a and b`
    * (bound as `x >= a and x <= b`) does in the planner's factoring over compiler expressions
    */
  private def leftConjuncts(e: VExpr): List[VExpr] =
    e match
      case AndExpr(l, r) =>
        leftConjuncts(l) :+ r
      case other =>
        List(other)

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
        val branches = disjuncts(o).map(leftConjuncts)
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
