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
package wvlet.lang.engine.plan

import wvlet.lang.engine.exec.Operator
import wvlet.lang.model.expr.*

/** An input of a join graph: a lowered relation with its single-input conditions applied */
case class JoinInput(op: Operator, rows: Double)

/**
  * A condition over several inputs, or over none (a constant), with the inputs it reads. For an
  * equality, `sides` holds the inputs each side reads
  */
case class JoinEdge(cond: Expression, refs: Set[Int], sides: Option[(Set[Int], Set[Int])]):

  /**
    * Whether this is an equality between the joined inputs and input i: one side reads only joined
    * inputs, the other only i
    */
  def isEquiTo(joined: collection.Set[Int], i: Int): Boolean = sides.exists { (l, r) =>
    (l.nonEmpty && l.subsetOf(joined) && r == Set(i)) ||
    (r.nonEmpty && r.subsetOf(joined) && l == Set(i))
  }

  /** The sides of this equality: the one over the joined inputs first */
  def sidesOf(joined: collection.Set[Int]): (Expression, Expression) =
    val eq = cond.asInstanceOf[Eq]
    if sides.exists(_._1.subsetOf(joined)) then
      (eq.left, eq.right)
    else
      (eq.right, eq.left)

end JoinEdge

/**
  * Inputs of a tree of inner joins and the conditions between them. The inputs each condition reads
  * are computed once, as the join order search checks them at every step
  */
case class JoinGraph(inputs: IndexedSeq[JoinInput], edges: List[JoinEdge])

object JoinGraph:

  /**
    * Build a join graph over lowered leaves, pushing single-input conditions to their inputs.
    *
    * @param conds
    *   each condition paired with the expression to read its inputs from (e.g., with uncorrelated
    *   scalar subqueries replaced by constants)
    * @param refs
    *   the inputs an expression reads
    * @param push
    *   applies conditions to an input
    * @param selectivity
    *   the estimated fraction of the rows of an input passing a condition
    */
  def apply(
      leaves: List[Operator],
      conds: List[(Expression, Expression)],
      refs: Expression => Set[Int],
      push: (Operator, List[Expression]) => Operator,
      selectivity: (Operator, Expression) => Double
  ): JoinGraph =
    val condRefs = conds.map((c, e) => (c, refs(e)))
    val inputs   = leaves
      .indices
      .map { i =>
        val local = condRefs.collect {
          case (c, r) if r == Set(i) =>
            c
        }
        val op =
          if local.isEmpty then
            leaves(i)
          else
            push(leaves(i), local)
        JoinInput(
          op,
          CardinalityEstimator.rows(op) * local.map(c => selectivity(leaves(i), c)).product
        )
      }
    val edges = conds
      .zip(condRefs)
      .collect {
        case ((c, e), (_, r)) if r.size != 1 =>
          val sides =
            e match
              case eq: Eq =>
                Some((refs(eq.left), refs(eq.right)))
              case _ =>
                None
          JoinEdge(c, r, sides)
      }
    JoinGraph(inputs, edges)

  end apply

end JoinGraph
