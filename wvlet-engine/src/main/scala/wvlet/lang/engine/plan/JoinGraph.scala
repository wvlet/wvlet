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

/**
  * A condition over several inputs of a join graph, or over none (a constant), with the inputs it
  * reads. An equality has its two sides with the inputs each side reads
  */
case class JoinEdge[C](cond: C, refs: Set[Int], sides: Option[JoinEdge.Sides[C]]):

  /**
    * Whether this is an equality between the joined inputs and input i: one side reads only joined
    * inputs, the other only i
    */
  def isEquiTo(joined: collection.Set[Int], i: Int): Boolean = sides.exists { s =>
    (s.leftRefs.nonEmpty && s.leftRefs.subsetOf(joined) && s.rightRefs == Set(i)) ||
    (s.rightRefs.nonEmpty && s.rightRefs.subsetOf(joined) && s.leftRefs == Set(i))
  }

  /** The sides of this equality: the one over the joined inputs first */
  def sidesOf(joined: collection.Set[Int]): (C, C) =
    val s = sides.get
    if s.leftRefs.subsetOf(joined) then
      (s.left, s.right)
    else
      (s.right, s.left)

end JoinEdge

object JoinEdge:
  /** The sides of an equality, with the inputs each one reads */
  case class Sides[C](left: C, leftRefs: Set[Int], right: C, rightRefs: Set[Int])

/**
  * Inputs of a tree of inner joins, with their estimated rows after their single-input conditions,
  * and the conditions between them. The inputs each condition reads are computed once, as the join
  * order search checks them at every step. `P` is the plan form of the inputs, and `C` the form of
  * the conditions
  */
case class JoinGraph[P, C](
    inputs: IndexedSeq[P],
    rows: IndexedSeq[Double],
    edges: List[JoinEdge[C]]
)

object JoinGraph:

  /**
    * Build a join graph over leaves whose operators (`op`) give the estimates, pushing single-input
    * conditions to their inputs. The graph's inputs are the leaves with their conditions pushed, in
    * input order.
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
  def build[P](
      leaves: List[P],
      op: P => Operator,
      conds: List[(Expression, Expression)],
      refs: Expression => Set[Int],
      push: (P, List[Expression]) => P,
      selectivity: (Operator, Expression) => Double
  ): JoinGraph[P, Expression] =
    val condRefs = conds.map((c, e) => (c, refs(e)))
    // Push and estimate one input at a time, as the conditions bind (and may plan subqueries)
    val pushed = leaves
      .indices
      .map { i =>
        val local = condRefs.collect {
          case (c, r) if r == Set(i) =>
            c
        }
        val p =
          if local.isEmpty then
            leaves(i)
          else
            push(leaves(i), local)
        val leaf = op(leaves(i))
        (p, CardinalityEstimator.rows(op(p)) * local.map(selectivity(leaf, _)).product)
      }
    val edges = conds
      .zip(condRefs)
      .collect {
        case ((c, e), (_, r)) if r.size != 1 =>
          val sides =
            (c, e) match
              case (ceq: Eq, eq: Eq) =>
                Some(JoinEdge.Sides(ceq.left, refs(eq.left), ceq.right, refs(eq.right)))
              case _ =>
                None
          JoinEdge(c, r, sides)
      }
    JoinGraph(pushed.map(_._1), pushed.map(_._2), edges)

  end build

end JoinGraph
