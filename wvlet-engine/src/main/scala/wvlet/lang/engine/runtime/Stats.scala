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

import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.expr.CastExpr
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.expr.InList
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.CardinalityEstimator
import wvlet.lang.engine.plan.JoinOrderOptimizer

/**
  * Row and distinct-count estimates of [[RuntimePlan]] nodes, read from source statistics. They
  * follow [[CardinalityEstimator]] over the lowered operators rule for rule, so that join order
  * reads the same estimates on both forms
  */
object Stats:
  import RuntimePlan.*

  /** Estimated rows of p: source row counts, passed through every node but joins */
  def rows(p: RuntimePlan): Double =
    p match
      case Scan(source, _, _, _) =>
        source.estimatedRows.map(_.toDouble).getOrElse(1e6)
      case Values(columns, rows) if columns.isEmpty && rows.size == 1 =>
        // Lowered to a SingleRowOp, which CardinalityEstimator reads as a leaf without statistics
        1e6
      case Values(_, _) =>
        1e3
      case Join(left, right, kind, lk, _, _, buildLeft) =>
        if lk.isEmpty then
          // A nested loop join
          rows(left)
        else
          // About the rows of the side every output row comes from. A join building on the left
          // probes with the right input
          val (probe, build, k) =
            if buildLeft then
              (right, left, JoinOrderOptimizer.swappedKind(kind))
            else
              (left, right, kind)
          k match
            case JoinKind.Right | JoinKind.RightSemi | JoinKind.RightAnti =>
              rows(build)
            case JoinKind.Full =>
              math.max(rows(probe), rows(build))
            case _ =>
              rows(probe)
      case AsOfJoin(left, _, _, _, _, _, _, _) =>
        // As CardinalityEstimator does for AsOfJoinOp: every output row comes from a left row
        rows(left)
      case InnerJoins(inputs, _) =>
        rows(inputs.head)
      case CTERef(_, _) =>
        // As for a scan of a shared result: its rows are unknown until it is computed
        1e6
      case Filter(child, _, _) =>
        rows(child)
      case Project(child, _, _) =>
        rows(child)
      case Sort(child, _, _) =>
        rows(child)
      case Limit(child, _, _) =>
        rows(child)
      case Aggregate(child, _, _, _) =>
        rows(child)
      case Union(inputs, _) =>
        // As CardinalityEstimator does over operators: the rows of the first input
        rows(inputs.head)
      case SetOp(left, _, _, _, _) =>
        rows(left)
      case Distinct(child) =>
        rows(child)
      case Sample(child, _) =>
        // As CardinalityEstimator does for the filter a sample lowers to
        rows(child)
      case Window(child, _, _) =>
        rows(child)
      case Unnest(child, _, _, _, _) =>
        // As CardinalityEstimator does for UnnestOp: the rows of its input
        rows(child)

  /** Distinct values of column `id` of p from source statistics, if traceable */
  def distinct(p: RuntimePlan, id: ColumnId): Option[Double] =
    p match
      case s @ Scan(source, columns, _, _) =>
        val i = columns.indexWhere(_.id == id)
        Option.when(i >= 0)(i).flatMap(i => source.estimatedDistinct(s.sourceColumnName(i)))
      case Filter(child, _, _) =>
        distinct(child, id)
      case Sample(child, _) =>
        distinct(child, id)
      case Project(child, exprs, columns) =>
        val i = columns.indexWhere(_.id == id)
        if i < 0 then
          None
        else
          exprs(i).expr match
            case c: ColRef =>
              distinct(child, ColumnId(c.index))
            case _ =>
              None
      case Join(left, right, _, lk, _, _, _) =>
        // A hash join; a nested loop join doesn't trace its columns
        if lk.isEmpty then
          None
        else if left.output.exists(_.id == id) then
          distinct(left, id)
        else
          distinct(right, id)
      case Values(_, _) | Sort(_, _, _) | Limit(_, _, _) | Aggregate(_, _, _, _) |
          InnerJoins(_, _) | CTERef(_, _) | Union(_, _) | SetOp(_, _, _, _, _) | Distinct(_) |
          Window(_, _, _) | Unnest(_, _, _, _, _) | AsOfJoin(_, _, _, _, _, _, _, _) =>
        // As CardinalityEstimator does over operators
        None

  /** Distinct values of an expression over p: a column, possibly cast */
  def distinct(p: RuntimePlan, e: VExpr): Option[Double] =
    e match
      case c: ColRef =>
        distinct(p, ColumnId(c.index))
      case c: CastExpr =>
        distinct(p, c.child)
      case _ =>
        None

  /**
    * Estimated fraction of the rows of p passing a condition: 1 / ndv for `x = literal` (n / ndv
    * for `x in (n literals)`), as in DuckDB, and [[CardinalityEstimator.DefaultSelectivity]]
    * otherwise
    */
  def selectivity(p: RuntimePlan, c: JoinCondition): Double =
    def compared(e: VExpr): Option[VExpr] =
      e match
        case Compare(CmpOp.Eq, Const(_, _), x) =>
          Some(x)
        case Compare(CmpOp.Eq, x, _) =>
          Some(x)
        case in: InList if !in.negate =>
          Some(in.child)
        case _ =>
          None
    c.literals
      .flatMap { n =>
        compared(c.cond.expr).flatMap(distinct(p, _)).filter(_ >= 1).map(d => math.min(1.0, n / d))
      }
      .getOrElse(CardinalityEstimator.DefaultSelectivity)

end Stats
