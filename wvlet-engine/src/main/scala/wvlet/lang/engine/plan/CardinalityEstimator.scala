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

import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.model.expr.*

/**
  * Row and distinct-count estimates of physical operators, read from source statistics. These drive
  * the join order and the build side of hash joins
  */
object CardinalityEstimator:

  /** Estimated fraction of rows passing a filter condition whose selectivity is unknown */
  val DefaultSelectivity: Double = 0.3

  /** Estimated rows of op: source row counts, passed through every operator but joins */
  def rows(op: Operator): Double =
    op match
      case s: ScanOp if s.source.isInstanceOf[BreakerSource] =>
        // The output of an aggregation read as morsels: estimated as the aggregation itself
        rows(s.source.asInstanceOf[BreakerSource].op)
      case s: ScanOp =>
        s.source.estimatedRows.map(_.toDouble).getOrElse(1e6)
      case b: BatchesOp =>
        1e3
      case j: HashJoinOp =>
        // About the rows of the side every output row comes from
        j.kind match
          case JoinKind.Right | JoinKind.RightSemi | JoinKind.RightAnti =>
            rows(j.right)
          case JoinKind.Full =>
            math.max(rows(j.left), rows(j.right))
          case _ =>
            rows(j.left)
      case other =>
        other.children.headOption.map(rows).getOrElse(1e6)

  /** Distinct values of output column i of op from source statistics, if traceable */
  def columnDistinct(op: Operator, i: Int): Option[Double] =
    op match
      case s: ScanOp if s.source.isInstanceOf[BreakerSource] && s.columns.isEmpty =>
        columnDistinct(s.source.asInstanceOf[BreakerSource].op, i)
      case s: ScanOp =>
        s.source.estimatedDistinct(s.sourceColumnName(i))
      case f: FilterOp =>
        val j =
          if f.keep == null then
            i
          else
            f.keep(i)
        columnDistinct(f.child, j)
      case p: ProjectOp =>
        p.exprs(i) match
          case c: ColRef =>
            columnDistinct(p.child, c.index)
          case _ =>
            None
      case j: HashJoinOp if j.schema.size == j.left.schema.size + j.right.schema.size =>
        if i < j.left.schema.size then
          columnDistinct(j.left, i)
        else
          columnDistinct(j.right, i - j.left.schema.size)
      case _ =>
        None

  /** Distinct values of a key over op: a column, possibly cast */
  def keyDistinct(op: Operator, key: VExpr): Option[Double] =
    key match
      case c: ColRef =>
        columnDistinct(op, c.index)
      case c: CastExpr =>
        keyDistinct(op, c.child)
      case _ =>
        None

  /**
    * Estimated rows of an inner equi-join, |L| * |R| / prod(max(ndv(l), ndv(r))) over the key
    * pairs. A key of unknown distinct count on one side has at most the other side's distinct
    * values (containment); with neither known, the smaller side's key is assumed unique, i.e. a
    * foreign key to primary key join. The divisor is capped at max(|L|, |R|) since keys of one join
    * are usually correlated (e.g. ticket number and item of a sales line)
    */
  def joinOutputRows(
      left: Operator,
      leftRows: Double,
      leftKeys: IndexedSeq[VExpr],
      right: Operator,
      rightRows: Double,
      rightKeys: IndexedSeq[VExpr]
  ): Double = joinOutputRows(
    leftRows,
    leftKeys.map(keyDistinct(left, _)),
    rightRows,
    rightKeys.map(keyDistinct(right, _))
  )

  /**
    * [[joinOutputRows]] from the distinct counts of each key pair (None for unknown) and the rows
    * of each side
    */
  def joinOutputRows(
      leftRows: Double,
      leftDistinct: IndexedSeq[Option[Double]],
      rightRows: Double,
      rightDistinct: IndexedSeq[Option[Double]]
  ): Double =
    val divisors = leftDistinct
      .indices
      .map { k =>
        val l = leftDistinct(k).map(math.min(_, leftRows))
        val r = rightDistinct(k).map(math.min(_, rightRows))
        val d =
          (l, r) match
            case (Some(a), Some(b)) =>
              math.max(a, b)
            // Containment: the side of unknown distinct count has at most the other's values
            case (Some(a), None) =>
              a
            case (None, Some(b)) =>
              b
            // Neither known: the smaller side holds the unique (primary) key
            case (None, None) =>
              math.min(leftRows, rightRows)
        math.max(1.0, d)
      }
    val divisor = math.min(divisors.product, math.max(math.max(leftRows, rightRows), 1.0))
    math.max(1.0, leftRows * rightRows / divisor)

  /**
    * Estimated fraction of the rows of op passing a condition: 1 / ndv for column = constant (n /
    * ndv for IN lists of n constants), as in DuckDB, and [[DefaultSelectivity]] otherwise. `bind`
    * binds an expression over the schema of op
    */
  def selectivity(op: Operator, c: Expression, bind: Expression => VExpr): Double =
    def distinct(e: Expression): Option[Double] =
      try
        keyDistinct(op, bind(e)).filter(_ >= 1)
      catch
        case _: Exception =>
          None
    val s =
      c match
        // x = null is `x is null` in wvlet
        case Eq(a, l: Literal, _) if !l.isInstanceOf[NullLiteral] =>
          distinct(a).map(1 / _)
        case Eq(l: Literal, b, _) if !l.isInstanceOf[NullLiteral] =>
          distinct(b).map(1 / _)
        case In(a, list, _) if list.forall(isNonNullLiteral) =>
          distinct(a).map(n => math.min(1.0, list.size / n))
        case _ =>
          None
    s.getOrElse(DefaultSelectivity)

  /**
    * The number of literals a condition compares an expression to, when it is `x = literal` (1) or
    * `x in (n literals)` (n): the conditions whose selectivity reads the distinct count of x
    */
  def literalCount(c: Expression): Option[Int] =
    c match
      case Eq(_, l: Literal, _) if !l.isInstanceOf[NullLiteral] =>
        Some(1)
      case Eq(l: Literal, _, _) if !l.isInstanceOf[NullLiteral] =>
        Some(1)
      case In(_, list, _) if list.forall(isNonNullLiteral) =>
        Some(list.size)
      case _ =>
        None

  private def isNonNullLiteral(e: Expression): Boolean =
    e.isInstanceOf[Literal] && !e.isInstanceOf[NullLiteral]

end CardinalityEstimator
