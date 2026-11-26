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

  /**
    * Whether op aggregates or deduplicates rows, which [[rows]] estimates as their input's rows
    */
  def aggregates(op: Operator): Boolean =
    op match
      case _: HashAggOp | _: PartitionedAggOp | _: DistinctOp | _: SetOpOp =>
        true
      case s: ScanOp if s.source.isInstanceOf[BreakerSource] =>
        aggregates(s.source.asInstanceOf[BreakerSource].op)
      case other =>
        other.children.exists(aggregates)

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
  ): Double = joinOutputRows(leftRows, leftDistinct, rightRows, rightDistinct, Nil)

  /**
    * [[joinOutputRows]] over the key pairs of `leftDistinct` and `rightDistinct`, also divided by
    * `divisors`: those of the key groups estimated by [[compositeDivisor]], which the cap of
    * correlated keys doesn't cover
    */
  def joinOutputRows(
      leftRows: Double,
      leftDistinct: IndexedSeq[Option[Double]],
      rightRows: Double,
      rightDistinct: IndexedSeq[Option[Double]],
      divisors: Seq[Double]
  ): Double =
    val keyDivisors = leftDistinct
      .indices
      .map(k => keyDivisor(leftRows, leftDistinct(k), rightRows, rightDistinct(k)))
    val capped = math.min(keyDivisors.product, math.max(math.max(leftRows, rightRows), 1.0))
    math.max(1.0, leftRows * rightRows / (capped * divisors.product))

  /** The divisor of one key pair: the larger of its sides' distinct values, at most their rows */
  private def keyDivisor(
      leftRows: Double,
      leftDistinct: Option[Double],
      rightRows: Double,
      rightDistinct: Option[Double]
  ): Double =
    val l = leftDistinct.map(math.min(_, leftRows))
    val r = rightDistinct.map(math.min(_, rightRows))
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

  /**
    * The divisor of an equi-join on several key pairs, all reading one table on each side, from the
    * unique keys the tables declare (`key_join_estimates`). Keys covering a declared key of a side
    * of `rows` base rows join as a foreign key to that primary key: values from a domain of `rows`,
    * so |L| * |R| / rows as a single-column key would. Keys that are no declared key of either
    * table are independent columns: the product of the pairs' divisors, without the cap that
    * [[joinOutputRows]] gives correlated keys. None when a side's keys are unknown
    */
  def compositeDivisor(
      leftRows: Double,
      leftDistinct: IndexedSeq[Option[Double]],
      rightRows: Double,
      rightDistinct: IndexedSeq[Option[Double]],
      left: KeyCoverage,
      right: KeyCoverage
  ): Option[Double] =
    def domain(rows: Double): Double = keyDivisor(leftRows, Some(rows), rightRows, Some(rows))
    (left, right) match
      case (KeyCoverage.Unique(a), KeyCoverage.Unique(b)) =>
        Some(domain(math.max(a, b)))
      case (KeyCoverage.Unique(a), _) =>
        Some(domain(a))
      case (_, KeyCoverage.Unique(b)) =>
        Some(domain(b))
      case (KeyCoverage.NotUnique, KeyCoverage.NotUnique) =>
        Some(
          leftDistinct
            .indices
            .map(k => keyDivisor(leftRows, leftDistinct(k), rightRows, rightDistinct(k)))
            .product
        )
      case _ =>
        None

  /**
    * The number of literals a condition compares an expression to, when it is `x = literal` (1) or
    * `x in (n literals)` (n): the conditions whose selectivity reads the distinct count of x. With
    * `constants` (`constant_list_selectivity`), a constant expression of literals (`1999 + 1`)
    * counts as a literal too
    */
  def literalCount(c: Expression, constants: Boolean = false): Option[Int] =
    def isValue(e: Expression): Boolean = isNonNullLiteral(e) || constants && isConstant(e)
    c match
      case Eq(_, l, _) if isValue(l) =>
        Some(1)
      case Eq(l, _, _) if isValue(l) =>
        Some(1)
      case In(_, list, _) if list.forall(isValue) =>
        Some(list.size)
      case _ =>
        None

  private def isNonNullLiteral(e: Expression): Boolean =
    e.isInstanceOf[Literal] && !e.isInstanceOf[NullLiteral]

  /** Whether e is arithmetic, casts and parentheses over non-NULL literals only */
  private def isConstant(e: Expression): Boolean =
    e match
      case a: ArithmeticBinaryExpr =>
        isConstant(a.left) && isConstant(a.right)
      case u: ArithmeticUnaryExpr =>
        isConstant(u.child)
      case c: Cast =>
        isConstant(c.expr)
      case p: ParenthesizedExpression =>
        isConstant(p.child)
      case other =>
        isNonNullLiteral(other)

end CardinalityEstimator

/** What the declared unique keys of a table say about the columns one side of a join reads */
enum KeyCoverage:
  /** Nothing known: the columns don't all read one table, or the table declares no keys */
  case Unknown

  /** The columns read one table that declares unique keys, and cover none of them */
  case NotUnique

  /** The columns cover a declared unique key of a table of `rows` rows */
  case Unique(rows: Double)
