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
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.JoinOrderOptimizer

/**
  * Group-count estimates of rows that come in clusters sharing their keys
  * (`clustered_group_estimates`): the lines of a TPC-DS order share its customer and sold date, so
  * a filter on the date keeps or drops an order's lines together, and the (customer, year) groups
  * of the kept rows number far fewer than the product of the keys' distinct values or the rows.
  * TPC-DS q74's web leg has 2.87M rows in 1M (customer, year) combinations at SF10, but 212,810
  * groups.
  *
  * The cluster size is the mean run length of equal consecutive values of the source columns
  * ([[wvlet.lang.engine.catalog.TableSource.keyRunLength]], sampled in storage order), at most that
  * of every column that selects the rows on the way, so that a filter cutting the clusters apart
  * (rows ordered by the group key, filtered by another column) leaves the estimate at the rows. R
  * rows in clusters of r rows over D key combinations are estimated as D (1 - e^(-(R / r) / D))
  * groups: the expected distinct combinations of R / r clusters drawn uniformly (Cardenas' formula)
  */
object ClusteredGroups:
  import RuntimePlan.*

  /** The run length from which rows count as clustered; shorter runs keep min(D, R) */
  val MinRun: Double = 2.0

  /**
    * Estimated groups of `rows` rows of p grouped by `keys`, which take `combinations` distinct
    * value combinations: clustered as above when the keys' run is at least [[MinRun]], and
    * min(combinations, rows) otherwise
    */
  def groups(p: RuntimePlan, keys: Seq[VExpr], combinations: Double, rows: Double): Double =
    val bound = math.min(combinations, rows)
    run(p, keys) match
      // An overflowed product of distinct values would make the formula NaN
      case Some(r) if r >= MinRun && rows > 0 && !combinations.isInfinite =>
        val d = math.max(1.0, combinations)
        math.max(1.0, math.min(bound, d * -math.expm1(-(rows / r) / d)))
      case _ =>
        bound

  /**
    * The run length of the tuple of `keys` in the rows of p: the shortest run of a key or of a
    * column selecting the rows on the way. None when one is unknown
    */
  def run(p: RuntimePlan, keys: Seq[VExpr]): Option[Double] =
    val runs = keys.map(exprRun(p, _)) ++ selectionRuns(p)
    Option.when(keys.nonEmpty && runs.forall(_.isDefined))(runs.flatten.min)

  private def exprRun(p: RuntimePlan, e: VExpr): Option[Double] =
    e match
      case c: ColRef =>
        columnRun(p, ColumnId(c.index))
      case c: CastExpr =>
        exprRun(p, c.child)
      case _ =>
        None

  /**
    * The probe and build inputs of a hash join whose output rows each come from one probe row (an
    * inner, left, semi or anti join, possibly building on its left), with their keys
    */
  private def probeAndBuild(
      j: Join
  ): Option[(RuntimePlan, IndexedSeq[BExpr], RuntimePlan, IndexedSeq[BExpr], JoinKind)] =
    if j.leftKeys.isEmpty then
      None
    else
      val (probe, pk, build, bk, kind) =
        if j.buildLeft then
          (j.right, j.rightKeys, j.left, j.leftKeys, JoinOrderOptimizer.swappedKind(j.kind))
        else
          (j.left, j.leftKeys, j.right, j.rightKeys, j.kind)
      kind match
        case JoinKind.Inner | JoinKind.Left | JoinKind.Semi | JoinKind.Anti =>
          Some((probe, pk, build, bk, kind))
        case _ =>
          None

  /**
    * The run length of column `id` in the rows of p: a scanned column's sampled run, passed through
    * filters and plain projections, and through joins from the probe side. A build-side column of a
    * join on a unique build key (a dimension) is a function of the probe keys, so it runs at least
    * as long as they do
    */
  private def columnRun(p: RuntimePlan, id: ColumnId): Option[Double] =
    p match
      case s @ Scan(source, columns, _, _) =>
        val i = columns.indexWhere(_.id == id)
        Option.when(i >= 0)(i).flatMap(i => source.keyRunLength(s.sourceColumnName(i)))
      case Filter(child, _, _) =>
        columnRun(child, id)
      case Project(child, exprs, columns) =>
        val i = columns.indexWhere(_.id == id)
        Option.when(i >= 0)(i).flatMap(i => exprRun(child, exprs(i).expr))
      case j: Join =>
        probeAndBuild(j).flatMap { (probe, pk, build, bk, _) =>
          if probe.output.exists(_.id == id) then
            columnRun(probe, id)
          else if build.output.exists(_.id == id) && uniqueKey(build, bk) then
            val runs = pk.map(k => exprRun(probe, k.expr))
            Option.when(runs.forall(_.isDefined))(runs.flatten.min)
          else
            None
        }
      case _ =>
        None

  /** Whether a single key has a distinct value per row of p (a dimension's primary key) */
  private def uniqueKey(p: RuntimePlan, keys: IndexedSeq[BExpr]): Boolean =
    keys.size == 1 && Stats.distinct(p, keys.head.expr).exists(_ >= 0.9 * Stats.rows(p))

  /**
    * The run lengths of the columns that select the rows of p on its probe path: filter columns,
    * the probe keys of inner, semi and anti joins, and join residuals. A sample keeps rows one by
    * one (run 1)
    */
  private def selectionRuns(p: RuntimePlan): Seq[Option[Double]] =
    p match
      case Filter(child, cond, _) =>
        cond.refs.toSeq.map(columnRun(child, _)) ++ selectionRuns(child)
      case Project(child, _, _) =>
        selectionRuns(child)
      case Sample(_, _) =>
        Seq(Some(1.0))
      case j: Join =>
        probeAndBuild(j) match
          case Some((probe, pk, _, _, kind)) =>
            val keys =
              if kind == JoinKind.Left then
                Seq.empty
              else
                pk.map(k => exprRun(probe, k.expr))
            keys ++ j.residual.toSeq.flatMap(_.refs).map(columnRun(j, _)) ++ selectionRuns(probe)
          case None =>
            Seq(None)
      case _ =>
        Seq.empty

end ClusteredGroups
