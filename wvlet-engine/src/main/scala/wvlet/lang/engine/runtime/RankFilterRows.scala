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

import wvlet.lang.engine.exec.WindowOp
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.VExpr

/**
  * Row estimates of a plan that keeps the top rows of each window partition (`rank_filter_rows`): a
  * filter bounding a row_number() or rank() by a constant (`rnk < 11`, `rnk <= 10`, `rnk = 1`)
  * keeps at most that many rows per partition, and only N rows overall without PARTITION BY (TPC-DS
  * q44's two top-10 rankings, which [[Stats.rows]] estimates as the store_sales rows below them).
  * rank() can keep more than N rows on ties at the bound; dense_rank() is not bounded this way, as
  * its ties are unbounded
  */
object RankFilterRows:
  import RuntimePlan.*

  private val rankingFunctions = Set("row_number", "rank")

  /**
    * At most how many rows p has, when p is a rank-bounded window under projections, filters, sorts
    * and limits. None when no such bound is found or the window's partitions are not estimable
    */
  def rows(p: RuntimePlan): Option[Double] =
    p match
      case Filter(w: Window, cond, _) =>
        windowBound(w, cond.expr).orElse(rows(w))
      case Filter(child, _, _) =>
        rows(child)
      case Project(child, _, _) =>
        rows(child)
      case Sort(child, _, limit) =>
        rows(child).map(r => limit.map(l => math.min(l.toDouble, r)).getOrElse(r))
      case Limit(child, limit, _) =>
        rows(child).map(math.min(limit.toDouble, _))
      case _ =>
        None

  /** The rows a filter `cond` directly over window w keeps, if it bounds one ranking call */
  private def windowBound(w: Window, cond: VExpr): Option[Double] =
    val ranked: Map[Int, WindowFunction] =
      w.calls
        .indices
        .collect {
          case i if rankingFunctions.contains(w.calls(i).name) && w.calls(i).orderBy.nonEmpty =>
            w.columns(i).id.value -> w.calls(i)
        }
        .toMap
    def conjuncts(e: VExpr): List[VExpr] =
      e match
        case AndExpr(l, r) =>
          conjuncts(l) ++ conjuncts(r)
        case other =>
          List(other)
    if ranked.isEmpty then
      None
    else
      // Split every `and`, so that `rnk between 1 and 10` bounds the ranking as WindowOp.topNOf
      // reads it
      conjuncts(cond)
        .flatMap(WindowOp.rankBound(_, ranked.contains))
        .flatMap { (id, n) =>
          partitions(w.child, ranked(id)).map(_ * math.max(0L, n).toDouble)
        }
        .minOption
        .map(math.max(1.0, _))

  /** Estimated partitions of a window call: one without PARTITION BY */
  private def partitions(child: RuntimePlan, call: WindowFunction): Option[Double] =
    if call.partitionBy.isEmpty then
      Some(1.0)
    else
      Stats.groups(child, call.partitionBy.map(_.expr))

end RankFilterRows
