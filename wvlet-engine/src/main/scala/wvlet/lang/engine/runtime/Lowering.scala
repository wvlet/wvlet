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

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.plan.JoinOrderOptimizer
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.plan.Planner
import wvlet.lang.engine.plan.ScanFilters
import wvlet.lang.engine.vector.PType
import wvlet.uni.cloud.parquet.ParquetFilter

import scala.collection.mutable

/**
  * Turns a [[RuntimePlan]] into physical operators: column ids become input positions. The physical
  * passes over operators (column pruning, parallelism) run afterwards, as for the recursive
  * planner.
  */
object Lowering:
  import RuntimePlan.*

  /**
    * Lower `p`. With `joinFilters`, hash joins whose unmatched probe rows are dropped give the
    * Parquet scans on their probe side the range of their build keys ([[RuntimeFilter]])
    */
  def apply(p: RuntimePlan, joinFilters: Boolean = false, presizeAggs: Boolean = false): Operator =
    Lower(joinFilters, presizeAggs).lower(p)

  /**
    * The group table size of an aggregation of p by `keys`, from [[Stats.groups]]: 0 when unknown
    */
  def expectedGroups(p: RuntimePlan, keys: Seq[BExpr]): Int =
    if keys.isEmpty then
      0
    else
      Stats
        .groups(p, keys.map(_.expr))
        .map(g => math.min(g, GroupTable.MaxPresizeGroups.toDouble).toInt)
        .getOrElse(0)

  private class Lower(joinFilters: Boolean, presizeAggs: Boolean):
    // Filters for the scans not lowered yet, by the id of the column they filter
    private val pending = mutable.Map.empty[Int, List[RuntimeFilter]]
    // The nodes not lowered yet that are in a duplicate-insensitive context, set by their parent
    // ([[ReduceSemiJoins.childContexts]]), so that shared CTE readers report it
    private val setNodes = java.util.IdentityHashMap[RuntimePlan, Boolean]()

    def lower(p: RuntimePlan): Operator =
      val inSet = setNodes.getOrDefault(p, false)
      val kids  = p.children
      if kids.nonEmpty then
        val contexts = ReduceSemiJoins.childContexts(p, inSet)
        kids
          .indices
          .foreach { i =>
            // A node read twice is in such a context only if both reads are
            val c = contexts(i) && setNodes.getOrDefault(kids(i), true)
            setNodes.put(kids(i), c)
          }
      lowerNode(p, inSet)

    private def lowerNode(p: RuntimePlan, inSet: Boolean): Operator =
      p match
        case Scan(source, _, read, filter) =>
          ScanOp(source, p.schema, read, filter, scanFilters(p.output))
        case Values(columns, rows) if columns.isEmpty && rows.size == 1 =>
          // One row without columns (a query without FROM), as the planner plans it
          SingleRowOp()
        case Values(_, rows) =>
          BatchesOp(p.schema, Seq(rows), "Values")
        case Filter(ref @ CTERef(cte, columns), cond, counts) =>
          // The reader reports its condition over the body's columns, which share positions
          val e      = cte.overBody(columns, cond.expr)
          val filter = JoinCondition(BExpr(e), PushDownFilters.remapCounts(cond.expr, e, counts))
          FilterOp(
            cte.reader(ref.schema, Some(filter), setNodes.getOrDefault(ref, false)),
            cond.lower(positions(ref))
          )
        case Filter(child, cond, _) =>
          FilterOp(lower(child), cond.lower(positions(child)))
        case Project(child, exprs, _) =>
          val pos = positions(child)
          ProjectOp(lower(child), exprs.map(_.lower(pos)), p.schema)
        case Sort(child, keys, limit) =>
          val pos = positions(child)
          SortOp(lower(child), keys.map(_.lower(pos)), limit)
        case Limit(child, limit, offset) =>
          LimitOp(lower(child), limit, offset)
        case j @ Join(left, right, kind, lk, rk, residual, buildLeft) =>
          val lpos = positions(left)
          val rpos = positions(right)
          // Before lowering the probe side, whose scans take the filters
          val filters =
            if joinFilters then
              runtimeFilters(j)
            else
              Nil
          JoinOrderOptimizer.joinOp(
            lower(left),
            lower(right),
            kind,
            lk.map(_.lower(lpos)),
            rk.map(_.lower(rpos)),
            residual.map(_.lower(positions(j.inputs))),
            buildLeft,
            p.schema,
            filters
          )
        case AsOfJoin(left, right, kind, lk, rk, lv, rv, op) =>
          val lpos = positions(left)
          val rpos = positions(right)
          AsOfJoinOp(
            lower(left),
            lower(right),
            lk.map(_.lower(lpos)),
            rk.map(_.lower(rpos)),
            lv.lower(lpos),
            rv.lower(rpos),
            op,
            kind,
            p.schema
          )
        case Aggregate(child, keys, calls, _) =>
          val pos = positions(child)
          HashAggOp(
            lower(child),
            keys.map(_.lower(pos)),
            calls.map(_.lower(pos)),
            p.schema,
            if presizeAggs then
              expectedGroups(child, keys)
            else
              0
          )
        case _: InnerJoins =>
          throw StatusCode
            .UNEXPECTED_STATE
            .newException(
              s"InnerJoins must be ordered by ReorderJoins before lowering:\n${p.explain}"
            )
        case Union(inputs, _) =>
          UnionAllOp(inputs.map(lower), p.schema)
        case SetOp(left, right, intersect, all, _) =>
          SetOpOp(lower(left), lower(right), intersect, all, p.schema)
        case Distinct(child) =>
          DistinctOp(lower(child))
        case Sample(child, percentage) =>
          Planner.sampleFilter(lower(child), percentage)
        case Window(child, calls, _) =>
          val pos = positions(child)
          WindowOp(lower(child), calls.map(_.lower(pos)), p.schema)
        case Unnest(child, lists, keepInput, outer, _) =>
          val pos = positions(child)
          UnnestOp(lower(child), lists.map(_.lower(pos)), keepInput, p.schema, outer)
        case CTERef(cte, _) =>
          cte.reader(p.schema, None, inSet)

    private def scanFilters(columns: IndexedSeq[Column]): Seq[RuntimeFilter] =
      if pending.isEmpty then
        Nil
      else
        columns.flatMap(c => pending.remove(c.id.value).getOrElse(Nil))

    /**
      * A filter for each key of `j` whose probe key is a column read from a Parquet scan, given to
      * that scan once lowered. Only for joins that drop unmatched probe rows: inner, semi, and
      * right joins, and left / anti joins building on the left (a right join / right anti join over
      * swapped inputs)
      */
    private def runtimeFilters(j: Join): Seq[RuntimeFilter] =
      val probeIsLeft: Option[Boolean] =
        (j.kind, j.buildLeft) match
          case (JoinKind.Inner | JoinKind.Semi, buildLeft) =>
            Some(!buildLeft)
          case (JoinKind.Right, false) =>
            Some(true)
          case (JoinKind.Left | JoinKind.Anti, true) =>
            Some(false)
          case _ =>
            None
      probeIsLeft
        .toSeq
        .flatMap { left =>
          val (probe, probeKeys, buildKeys) =
            if left then
              (j.left, j.leftKeys, j.rightKeys)
            else
              (j.right, j.rightKeys, j.leftKeys)
          probeKeys
            .indices
            .flatMap { k =>
              probeKeys(k).expr match
                case c: ColRef if c.ptype == buildKeys(k).ptype && rangeType(c.ptype) =>
                  scanColumns(probe, c.index).map { (id, column) =>
                    val f = RuntimeFilter(k, column, rangeFilter(column, c.ptype))
                    pending(id) = f :: pending.getOrElse(id, Nil)
                    f
                  }
                case _ =>
                  Nil
            }
        }

    end runtimeFilters

  end Lower

  /**
    * Whether a range of build keys of type `t` translates to a row-group filter. Join keys are cast
    * to a common type, so the build key has the probe column's type when the probe key is a column
    */
  private def rangeType(t: PType): Boolean =
    t match
      case PType.Int32 | PType.Int64 | PType.Date | PType.Timestamp | PType.TimestampTz =>
        true
      case _ =>
        false

  /** The scan filter for a [min, max] range of build key values over a column of type t */
  private def rangeFilter(column: String, t: PType): (Long, Long) => Option[ParquetFilter] =
    t match
      case PType.Int32 | PType.Date =>
        (lo, hi) => ScanFilters.range(column, t, lo.toInt, hi.toInt)
      case _ =>
        (lo, hi) => ScanFilters.range(column, t, lo, hi)

  /**
    * The Parquet columns of the scans that column `id` of `p` reads, with the id of each scan's
    * column, when each row of `p` holds the value of the scan row it comes from, so that dropping
    * scan rows by that column only drops rows of `p` with those values: through filters, plain
    * projections, the inputs of UNION ALL, and the probe sides of joins that preserve their rows
    */
  private def scanColumns(p: RuntimePlan, id: Int): Seq[(Int, String)] =
    p match
      case s @ Scan(source, columns, _, _) if source.supportsFilter =>
        columns.indexWhere(_.id.value == id) match
          case -1 =>
            Nil
          case i =>
            Seq(s.sourceColumnName(i)).filter(source.canFilterOn).map(id -> _)
      case Filter(child, _, _) =>
        scanColumns(child, id)
      case Project(child, exprs, columns) =>
        columns.indexWhere(_.id.value == id) match
          case -1 =>
            Nil
          case i =>
            exprs(i).expr match
              case c: ColRef =>
                scanColumns(child, c.index)
              case _ =>
                Nil
      // Each output row is a row of one input. An input column cast to a wider type is skipped
      case Union(inputs, columns) =>
        columns.indexWhere(_.id.value == id) match
          case -1 =>
            Nil
          case i =>
            inputs.flatMap { in =>
              val c = in.output(i)
              if c.ptype == columns(i).ptype then
                scanColumns(in, c.id.value)
              else
                Nil
            }
      // Only into the probe side: a build side is read while building, which can come before the
      // build of the join above sets the filter (morsel pipelines prepare inner builds first)
      case Join(left, right, kind, _, _, _, buildLeft) =>
        if left.output.exists(_.id.value == id) then
          kind match
            case JoinKind.Inner | JoinKind.Semi | JoinKind.Anti | JoinKind.Left if !buildLeft =>
              scanColumns(left, id)
            case _ =>
              Nil
        else
          kind match
            case JoinKind.Inner | JoinKind.Right if buildLeft =>
              scanColumns(right, id)
            case _ =>
              Nil
      case _ =>
        Nil

  /** Column id -> position in the output of `p` (-1 for ids it doesn't output) */
  private def positions(p: RuntimePlan): Array[Int] = positions(p.output)

  /** Column id -> position in `out` (-1 for ids it doesn't have) */
  private def positions(out: IndexedSeq[Column]): Array[Int] =
    val pos = Array.fill(out.map(_.id.value).maxOption.getOrElse(-1) + 1)(-1)
    out.indices.foreach(i => pos(out(i).id.value) = i)
    pos

end Lowering
