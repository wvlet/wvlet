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
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.vector.PType

/**
  * Read a top-N's pass-through string columns only for its N rows (`late_columns`). Under a sort
  * with a limit, string columns of a scan T that nothing reads on the way up (no filter, join key,
  * computed projection or sort key), only copies, are read for every row that reaches the sort,
  * then all but N are dropped. When T declares a unique key K that also reaches the sort, and inner
  * joins on the way up read each column of K (so none is NULL), the top-N is computed without those
  * columns, and a hash join of a second scan of T (K and the strings) with the N rows on K reads
  * them back: K is unique, so each of the N rows finds exactly the T row it came from. The N keys
  * filter that scan per row and skip its row groups that hold none of them (`key_set_row_groups`).
  * A sort over the N joined rows restores their order.
  *
  * TPC-H q10 sorts 381K customers by revenue for the top 20, and decoded their name, address, phone
  * and comment for each of them.
  *
  * It fires when at least [[MinRowsPerLimit]] times N rows are estimated to reach the sort, and at
  * least 1/[[MaxScanPerRow]] of T's rows, so the second scan of T's keys costs less than the
  * strings it saves.
  */
object LateColumns extends Pass:
  import RuntimePlan.*

  /** The estimated rows reaching the sort per row it keeps, at least */
  val MinRowsPerLimit: Double = 64.0

  /** T's rows per estimated row reaching the sort, at most */
  val MaxScanPerRow: Double = 16.0

  override def name: String = Optimization.LateColumns.name

  override def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    ctx.newColumnId match
      case Some(newId) if ctx.options.isEnabled(Optimization.LateColumns) =>
        walk(p, ctx, newId)
      case _ =>
        p

  private def walk(p: RuntimePlan, ctx: OptimizerContext, newId: () => ColumnId): RuntimePlan =
    val children = p.children
    val next     = children.map(walk(_, ctx, newId))
    val node     =
      if next.corresponds(children)(_ eq _) then
        p
      else
        p.withChildren(next)
    node match
      case s @ Sort(_, _, Some(n)) if n > 0 =>
        rewrite(s, n, ctx, newId) match
          case Some(r) =>
            ctx.fired(name)
            r
          case None =>
            node
      case _ =>
        node

  /** A scan column: the scan (compared by reference) and the column's position in it */
  private final class Origin(val scan: Scan, val pos: Int):
    override def equals(o: Any): Boolean =
      o match
        case x: Origin =>
          (x.scan eq scan) && x.pos == pos
        case _ =>
          false

    override def hashCode: Int = System.identityHashCode(scan) * 31 + pos

  /**
    * The scan columns that p's columns copy, the scan columns read by more than a copy on the way
    * up, and those read as inner join keys
    */
  private case class Trace(
      origins: Map[ColumnId, Origin],
      read: Set[Origin],
      joinKeys: Set[Origin]
  ):
    def reading(refs: Iterable[ColumnId]): Trace = copy(read = read ++ refs.flatMap(origins.get))

  private val untraced = Trace(Map.empty, Set.empty, Set.empty)

  private def trace(p: RuntimePlan): Trace =
    p match
      case s: Scan =>
        Trace(
          s.columns.indices.map(i => s.columns(i).id -> Origin(s, i)).toMap,
          Set.empty,
          Set.empty
        )
      case Filter(child, cond, _) =>
        trace(child).reading(cond.refs)
      case Project(child, exprs, columns) =>
        val t      = trace(child)
        val copies =
          columns
            .indices
            .flatMap { i =>
              exprs(i).expr match
                case c: ColRef =>
                  t.origins.get(ColumnId(c.index)).map(columns(i).id -> _)
                case _ =>
                  None
            }
            .toMap
        val computed = exprs.filterNot(_.expr.isInstanceOf[ColRef]).flatMap(_.refs)
        Trace(copies, t.read, t.joinKeys).reading(computed)
      case Join(left, right, JoinKind.Inner, lk, rk, residual, _) if lk.nonEmpty =>
        val l    = trace(left)
        val r    = trace(right)
        val all  = l.origins ++ r.origins
        val keys = (lk ++ rk).flatMap { k =>
          k.expr match
            case c: ColRef =>
              all.get(ColumnId(c.index))
            case _ =>
              None
        }
        Trace(all, l.read ++ r.read, l.joinKeys ++ r.joinKeys ++ keys).reading(
          (lk ++ rk ++ residual).flatMap(_.refs)
        )
      case _ =>
        untraced

  /** The top-N computed without one scan's pass-through strings, which are joined back after it */
  private def rewrite(
      s: Sort,
      n: Long,
      ctx: OptimizerContext,
      newId: () => ColumnId
  ): Option[RuntimePlan] =
    val options = ctx.options
    val out     = s.child.output
    val t       = trace(s.child).reading(s.keys.flatMap(_.expr.refs))
    // The sort's columns copying each scan's columns, by scan
    val byScan: Seq[(Scan, IndexedSeq[(Column, Origin)])] =
      out.flatMap(c => t.origins.get(c.id).map(c -> _)).groupBy(_._2.scan).toSeq
    val candidates = byScan.flatMap { (scan, cols) =>
      val names = scan.columns.indices.map(i => scan.sourceColumnName(i).toLowerCase)
      // A declared unique key whose columns all reach the sort and are read by inner joins
      val key: Option[IndexedSeq[(Column, Origin)]] = scan
        .source
        .uniqueKeys
        .iterator
        .flatMap { k =>
          val at = k.map(c => names.indexOf(c.toLowerCase))
          if k.isEmpty || at.exists(_ < 0) then
            None
          else
            val keyCols = at.map(i => cols.find(_._2.pos == i))
            Option.when(
              keyCols.forall(_.isDefined) && keyCols.forall(c => t.joinKeys.contains(c.get._2))
            )(keyCols.flatten.toIndexedSeq)
        }
        .nextOption()
      key.flatMap { keyCols =>
        val keyPos = keyCols.map(_._2.pos).toSet
        val late   = cols.filter((c, o) =>
          c.ptype == PType.Str && !keyPos.contains(o.pos) && !t.read.contains(o)
        )
        Option.when(late.nonEmpty)((scan, keyCols, late))
      }
    }
    if candidates.isEmpty then
      return None
    val rows = Stats.treeRows(s.child, options)
    if rows < MinRowsPerLimit * n then
      return None
    // The scan with the most late columns among those whose rows are few enough to scan again
    val fit = candidates.filter((scan, _, _) =>
      scan.source.estimatedRows.forall(_ <= rows * MaxScanPerRow)
    )
    fit
      .maxByOption(_._3.size)
      .map { (scan, keyCols, late) =>
        // A second scan of the key and the late columns, by their first copies at the sort
        val reread = (keyCols ++ late).distinctBy(_._2.pos)
        val fresh  = reread.map((c, o) => o.pos -> Column(newId(), c.name, c.ptype, c.qualifiers))
        val copyOf = fresh.toMap
        val again  = Scan(
          scan.source,
          fresh.map(_._2),
          Some(reread.map((_, o) => scan.sourcePosition(o.pos))),
          None
        )
        val joined = Join(
          again,
          s,
          JoinKind.Inner,
          keyCols.map((_, o) => BExpr.ref(copyOf(o.pos))),
          keyCols.map((c, _) => BExpr.ref(c)),
          None,
          buildLeft = false
        )
        val lateAt = late.map((c, o) => c.id -> copyOf(o.pos)).toMap
        Project(
          Sort(joined, s.keys, Some(n)),
          out.map(c => BExpr.ref(lateAt.getOrElse(c.id, c))),
          out
        )
      }

  end rewrite

end LateColumns
