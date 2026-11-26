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

import scala.collection.mutable

/**
  * Group by the keys that determine the others (`dependent_group_keys`). When the group keys
  * include a declared unique key of a table ([[wvlet.lang.engine.catalog.TableSource.uniqueKeys]]),
  * the table's other columns, and expressions over them only, are functions of that key: rows of
  * one group share them. Such a key is grouped no more, and is carried as an `any_value` of its
  * group, so that the hash table compares fewer and narrower keys.
  *
  * A table's columns are known from the keys read as plain columns, from the equal columns of inner
  * hash joins (TPC-H q10's `c_nationkey = n_nationkey` makes nation's key known from customer's),
  * and from every column of a table once a declared key of it is known. Columns are traced through
  * filters, samples, deterministic projections, both inputs of inner hash joins, and the preserved
  * input of outer, semi and anti hash joins: a NULL-padded row of a table would break its key's
  * uniqueness, so the padded input of an outer join is not traced. Declared keys hold NULLs as
  * values (no two rows share them), so a group of NULL keys is still one table row.
  *
  * TPC-H q10 groups by customer's seven columns and nation's name, all determined by `c_custkey`:
  * it now groups by that one long key. The aggregation's columns keep their ids, and a projection
  * restores their order.
  */
object DependentGroupKeys extends Pass:
  import RuntimePlan.*

  override def name: String = Optimization.DependentGroupKeys.name

  override def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    if !ctx.options.isEnabled(Optimization.DependentGroupKeys) then
      p
    else
      walk(p, ctx)

  private def walk(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    val children = p.children
    val next     = children.map(walk(_, ctx))
    val node     =
      if next.corresponds(children)(_ eq _) then
        p
      else
        p.withChildren(next)
    node match
      case a: Aggregate if a.keys.size > 1 =>
        reduce(a) match
          case Some(r) =>
            ctx.fired(name)
            r
          case None =>
            node
      case _ =>
        node

  /** A source column: the scan (by its number in [[Lineage.scans]]) and the source position */
  private type Src = (Int, Int)

  /**
    * The source columns a column is a function of, and the one it is a copy of, if it is a scan
    * column passed through unchanged: only copies make a column known or equal to another, as
    * `c_custkey % 10` is a function of `c_custkey` without determining it
    */
  private case class Traced(srcs: Set[Src], copy: Option[Src])

  /**
    * The source columns of the traced output columns of a plan, the scans they read, and the equal
    * source columns of its inner hash joins
    */
  private class Lineage:
    val scans      = mutable.ArrayBuffer.empty[Scan]
    val equalities = mutable.ArrayBuffer.empty[(Src, Src)]

    /** The traced columns of each node traced so far, by identity */
    val nodes = java.util.IdentityHashMap[RuntimePlan, Map[ColumnId, Traced]]()

    /** The source columns each traced output column of p reads */
    def trace(p: RuntimePlan): Map[ColumnId, Traced] =
      val t = traceNode(p)
      nodes.put(p, t)
      t

    private def traceNode(p: RuntimePlan): Map[ColumnId, Traced] =
      p match
        case s: Scan =>
          val no = scans.size
          scans += s
          s.columns
            .indices
            .map { i =>
              val src = (no, s.sourcePosition(i))
              s.columns(i).id -> Traced(Set(src), Some(src))
            }
            .toMap
        case Filter(child, _, _) =>
          trace(child)
        case Sample(child, _) =>
          trace(child)
        case Project(child, exprs, columns) =>
          val in = trace(child)
          columns
            .indices
            .flatMap { i =>
              val e    = exprs(i)
              val refs = e.refs
              Option.when(
                refs.nonEmpty && refs.forall(in.contains) && PushDownFilters.isDeterministic(e.expr)
              )(
                columns(i).id -> (
                  e.expr match
                    case c: ColRef =>
                      in(ColumnId(c.index))
                    case _ =>
                      Traced(refs.flatMap(in(_).srcs), None)
                )
              )
            }
            .toMap
        case Join(left, right, kind, lk, rk, _, _) if lk.nonEmpty =>
          kind match
            case JoinKind.Inner =>
              val l = trace(left)
              val r = trace(right)
              lk.indices
                .foreach { i =>
                  (lk(i).expr, rk(i).expr) match
                    case (a: ColRef, b: ColRef) =>
                      (
                        l.get(ColumnId(a.index)).flatMap(_.copy),
                        r.get(ColumnId(b.index)).flatMap(_.copy)
                      ) match
                        case (Some(sa), Some(sb)) =>
                          equalities += ((sa, sb))
                        case _ =>
                    case _ =>
                }
              l ++ r
            case JoinKind.Left | JoinKind.Semi | JoinKind.Anti =>
              trace(left)
            case JoinKind.Right =>
              trace(right)
            case _ =>
              Map.empty
        case _ =>
          Map.empty

    /** The declared unique keys of scan `no`, as source positions */
    private def declaredKeys(no: Int): Seq[Set[Int]] =
      val source = scans(no).source
      val names  = source.schema.fields.map(_.name.toLowerCase)
      source
        .uniqueKeys
        .flatMap { k =>
          val pos = k.map(c => names.indexOf(c.toLowerCase))
          Option.when(k.nonEmpty && pos.forall(_ >= 0))(pos.toSet)
        }

    private lazy val keysOf: IndexedSeq[Seq[Set[Int]]] = scans.indices.map(declaredKeys)

    /**
      * Whether the source columns `cols` are determined by the source columns `known`: closed under
      * the equalities, and under every column of a scan once one of its declared keys is
      */
    def determines(known: Set[Src], cols: Set[Src]): Boolean =
      val k                        = mutable.Set.from(known)
      val determined               = mutable.Set.empty[Int]
      def covered(c: Src): Boolean = k.contains(c) || determined.contains(c._1)
      var changed                  = true
      while changed && !cols.forall(covered) do
        changed = false
        equalities.foreach { (a, b) =>
          if covered(a) && !covered(b) then
            k += b
            changed = true
          else if covered(b) && !covered(a) then
            k += a
            changed = true
        }
        scans
          .indices
          .foreach { no =>
            if !determined.contains(no) && keysOf(no).exists(_.forall(pos => covered((no, pos))))
            then
              determined += no
              changed = true
          }
      cols.forall(covered)

  end Lineage

  /**
    * Whether the keys of `a` determine the columns `cols` of `x`, a node of a's child, through the
    * declared keys and inner join equalities of a's child
    */
  private[runtime] def determines(a: Aggregate, x: RuntimePlan, cols: Seq[ColumnId]): Boolean =
    val lineage = Lineage()
    val traced  = lineage.trace(a.child)
    Option(lineage.nodes.get(x)).exists { xt =>
      cols.forall(xt.contains) && {
        val known =
          a.keys
            .flatMap(k =>
              k.expr match
                case c: ColRef =>
                  traced.get(ColumnId(c.index)).flatMap(_.copy)
                case _ =>
                  None
            )
            .toSet
        lineage.determines(known, cols.flatMap(xt(_).srcs).toSet)
      }
    }

  /**
    * Whether no two rows of p share their values of the columns `cols`: p is a scan whose columns
    * `cols` copy a declared key of it, under filters, samples and plain-copy projections, or an
    * inner hash join whose one side is unique on `cols` and whose other side is unique on its join
    * keys (each row finds at most one match)
    */
  private[runtime] def uniqueOn(p: RuntimePlan, cols: Seq[ColumnId]): Boolean =
    p match
      case s: Scan =>
        val names = s.source.schema.fields.map(_.name.toLowerCase)
        val pos   =
          cols
            .flatMap { c =>
              val i = s.columns.indexWhere(_.id == c)
              Option.when(i >= 0)(s.sourcePosition(i))
            }
            .toSet
        s.source
          .uniqueKeys
          .exists { k =>
            val kp = k.map(c => names.indexOf(c.toLowerCase))
            k.nonEmpty && kp.forall(_ >= 0) && kp.toSet.subsetOf(pos)
          }
      case Filter(child, _, _) =>
        uniqueOn(child, cols)
      case Sample(child, _) =>
        uniqueOn(child, cols)
      case Project(child, exprs, columns) =>
        val copies = cols.flatMap { c =>
          val i = columns.indexWhere(_.id == c)
          if i < 0 then
            None
          else
            exprs(i).expr match
              case r: ColRef =>
                Some(ColumnId(r.index))
              case _ =>
                None
        }
        uniqueOn(child, copies)
      case Join(left, right, JoinKind.Inner, lk, rk, _, _) if lk.nonEmpty =>
        def on(side: RuntimePlan): Seq[ColumnId] =
          val ids = side.output.map(_.id).toSet
          cols.filter(ids.contains)
        (looksUp(right, rk) && uniqueOn(left, on(left))) ||
        (looksUp(left, lk) && uniqueOn(right, on(right)))
      case _ =>
        false

  /**
    * Whether a join by `keys` finds at most one row of `side`: the keys are plain columns, and
    * `side` is unique on them ([[uniqueOn]])
    */
  private[runtime] def looksUp(side: RuntimePlan, keys: Seq[BExpr]): Boolean =
    keys.forall(_.expr.isInstanceOf[ColRef]) && uniqueOn(side, keys.flatMap(_.refs))

  /** The aggregation grouped by the keys that determine the others, if any key is determined */
  private def reduce(a: Aggregate): Option[RuntimePlan] =
    val lineage = Lineage()
    val traced  = lineage.trace(a.child)
    if lineage.scans.forall(_.source.uniqueKeys.isEmpty) then
      None
    else
      val nk = a.keys.size
      // The source columns of each key, and those it makes known when it is a plain column
      val srcs: IndexedSeq[Option[Set[Src]]] = a
        .keys
        .map { k =>
          val refs = k.refs
          Option.when(
            refs.nonEmpty && refs.forall(traced.contains) && PushDownFilters.isDeterministic(k.expr)
          )(refs.flatMap(traced(_).srcs))
        }
      val plain: IndexedSeq[Set[Src]] = a
        .keys
        .indices
        .map { i =>
          a.keys(i).expr match
            case c: ColRef =>
              traced.get(ColumnId(c.index)).flatMap(_.copy).toSet
            case _ =>
              Set.empty
        }
      // Try to drop computed keys first, then the widest types, the last keys first among equals
      val order = a
        .keys
        .indices
        .filter(srcs(_).isDefined)
        .sortBy { i =>
          val computed =
            a.keys(i).expr match
              case _: ColRef =>
                1
              case _ =>
                0
          (computed, widthRank(a.columns(i).ptype), -i)
        }
      val kept = mutable.Set.from(a.keys.indices)
      order.foreach { i =>
        val rest = kept - i
        if rest.nonEmpty && lineage.determines(rest.flatMap(plain).toSet, srcs(i).get) then
          kept -= i
      }
      if kept.size == nk then
        None
      else
        val keptIdx    = a.keys.indices.filter(kept.contains)
        val droppedIdx = a.keys.indices.filterNot(kept.contains)
        val payload    = droppedIdx.map(i =>
          AggregateCall(
            "any_value",
            Seq(a.keys(i)),
            distinct = false,
            filter = None,
            a.columns(i).ptype
          )
        )
        val agg = Aggregate(
          a.child,
          keptIdx.map(a.keys),
          a.calls ++ payload,
          keptIdx.map(a.columns) ++ a.columns.drop(nk) ++ droppedIdx.map(a.columns)
        )
        Some(Project(agg, a.columns.map(BExpr.ref), a.columns))
      end if
    end if

  end reduce

  /** Lower for the types costlier to hash and compare, which are dropped first */
  private def widthRank(t: PType): Int =
    t match
      case PType.Str =>
        0
      case _: PType.Decimal =>
        1
      case _ =>
        2

end DependentGroupKeys
