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
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.PType

/**
  * Group by a dimension row's id instead of its string columns (`dimension_group_ids`). An
  * aggregation whose group keys Ks are string columns of a dimension D, which a larger input looks
  * up by D's declared unique key `pk`, groups the joined rows by a representative id of their Ks:
  * `rep = min(pk) over (partition by Ks)`, computed by a window over D's rows. Rows with equal Ks
  * get the same rep, and different Ks different reps (a rep is the key of a row with those Ks), so
  * grouping by rep forms the same groups. The join then carries one long per row instead of the
  * strings, the aggregation hashes packed longs, and the strings are joined back per group from D's
  * rows whose `pk = rep`. A window partition holds the rows of NULL Ks together, so they get one
  * rep, as the rows form one group.
  *
  * TPC-DS q47 and q89 group store sales by item category and brand and by store name and company,
  * and q57 by item and call center strings: each sale row carried four strings into the group
  * table.
  *
  * It fires when the input that looks D up is estimated at [[MinFanout]] times D's rows, and Ks are
  * read by nothing but the group keys.
  */
object DimensionGroupIds extends Pass:
  import RuntimePlan.*

  /** The estimated rows of the looking-up input per row of D, at least */
  val MinFanout: Double = 8.0

  /** The rows of D per estimated group of its keys, at least, unless D is small */
  val MinCoarseness: Double = 4.0

  /** The rows of a dimension small enough to rewrite whatever its keys' groups */
  val SmallDimension: Double = 1000.0

  override def name: String = Optimization.DimensionGroupIds.name

  override def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    ctx.newColumnId match
      case Some(newId) if ctx.options.isEnabled(Optimization.DimensionGroupIds) =>
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
      case a: Aggregate =>
        rewrite(a, ctx, newId) match
          case Some(r) =>
            ctx.fired(name)
            r
          case None =>
            node
      case _ =>
        node

  /**
    * The aggregation grouped by dimension reps, one dimension at a time, under the projections that
    * restore its columns
    */
  private def rewrite(
      a: Aggregate,
      ctx: OptimizerContext,
      newId: () => ColumnId
  ): Option[RuntimePlan] =
    def loop(a: Aggregate, wrap: RuntimePlan => RuntimePlan, fired: Boolean): Option[RuntimePlan] =
      once(a, ctx, newId) match
        case Some((next, outer)) =>
          loop(next, p => wrap(outer(p)), fired = true)
        case None =>
          Option.when(fired)(wrap(a))
    loop(a, identity, fired = false)

  /** A dimension side found under the aggregation, rewritten to carry its rep */
  private case class Found(
      plan: RuntimePlan,
      // The rep column, at the found node's level
      rep: Column,
      // The top-level key column ids replaced, and their column ids in the dimension
      keys: Seq[(ColumnId, ColumnId)],
      // The dimension input and its key column
      dim: RuntimePlan,
      pk: Column
  )

  /**
    * The aggregation grouped by one dimension's rep instead of its string keys, and the plan over
    * it that joins the strings back and restores a's columns
    */
  private def once(
      a: Aggregate,
      ctx: OptimizerContext,
      newId: () => ColumnId
  ): Option[(Aggregate, RuntimePlan => RuntimePlan)] =
    val options = ctx.options
    if a.keys.isEmpty || !a.keys.forall(k => PushDownFilters.isDeterministic(k.expr)) then
      return None
    val candidates: Seq[ColumnId] =
      a.keys
        .collect {
          case BExpr(c: ColRef) if c.ptype == PType.Str =>
            ColumnId(c.index)
        }
        .distinct
    if candidates.isEmpty then
      return None
    val candidateSet = candidates.toSet
    // Columns read other than as plain candidate keys
    val readElsewhere =
      a.calls.flatMap(_.exprs.flatMap(_.refs)).toSet ++
        a.keys
          .filter {
            case BExpr(c: ColRef) =>
              !candidateSet.contains(ColumnId(c.index))
            case _ =>
              true
          }
          .flatMap(_.refs)

    // `keyMap`: candidate key ids at the top to their ids at p; `needed`: columns of p read
    // above it other than as candidate keys
    def down(
        p: RuntimePlan,
        keyMap: Map[ColumnId, ColumnId],
        needed: Set[ColumnId]
    ): Option[Found] =
      p match
        case j @ Join(left, right, JoinKind.Inner, lk, rk, residual, _) if lk.size == 1 =>
          val below = needed ++ (lk ++ rk ++ residual).flatMap(_.refs)
          def dimension(d: RuntimePlan, dKey: BExpr, other: RuntimePlan): Option[Found] =
            val ids  = d.output.map(_.id).toSet
            val keys = keyMap.toSeq.filter((_, at) => ids.contains(at) && !below.contains(at))
            dKey.expr match
              case c: ColRef
                  if keys.nonEmpty &&
                    (c.ptype == PType.Int64 || c.ptype == PType.Int32) && oneScan(d) &&
                    DependentGroupKeys.uniqueOn(d, Seq(ColumnId(c.index))) &&
                    Stats.treeRows(other, options) >= MinFanout * Stats.treeRows(d, options) &&
                    coarse(d, keys.map(_._2), options) && !aggregated(other) =>
                val pk  = d.output.find(_.id.value == c.index).get
                val rep = Column(newId(), s"${pk.name}_rep", pk.ptype)
                val win = Window(d, IndexedSeq(repCall(pk, keys.map(_._2), d)), IndexedSeq(rep))
                // D's columns but the replaced keys, and the rep
                val dropped = keys.map(_._2).toSet
                val kept    = d.output.filterNot(c => dropped.contains(c.id)) :+ rep
                Some(Found(Project(win, kept.map(BExpr.ref), kept), rep, keys, d, pk))
              case _ =>
                None
          // The dimension on the build side first, then the probe side, then deeper
          dimension(right, rk.head, left)
            .map(f => f.copy(plan = j.copy(right = f.plan)))
            .orElse(dimension(left, lk.head, right).map(f => f.copy(plan = j.copy(left = f.plan))))
            .orElse(down(left, keyMap, below).map(f => f.copy(plan = j.copy(left = f.plan))))
            .orElse(down(right, keyMap, below).map(f => f.copy(plan = j.copy(right = f.plan))))
        case Filter(child, cond, counts) =>
          down(child, keyMap, needed ++ cond.refs).map(f =>
            f.copy(plan = Filter(f.plan, cond, counts))
          )
        case Project(child, exprs, columns) =>
          val at      = columns.indices.map(i => columns(i).id -> i).toMap
          val through = keyMap.flatMap { (top, id) =>
            at.get(id)
              .flatMap(i =>
                exprs(i).expr match
                  case c: ColRef =>
                    Some(top -> ColumnId(c.index))
                  case _ =>
                    None
              )
          }
          val neededIdx = columns.indices.filter(i => needed.contains(columns(i).id))
          // A candidate key computed here (lower(p_brand)) reads its inputs as more than keys
          val computed = keyMap
            .values
            .flatMap(at.get)
            .filter(i => !exprs(i).expr.isInstanceOf[ColRef])
          val below = (neededIdx ++ computed).flatMap(i => exprs(i).refs).toSet
          down(child, through, below).flatMap { f =>
            // The replaced keys are gone below: keep the columns that don't read them, which
            // must include the needed ones
            val childIds = f.plan.output.map(_.id).toSet
            val kept     = columns.indices.filter(i => exprs(i).refs.forall(childIds.contains))
            Option.when((neededIdx ++ computed).forall(kept.contains))(
              f.copy(plan =
                Project(f.plan, kept.map(exprs) :+ BExpr.ref(f.rep), kept.map(columns) :+ f.rep)
              )
            )
          }
        case _ =>
          None

    down(a.child, candidates.map(c => c -> c).toMap, readElsewhere).map { f =>
      val replaced                             = f.keys.map(_._1).toSet
      val keyIdx                               = a.keys.indices
      def replacedAt(i: Int): Option[ColumnId] =
        a.keys(i).expr match
          case c: ColRef if replaced.contains(ColumnId(c.index)) =>
            Some(ColumnId(c.index))
          case _ =>
            None
      val keptIdx        = keyIdx.filter(replacedAt(_).isEmpty)
      val repOut         = Column(newId(), f.rep.name, f.rep.ptype)
      val calls          = a.columns.drop(a.keys.size)
      val agg: Aggregate = Aggregate(
        f.plan,
        keptIdx.map(a.keys) :+ BExpr.ref(f.rep),
        a.calls,
        keptIdx.map(a.columns) ++ (repOut +: calls)
      )
      val outer: RuntimePlan => RuntimePlan =
        below =>
          // The strings of each rep, from a copy of D grouped by them: the same min(key) per group
          val (dim, m) = copyWithNewIds(f.dim, newId)
          val pk2      = m(f.pk.id)
          val keyCols2 = f.keys.map((_, d) => m(d)).toIndexedSeq
          val keyOuts  = keyCols2.map(c => c.copy(id = newId()))
          val rep2     = Column(newId(), f.rep.name, f.rep.ptype)
          val strings  = Aggregate(
            dim,
            keyCols2.map(BExpr.ref),
            IndexedSeq(
              AggregateCall("min", Seq(BExpr.ref(pk2)), distinct = false, filter = None, pk2.ptype)
            ),
            keyOuts :+ rep2
          )
          val joined = Join(
            below,
            strings,
            JoinKind.Inner,
            IndexedSeq(BExpr.ref(repOut)),
            IndexedSeq(BExpr.ref(rep2)),
            None,
            buildLeft = false
          )
          val stringOf = f.keys.map(_._1).zip(keyOuts).toMap
          Project(
            joined,
            a.columns
              .indices
              .map { i =>
                if i < a.keys.size then
                  replacedAt(i) match
                    case Some(id) =>
                      BExpr.ref(stringOf(id))
                    case None =>
                      BExpr.ref(a.columns(i))
                else
                  BExpr.ref(a.columns(i))
              },
            a.columns
          )
      (agg, outer)
    }

  end once

  /**
    * Whether the keys `keys` of d form at most 1/[[MinCoarseness]] as many groups as d has rows (by
    * the product of their estimated distinct values), or d has at most [[SmallDimension]] rows.
    * With nearly unique strings (TPC-DS q04's customer names, q98's item ids) each rep is one row
    * of d, and the windows and the join back cost more than the strings they save
    */
  private def coarse(d: RuntimePlan, keys: Seq[ColumnId], options: OptimizerOptions): Boolean =
    val rows   = Stats.treeRows(d, options)
    val groups =
      keys.foldLeft(Option(1.0)) { (acc, k) =>
        acc.flatMap(n => EagerAggregation.distinct(d, k).map(v => n * math.max(1.0, v)))
      }
    rows <= SmallDimension || groups.exists(_ * MinCoarseness <= rows)

  /**
    * Whether p's rows are an aggregation's groups (under filters and projections): TPC-DS q31 looks
    * up the addresses of store sales already summed per address, quarter and year, which the
    * windows over the addresses cost more than they save
    */
  private def aggregated(p: RuntimePlan): Boolean =
    p match
      case _: Aggregate =>
        true
      case Filter(child, _, _) =>
        aggregated(child)
      case Project(child, _, _) =>
        aggregated(child)
      case _ =>
        false

  /**
    * Whether p is a scan under deterministic filters and projections, whose copy for the join back
    * reads the same rows (a sample would draw others)
    */
  private def oneScan(p: RuntimePlan): Boolean =
    p match
      case _: Scan =>
        true
      case Filter(child, cond, _) =>
        PushDownFilters.isDeterministic(cond.expr) && oneScan(child)
      case Project(child, exprs, _) =>
        exprs.forall(e => PushDownFilters.isDeterministic(e.expr)) && oneScan(child)
      case _ =>
        false

  /** `min(pk) over (partition by keys)` over d's rows */
  private def repCall(pk: Column, keys: Seq[ColumnId], d: RuntimePlan): WindowFunction =
    val byId = d.output.map(c => c.id -> c).toMap
    WindowFunction(
      "min",
      Seq(BExpr.ref(pk)),
      keys.map(k => BExpr.ref(byId(k))),
      Nil,
      None,
      ignoreNulls = false,
      pk.ptype
    )

  /**
    * A copy of p, a scan under filters and projections, with new column ids, and the new column of
    * each old one
    */
  private def copyWithNewIds(
      p: RuntimePlan,
      newId: () => ColumnId
  ): (RuntimePlan, Map[ColumnId, Column]) =
    def fresh(cs: IndexedSeq[Column]): IndexedSeq[Column] = cs.map(c => c.copy(id = newId()))
    def remap(e: BExpr, m: Map[ColumnId, Column]): BExpr  = EagerAggregation.remap(e, m)
    p match
      case s: Scan =>
        val cols = fresh(s.columns)
        (s.copy(columns = cols), s.columns.map(_.id).zip(cols).toMap)
      case Filter(child, cond, counts) =>
        val (c, m) = copyWithNewIds(child, newId)
        (Filter(c, remap(cond, m), counts), m)
      case Project(child, exprs, columns) =>
        val (c, m) = copyWithNewIds(child, newId)
        val cols   = fresh(columns)
        (Project(c, exprs.map(remap(_, m)), cols), m ++ columns.map(_.id).zip(cols))
      case other =>
        throw IllegalStateException(s"Not a scan under filters and projections: ${other}")

  end copyWithNewIds

end DimensionGroupIds
