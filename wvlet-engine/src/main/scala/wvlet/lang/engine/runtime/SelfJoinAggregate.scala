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
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.IsNullExpr
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.vector.PType

/**
  * A semi join of a table T with itself on an equal key and a different value,
  * `T as l ⋉ T on l.k = T.k and l.x <> T.x`, with both sides whole scans of T, becomes a semi join
  * on k alone with the keys whose group holds two different values of x:
  * `filter(T, x is not null) ⋉ (T where k is not null group by k having min(x) <> max(x))`. A NULL
  * x never satisfies `<>`, and a non-NULL one is in its own key's group, so some row of the group
  * differs from it exactly when min and max (which skip NULLs) differ
  * (`formal/WvletFormal/SelfJoinAggregate`). The join then builds a table of one row per key
  * instead of every row of T, and checks no residual over each key's duplicates.
  *
  * Where only the distinct keys of the join matter ([[ReduceSemiJoins]]' duplicate-insensitive
  * context), the join is the aggregation's keys alone, as in DuckDB's SELF_JOIN_AGGREGATE: TPC-DS
  * q95's `ws_wh`, read only through `ws_order_number`, scans web_sales once instead of joining it
  * with itself.
  *
  * The model also covers a left side that is filtered rows of T (TPC-DS q94 and q16, TPC-H q21),
  * but there the aggregation over all of T costs more than the hash table it replaces, so the rule
  * leaves them. The value must not be a float, whose NaN compares as different from itself under
  * `<>` but not under min / max.
  */
object SelfJoinAggregate:
  import RuntimePlan.*

  /**
    * The rewrite of p, a node of [[ReduceSemiJoins]]' walk in a duplicate-insensitive context or
    * not (`inSet`) whose parent reads its columns `needed`, if it is such a self join
    */
  def rewrite(
      p: RuntimePlan,
      inSet: Boolean,
      needed: Set[ColumnId],
      ctx: OptimizerContext
  ): Option[RuntimePlan] =
    if !ctx.options.isEnabled(Optimization.SelfJoinAggregate) then
      None
    else
      for
        newId <- ctx.newColumnId
        m     <- matchSelfJoin(p)
      yield
        val (k, x)             = (m.k, m.x)
        val keyed              = Filter(m.table, BExpr(IsNullExpr(ref(k), negate = true)))
        val key                = Column(newId(), k.name, k.ptype, k.qualifiers)
        val minX               = Column(newId(), s"min_${x.name}", x.ptype)
        val maxX               = Column(newId(), s"max_${x.name}", x.ptype)
        def call(name: String) = AggregateCall(
          name,
          Seq(BExpr(ref(x))),
          distinct = false,
          filter = None,
          x.ptype
        )
        val groups = Filter(
          Aggregate(
            keyed,
            IndexedSeq(BExpr(ref(k))),
            IndexedSeq(call("min"), call("max")),
            IndexedSeq(key, minX, maxX)
          ),
          BExpr(Compare(CmpOp.Ne, ref(minX), ref(maxX)))
        )
        if inSet && needed.subsetOf(Set(m.leftKey.id)) then
          // The distinct keys of T ⋉ T are those of the groups, under the left key column
          Project(groups, IndexedSeq(BExpr(ref(key))), IndexedSeq(m.leftKey))
        else
          Join(
            Filter(m.left, BExpr(IsNullExpr(ref(m.leftValue), negate = true))),
            groups,
            JoinKind.Semi,
            IndexedSeq(BExpr(ref(m.leftKey))),
            IndexedSeq(BExpr(ref(key))),
            residual = None,
            buildLeft = m.buildLeft
          )

  /**
    * A matched self join: the left scan with its key and value columns, and the right scan of the
    * same table with its key and value columns
    */
  private case class SelfJoin(
      left: Scan,
      leftKey: Column,
      leftValue: Column,
      table: Scan,
      k: Column,
      x: Column,
      buildLeft: Boolean
  )

  private def matchSelfJoin(p: RuntimePlan): Option[SelfJoin] =
    p match
      case Join(
            left @ Scan(leftSource, _, _, None),
            right @ Scan(source, _, _, None),
            JoinKind.Semi,
            lk,
            rk,
            Some(res),
            buildLeft
          ) if lk.size == 1 && (leftSource eq source) =>
        val leftCols  = left.columns.map(c => c.id -> c).toMap
        val rightCols = right.columns.map(c => c.id -> c).toMap
        def pair(a: ColumnId, b: ColumnId): Option[(Column, Column)] = leftCols
          .get(a)
          .zip(rightCols.get(b))
          .orElse(leftCols.get(b).zip(rightCols.get(a)))
        for
          (lkc, k) <- columnOf(lk.head).zip(columnOf(rk.head)).flatMap(pair)
          (lx, x)  <-
            res.expr match
              case Compare(CmpOp.Ne, a: ColRef, b: ColRef) =>
                pair(ColumnId(a.index), ColumnId(b.index))
              case _ =>
                None
          if comparable(x.ptype) && sourceIndex(left, lkc.id) == sourceIndex(right, k.id) &&
            sourceIndex(left, lx.id) == sourceIndex(right, x.id) &&
            sourceIndex(right, k.id) != sourceIndex(right, x.id)
        yield SelfJoin(left, lkc, lx, right, k, x, buildLeft)
      case _ =>
        None

  /** Whether min / max over the type agree with `<>`: equal exactly when every value is */
  private def comparable(t: PType): Boolean =
    t match
      case PType.Bool | PType.Int32 | PType.Int64 | PType.Str | PType.Date | PType.Timestamp | PType
            .TimestampTz | _: PType.Decimal =>
        true
      case _ =>
        false

  private def columnOf(e: BExpr): Option[ColumnId] =
    e.expr match
      case c: ColRef =>
        Some(ColumnId(c.index))
      case _ =>
        None

  /** The position in the source of the scan column of id `id` */
  private def sourceIndex(s: Scan, id: ColumnId): Int =
    val i = s.columns.indexWhere(_.id == id)
    s.read.map(_(i)).getOrElse(i)

  private def ref(c: Column): ColRef = ColRef(c.id.value, c.ptype, c.name)

end SelfJoinAggregate
