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
import wvlet.lang.engine.vector.PType

import scala.collection.mutable

/**
  * Join the readers of a shared CTE on a long key instead of the string key it determines
  * (`cte_determining_join_keys`). TPC-DS q04, q11 and q74 join their per-year `year_total` readers
  * on `customer_id`, a copy of customer's declared unique `c_customer_id`. Customer also declares
  * `c_customer_sk` unique, so two body rows from customer rows share `c_customer_id` exactly when
  * they share `c_customer_sk`: the body carries `c_customer_sk` as one more column, and the readers
  * join on it.
  *
  * The body column must copy a single-column declared key of a scan, traced through projections,
  * filters, the preserved inputs of joins, union branches (each from the same table), and group
  * keys, where the long key joins the group keys (the string key determines it, so the groups stay
  * the same; `dependent_group_keys` then groups by the long key alone). The long key must be
  * another single-column declared key of the same scan, and non-NULL: an inner equi-join on it
  * between the scan and the body output. Then `a.k = b.k` (a top-level condition of inner joins of
  * two readers) is `a.long = b.long and a.k is not null`: equal long keys are one table row, whose
  * string keys are equal unless NULL.
  */
object CTEJoinKeys:
  import RuntimePlan.*

  /**
    * The long key column a body output carries, the table, the string key it determines (`source`)
    * and the long key it copies, and whether it is non-NULL
    */
  private case class Extended(
      plan: RuntimePlan,
      key: Column,
      table: String,
      source: String,
      name: String,
      nonNull: Boolean
  )

  /**
    * The body with one more output column at the end: the long declared key that determines the
    * string key at output `position`, if there is one
    */
  def extendBody(body: RuntimePlan, position: Int, newId: () => ColumnId): Option[RuntimePlan] =
    val out = body.output
    if position >= out.size || out(position).ptype != PType.Str then
      None
    else
      extend(body, out(position).id, newId)
        .filter(_.nonNull)
        .map { e =>
          val keyCol = Column(newId(), s"__${e.name}", e.key.ptype)
          Project(e.plan, (out :+ e.key).map(BExpr.ref), out :+ keyCol)
        }
        // A second long key of the same name would make the readers' columns ambiguous
        .filter(_.schema.names.distinct.size == out.size + 1)

  private def ref(c: Column): BExpr = BExpr.ref(c)

  /** Whether `cond` is an equality of column `id` and another column */
  private def equates(cond: BExpr, id: ColumnId): Boolean = FactorOr
    .conjuncts(cond.expr)
    .exists {
      case Compare(CmpOp.Eq, a: ColRef, b: ColRef) =>
        a.index != b.index && (a.index == id.value || b.index == id.value)
      case _ =>
        false
    }

  /** Whether `keys` hold a plain reference to column `id` */
  private def keysRef(keys: IndexedSeq[BExpr], id: ColumnId): Boolean = keys.exists(
    _.expr match
      case c: ColRef =>
        c.index == id.value
      case _ =>
        false
  )

  /**
    * The plan with the long key column that determines column `id` of p (a copy of a declared
    * string key) among its outputs, which keep their ids
    */
  private def extend(p: RuntimePlan, id: ColumnId, newId: () => ColumnId): Option[Extended] =
    p match
      case s: Scan =>
        val i = s.columns.indexWhere(_.id == id)
        if i < 0 then
          None
        else
          val keys = s.source.uniqueKeys.filter(_.size == 1).map(_.head.toLowerCase)
          if !keys.contains(s.sourceColumnName(i).toLowerCase) then
            None
          else
            s.columns
              .indices
              .find { j =>
                j != i && keys.contains(s.sourceColumnName(j).toLowerCase) &&
                (s.columns(j).ptype == PType.Int64 || s.columns(j).ptype == PType.Int32)
              }
              .map(j =>
                Extended(
                  s,
                  s.columns(j),
                  s.source.name.toLowerCase,
                  s.sourceColumnName(i).toLowerCase,
                  s.sourceColumnName(j).toLowerCase,
                  nonNull = false
                )
              )
      case Filter(child, cond, counts) =>
        extend(child, id, newId).map(e =>
          e.copy(
            plan = Filter(e.plan, cond, counts),
            nonNull = e.nonNull || equates(cond, e.key.id)
          )
        )
      case Project(child, exprs, columns) =>
        val i = columns.indexWhere(_.id == id)
        if i < 0 then
          None
        else
          exprs(i).expr match
            case c: ColRef =>
              extend(child, ColumnId(c.index), newId).map { e =>
                val col = Column(newId(), e.key.name, e.key.ptype)
                e.copy(plan = Project(e.plan, exprs :+ ref(e.key), columns :+ col), key = col)
              }
            case _ =>
              None
      case j @ Join(left, right, kind, lk, rk, _, _) =>
        val inLeft = left.output.exists(_.id == id)
        if inLeft && (kind == JoinKind.Inner || kind == JoinKind.Left || j.isSemi) then
          extend(left, id, newId).map(e =>
            e.copy(
              plan = j.copy(left = e.plan),
              nonNull = e.nonNull || kind == JoinKind.Inner && keysRef(lk, e.key.id)
            )
          )
        else if !inLeft && !j.isSemi && (kind == JoinKind.Inner || kind == JoinKind.Right) then
          extend(right, id, newId).map(e =>
            e.copy(
              plan = j.copy(right = e.plan),
              nonNull = e.nonNull || kind == JoinKind.Inner && keysRef(rk, e.key.id)
            )
          )
        else
          None
      case InnerJoins(inputs, conds) =>
        val k = inputs.indexWhere(_.output.exists(_.id == id))
        if k < 0 then
          None
        else
          extend(inputs(k), id, newId).map(e =>
            e.copy(
              plan = InnerJoins(inputs.updated(k, e.plan), conds),
              nonNull = e.nonNull || conds.exists(c => equates(c.cond, e.key.id))
            )
          )
      case Aggregate(child, keys, calls, columns) =>
        val i = columns.indexWhere(_.id == id)
        if i < 0 || i >= keys.size then
          None
        else
          keys(i).expr match
            case c: ColRef =>
              extend(child, ColumnId(c.index), newId).map { e =>
                // The string key determines the long key, so the groups stay the same
                val col = Column(newId(), e.key.name, e.key.ptype)
                val n   = keys.size
                e.copy(
                  plan = Aggregate(
                    e.plan,
                    keys :+ ref(e.key),
                    calls,
                    (columns.take(n) :+ col) ++ columns.drop(n)
                  ),
                  key = col
                )
              }
            case _ =>
              None
      case Union(inputs, columns) =>
        val i = columns.indexWhere(_.id == id)
        if i < 0 then
          None
        else
          val branches = inputs.map(in => extend(in, in.output(i).id, newId))
          if branches.exists(_.isEmpty) then
            None
          else
            val es = branches.flatten
            val e  = es.head
            // Each branch copies the same string key: equal long keys are then equal strings
            if es.exists(x =>
                x.table != e.table || x.source != e.source || x.name != e.name ||
                  x.key.ptype != e.key.ptype
              )
            then
              None
            else
              // Union columns are by position: each branch outputs the key last
              val aligned = inputs
                .zip(es)
                .map { (in, x) =>
                  val o = in.output
                  Project(
                    x.plan,
                    (o :+ x.key).map(ref),
                    o :+ Column(newId(), x.key.name, x.key.ptype)
                  )
                }
              val col = Column(newId(), e.key.name, e.key.ptype)
              Some(
                Extended(
                  Union(aligned, columns :+ col),
                  col,
                  e.table,
                  e.source,
                  e.name,
                  es.forall(_.nonNull)
                )
              )
          end if
        end if
      case _ =>
        None

  /**
    * The plan with the equalities between readers of one shared CTE rewritten to the long keys that
    * `extendKey` makes the CTE carry: `extendKey(cte, position)` gives the body position of the
    * long key for the string key at `position`, if the body can carry it
    */
  def rewrite(
      p: RuntimePlan,
      extendKey: (SharedCTE, Int) => Option[Int],
      newId: () => ColumnId
  ): RuntimePlan =
    val children = p.children
    val next     = children.map(rewrite(_, extendKey, newId))
    val node     =
      if next.corresponds(children)(_ eq _) then
        p
      else
        p.withChildren(next)
    node match
      case j: InnerJoins =>
        rewriteJoins(j, extendKey, newId).getOrElse(node)
      case _ =>
        node

  private def rewriteJoins(
      j: InnerJoins,
      extendKey: (SharedCTE, Int) => Option[Int],
      newId: () => ColumnId
  ): Option[RuntimePlan] =
    // Reader column id -> (input index, body position)
    val readerCols = mutable.HashMap.empty[Int, (Int, Int)]
    j.inputs
      .indices
      .foreach { k =>
        j.inputs(k) match
          case CTERef(_, columns) =>
            columns.indices.foreach(i => readerCols(columns(i).id.value) = (k, i))
          case _ =>
      }
    def cteOf(k: Int): SharedCTE =
      j.inputs(k) match
        case CTERef(cte, _) =>
          cte
        case _ =>
          throw IllegalStateException("not a CTE reader")
    val inputs = j.inputs.toArray
    // The column of a reader at a body position, added when it doesn't read it yet
    def columnAt(k: Int, position: Int): Column =
      inputs(k) match
        case CTERef(cte, columns) =>
          if position < columns.size then
            columns(position)
          else
            val body  = cte.body.output
            val extra = (columns.size to position).map(i =>
              Column(newId(), body(i).name, body(i).ptype)
            )
            inputs(k) = CTERef(cte, columns ++ extra)
            extra.last
        case _ =>
          throw IllegalStateException("not a CTE reader")
    val notNull = mutable.LinkedHashSet.empty[Int]
    var changed = false
    val conds   = j
      .conds
      .map { c =>
        c.cond.expr match
          case Compare(CmpOp.Eq, a: ColRef, b: ColRef)
              if readerCols.contains(a.index) && readerCols.contains(b.index) =>
            val (ka, pa) = readerCols(a.index)
            val (kb, pb) = readerCols(b.index)
            if ka != kb && pa == pb && (cteOf(ka) eq cteOf(kb)) then
              extendKey(cteOf(ka), pa) match
                case Some(q) =>
                  val la = columnAt(ka, q)
                  val lb = columnAt(kb, q)
                  notNull += a.index
                  changed = true
                  c.withCond(Compare(CmpOp.Eq, ref(la).expr, ref(lb).expr))
                case None =>
                  c
            else
              c
          case _ =>
            c
      }
    Option.when(changed) {
      val guards = notNull
        .toSeq
        .map { id =>
          val (k, i) = readerCols(id)
          val col    = inputs(k).output(i)
          JoinCondition(BExpr(IsNullExpr(ColRef(id, col.ptype, col.name), negate = true)))
        }
      val joined = InnerJoins(inputs.toIndexedSeq, conds ++ guards)
      // The same output as before: the added reader columns stay inside
      Project(joined, j.output.map(ref), j.output)
    }

  end rewriteJoins

end CTEJoinKeys
