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

import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.VExprs

import scala.collection.mutable

/**
  * Structural identity of runtime plans, for sharing subplans that compute the same rows (framework
  * pass 5): the planner evaluates uncorrelated scalar subqueries whose inputs have the same key as
  * one query.
  */
object ShareSubplans:
  /**
    * A key of the plan that is equal for two plans exactly when they are equal up to the choice of
    * column ids: ids are renumbered in order of definition, children first. Column names, types,
    * and qualifiers are part of the key, so that names resolve alike over plans with equal keys.
    *
    * Equality is conservative. Expressions compare as values after renumbering (floating-point
    * constants by their bits), so a row function compares its function by reference. Literal
    * counts, which only steer join order estimates, are left out. A plan with `Values` rows or a
    * `CTERef`, whose batches and CTEs only compare by reference, or one reading a column it doesn't
    * define (an outer reference), has no key.
    */
  def key(p: RuntimePlan): Option[Any] =
    val ids = mutable.HashMap.empty[Int, Int]
    try
      Some(Canonicalizer(ids, loose = false).canon(p))
    catch
      case _: NoKey =>
        None

  /**
    * A key of the rows of `p` as a set of columns, and the canonical id of each output column in
    * order: equal for two plans that compute the same rows up to the choice of column ids, column
    * qualifiers (table aliases), the order of inner-join inputs and conditions, and the order of
    * the output columns. A column of one plan reads the same values as the column of the other with
    * the same canonical id. Callers bind over the ids, not over names
    */
  def looseKey(p: RuntimePlan): Option[(Any, IndexedSeq[Int])] =
    val ids = mutable.HashMap.empty[Int, Int]
    try
      val k = Canonicalizer(ids, loose = true).canon(p)
      Some((k, p.output.map(c => ids(c.id.value))))
    catch
      case _: NoKey | _: NoSuchElementException =>
        None

  private class NoKey extends RuntimeException(null, null, false, false)

  private class Canonicalizer(ids: mutable.HashMap[Int, Int], loose: Boolean):
    private def define(cs: IndexedSeq[Column]): IndexedSeq[Any] = cs.map { c =>
      val id = ids.size + 1
      ids(c.id.value) = id
      if loose then
        (id, c.name, c.ptype)
      else
        (id, c.name, c.ptype, c.qualifiers)
    }

    // With the bits of its floating-point constants, since `==` on boxed values equates 0.0 and
    // -0.0 (and Scala.js prints both as 0)
    private def expr(e: BExpr): Any =
      val v = renumber(e)
      (v, floatBits(v))

    private def floatBits(e: VExpr): Seq[Long] =
      e match
        case Const(d: Double, _) =>
          Seq(java.lang.Double.doubleToLongBits(d))
        case Const(f: Float, _) =>
          Seq(java.lang.Float.floatToIntBits(f).toLong)
        case other =>
          other.children.flatMap(floatBits)

    private def renumber(e: BExpr): VExpr = VExprs.mapRefs(
      e.expr,
      c =>
        ids.get(c.index) match
          case Some(id) =>
            c.copy(index = id)
          case None =>
            throw NoKey()
    )

    private def exprs(es: Seq[BExpr]): Seq[Any] = es.map(expr)

    private def order(k: OrderKey): Any = (expr(k.expr), k.ascending, k.nullsFirst)

    def canon(p: RuntimePlan): Any =
      p match
        case RuntimePlan.Scan(source, cols, read, filter) =>
          ("scan", source.key, read, filter, define(cols))
        case RuntimePlan.Values(_, _) | RuntimePlan.CTERef(_, _) | RuntimePlan.Sample(_, _) =>
          // A sample draws its own random numbers, so two of them aren't shared
          throw NoKey()
        case RuntimePlan.Filter(child, cond, _) =>
          val c = canon(child)
          ("filter", c, expr(cond))
        case RuntimePlan.Project(child, es, cols) =>
          val c = canon(child)
          val e = exprs(es)
          ("project", c, e, define(cols))
        case RuntimePlan.Sort(child, keys, limit) =>
          val c = canon(child)
          ("sort", c, keys.map(order), limit)
        case RuntimePlan.Limit(child, limit, offset) =>
          ("limit", canon(child), limit, offset)
        case RuntimePlan.Aggregate(child, keys, calls, cols) =>
          val c  = canon(child)
          val k  = exprs(keys)
          val cs = calls.map(a =>
            (a.name, exprs(a.args), a.distinct, a.filter.map(expr), a.ptype, a.order)
          )
          ("aggregate", c, k, cs, define(cols))
        case RuntimePlan.Join(left, right, kind, lk, rk, residual, buildLeft) =>
          val l = canon(left)
          val r = canon(right)
          ("join", l, r, kind, exprs(lk), exprs(rk), residual.map(expr), buildLeft)
        case RuntimePlan.AsOfJoin(left, right, kind, lk, rk, lv, rv, op) =>
          val l = canon(left)
          val r = canon(right)
          ("asof_join", l, r, kind, exprs(lk), exprs(rk), expr(lv), expr(rv), op)
        case RuntimePlan.InnerJoins(inputs, conds) if loose =>
          // Inputs in the order of their own keys, each numbered apart, then numbered in that order
          val sorted = inputs.sortBy(in =>
            Canonicalizer(mutable.HashMap.empty, loose).canon(in).toString
          )
          val in = sorted.map(canon)
          ("inner_joins", in, conds.map(c => expr(c.cond)).sortBy(_.toString))
        case RuntimePlan.InnerJoins(inputs, conds) =>
          val in = inputs.map(canon)
          ("inner_joins", in, conds.map(c => expr(c.cond)))
        case RuntimePlan.Union(inputs, cols) =>
          val in = inputs.map(canon)
          ("union", in, define(cols))
        case RuntimePlan.SetOp(left, right, intersect, all, cols) =>
          val l = canon(left)
          val r = canon(right)
          ("set_op", l, r, intersect, all, define(cols))
        case RuntimePlan.Distinct(child) =>
          ("distinct", canon(child))
        case RuntimePlan.Window(child, calls, cols) =>
          val c  = canon(child)
          val ws = calls.map(w =>
            (
              w.name,
              exprs(w.args),
              exprs(w.partitionBy),
              w.orderBy.map(order),
              w.frame,
              w.ignoreNulls,
              w.ptype
            )
          )
          ("window", c, ws, define(cols))
        case RuntimePlan.Unnest(child, lists, keepInput, outer, cols) =>
          val c = canon(child)
          ("unnest", c, exprs(lists), keepInput, outer, define(cols))
      end match
    end canon

  end Canonicalizer

end ShareSubplans
