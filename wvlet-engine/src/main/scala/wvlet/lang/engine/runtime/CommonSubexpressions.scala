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

import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.exec.ProjectOp
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.plan.VExprs
import wvlet.lang.engine.vector.*

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Common subexpression elimination over the expressions of one operator (`common_subexpressions`):
  * a non-trivial subexpression that occurs more than once across them is computed once, in a
  * [[ProjectOp]] below the operator that appends it to the input columns, and the expressions read
  * it by position. TPC-H q1's `l_extendedprice * (1 - l_discount)` is computed once and read by
  * both `sum(...)` and `sum(... * (1 + l_tax))`.
  *
  *   - The largest repeated subexpression is shared first; its own repeated parts are shared again,
  *     in an earlier projection.
  *   - A subexpression is shared only when at least one occurrence is evaluated for every row (not
  *     only in a later CASE branch or COALESCE argument), so that sharing never evaluates what the
  *     expressions would have skipped.
  *   - Volatile functions (`random()`) are never shared. Expressions are matched with `==`, not by
  *     [[VExpr.sig]], which leaves out RowFn closures.
  */
object CommonSubexpressions:
  /** The most subexpressions shared over one operator */
  private val MaxShared = 32

  /** A shared subexpression while they are chosen: resolved to a column position at the end */
  private case class SharedRef(k: Int, ptype: PType) extends VExpr:
    def eval(b: Batch): Vec = throw IllegalStateException(s"Unresolved shared expression ${k}")
    def sig: String         = s"shared${k}"
    override def isConstant: Boolean = false

  /**
    * The input with the shared subexpressions of `exprs` appended as columns, and the expressions
    * over it. The input and `exprs` as they are when nothing repeats
    */
  def share(input: Operator, exprs: IndexedSeq[VExpr]): (Operator, IndexedSeq[VExpr]) =
    var roots = exprs
    val defs  = mutable.ArrayBuffer.empty[VExpr]
    var more  = true
    while more && defs.size < MaxShared do
      // The definitions are evaluated for every row, as the projection computes them
      mostShared(roots, defs.toSeq) match
        case Some(e) =>
          val ref = SharedRef(defs.size, e.ptype)
          roots = roots.map(replace(_, e, ref))
          defs.mapInPlace(replace(_, e, ref))
          defs += e
        case None =>
          more = false
    if defs.isEmpty then
      (input, exprs)
    else
      project(input, roots, defs.toIndexedSeq)

  /** The largest subexpression worth sharing, if any */
  private def mostShared(roots: Seq[VExpr], defs: Seq[VExpr]): Option[VExpr] =
    // Occurrences, and those evaluated for every row, in first-seen order
    val counts = mutable.LinkedHashMap.empty[VExpr, Array[Int]]
    val sizes  = mutable.HashMap.empty[VExpr, Int]

    // The size of e, or -1 when e (or a part of it) can't be shared
    def visit(e: VExpr, conditional: Boolean): Int =
      e match
        case _: ColRef | _: Const | _: SharedRef | _: Materialized =>
          1
        case f: RowFn if !PushDownFilters.isDeterministic(f) =>
          // Still look inside: deterministic arguments may repeat elsewhere
          f.args.foreach(visit(_, conditional))
          -1
        case _ if e.children.isEmpty =>
          -1
        case _ =>
          val kids         = e.children
          val conditionals = conditionalChildren(e)
          var size         = 1
          var shareable    = true
          kids
            .zip(conditionals)
            .foreach { (c, cc) =>
              val s = visit(c, conditional || cc)
              if s < 0 then
                shareable = false
              else
                size += s
            }
          if shareable && !e.isConstant then
            val c = counts.getOrElseUpdate(e, Array(0, 0))
            c(0) += 1
            if !conditional then
              c(1) += 1
            sizes(e) = size
            size
          else
            -1
    roots.foreach(visit(_, false))
    defs.foreach(visit(_, false))
    var best: Option[VExpr] = None
    var bestSize            = 0
    counts.foreach { (e, c) =>
      if c(0) >= 2 && c(1) >= 1 && sizes(e) > bestSize then
        best = Some(e)
        bestSize = sizes(e)
    }
    best

  end mostShared

  /** For each child of e, whether it is evaluated only for some rows */
  private def conditionalChildren(e: VExpr): Seq[Boolean] =
    e match
      case c: CaseWhen =>
        // Only the first condition is evaluated for every row
        c.children.indices.map(_ > 0)
      case c: Coalesce =>
        // Later arguments are evaluated only while some rows are NULL
        c.children.indices.map(_ > 0)
      case other =>
        other.children.map(_ => false)

  private def replace(e: VExpr, target: VExpr, by: VExpr): VExpr =
    if e == target then
      by
    else
      e match
        case _: ColRef | _: Const | _: SharedRef | _: Materialized =>
          e
        case _ =>
          VExprs.mapChildren(e, replace(_, target, by))

  /**
    * Projections appending the definitions to the input, one per level: a definition reading
    * another is computed in a later projection than the one it reads
    */
  private def project(
      input: Operator,
      roots: IndexedSeq[VExpr],
      defs: IndexedSeq[VExpr]
  ): (Operator, IndexedSeq[VExpr]) =
    val level                    = Array.fill(defs.size)(-1)
    def refs(e: VExpr): Seq[Int] =
      e match
        case SharedRef(k, _) =>
          Seq(k)
        case other =>
          other.children.flatMap(refs)
    def levelOf(k: Int): Int =
      if level(k) < 0 then
        level(k) = refs(defs(k)).map(levelOf(_) + 1).maxOption.getOrElse(0)
      level(k)
    defs.indices.foreach(levelOf)
    val order    = defs.indices.sortBy(k => (level(k), k))
    val position = Array.fill(defs.size)(-1)
    val width    = input.schema.size
    order.zipWithIndex.foreach((k, i) => position(k) = width + i)

    def resolve(e: VExpr): VExpr =
      e match
        case SharedRef(k, t) =>
          ColRef(position(k), t, s"__cse_${position(k) - width}")
        case _: ColRef | _: Const | _: Materialized =>
          e
        case _ =>
          VExprs.mapChildren(e, resolve)

    var op = input
    order
      .groupBy(level(_))
      .toSeq
      .sortBy(_._1)
      .foreach { (_, ks) =>
        val in       = op.schema
        val pass     = in.fields.indices.map(i => ColRef(i, in(i).ptype, in(i).name): VExpr)
        val computed = ks.map(k => resolve(defs(k)))
        val fields   = ks.map(k => Field(s"__cse_${position(k) - width}", defs(k).ptype))
        op = ProjectOp(op, pass ++ computed, Schema(in.fields ++ fields))
      }
    (op, roots.map(resolve))

  end project

end CommonSubexpressions

/**
  * Constant folding over bound expressions (`fold_constants`): a row-independent subexpression is
  * evaluated once, at plan time, instead of once per batch. An integral or narrower-scale decimal
  * constant added to or subtracted from a decimal becomes a decimal constant at the result's scale,
  * so that the kernel doesn't widen and rescale it per batch (TPC-H's `1 - l_discount`).
  */
object FoldConstants:
  def apply(e: VExpr): VExpr =
    e match
      case _: ColRef | _: Const | _: Materialized =>
        e
      case _ if e.isConstant && PushDownFilters.isDeterministic(e) =>
        // An expression that fails at plan time (1 / 0) still fails only when rows exist
        try
          Const(e.eval(Batch.ofRows(1)).get(0), e.ptype)
        catch
          case NonFatal(_) =>
            e
      case _ if e.children.isEmpty =>
        e
      case _ =>
        try
          VExprs.mapChildren(e, apply) match
            case a @ Arith(ArithOp.Add | ArithOp.Sub, l, r, d: PType.Decimal) =>
              a.copy(left = scaled(l, d), right = scaled(r, d))
            case other =>
              other
        catch
          case _: IllegalStateException =>
            e

  /** A numeric constant as a decimal at the scale of `d`, when that is exact and fits */
  private def scaled(e: VExpr, d: PType.Decimal): VExpr =
    e match
      case Const(v, t) if v != null && (t.isIntegral || t.isDecimal) && t != d =>
        try
          val x = ValueOps.toBigDecimal(v).setScale(d.scale)
          if x.precision() <= d.precision then
            Const(x, d)
          else
            e
        catch
          case _: ArithmeticException =>
            e
      case other =>
        other

end FoldConstants
