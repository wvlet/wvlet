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
package wvlet.lang.engine.exec

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

/**
  * Expressions over the deferred columns of a filter's output, computed over the filter's input
  * rows (`dense_selected_rows`). A filter that keeps most rows (TPC-H q1 keeps 98% of lineitem)
  * hands its columns on as source vectors and the kept positions ([[Batch.take]]). Gathering the
  * kept rows of every column an expression reads costs more than computing the expression over all
  * the source rows, whose result is then deferred at the same positions.
  *
  * Only expressions that cannot fail on a row the filter dropped qualify: column references,
  * constants, and `+` / `-` / `*` of decimal or double values ([[Kernels.arith]] neither throws on
  * them nor divides). Every column the expression reads must be deferred at the same positions, and
  * at least half the source rows must be kept.
  */
private[exec] object DenseSelectedRows:

  /** A value over the source rows of a batch, whose rows are at `sel` (-1 for NULL) */
  final class Dense(val vec: Vec, val sel: Array[Int])

  /**
    * The positions shared by the columns `e` reads when [[eval]] computes it over their source
    * rows, or null when `e` doesn't qualify
    */
  def positions(e: VExpr, b: Batch): Array[Int] =
    if !b.isDeferred || !total(e) then
      return null
    var sel: Array[Int]      = null
    var srcSize              = -1
    var ok                   = true
    def scan(x: VExpr): Unit =
      x match
        case c: ColRef =>
          val src = b.deferredSource(c.index)
          if src == null then
            ok = false
          else
            val s = b.deferredPositions(c.index)
            if sel == null then
              sel = s
              srcSize = src.size
            else if (sel ne s) || src.size != srcSize then
              ok = false
        case _ =>
          x.children.foreach(scan)
    scan(e)
    if !ok || sel == null || b.size.toLong * 2 < srcSize then
      null
    else
      sel

  end positions

  /**
    * The value of `e` over the source rows of b's deferred columns, or null when `e` doesn't
    * qualify ([[positions]]). A column reference is its source vector
    */
  def eval(e: VExpr, b: Batch): Dense =
    val sel = positions(e, b)
    if sel == null then
      null
    else
      Dense(sourceValue(e, b), sel)

  /** The value of qualifying `e` over the source rows */
  private def sourceValue(e: VExpr, b: Batch): Vec =
    e match
      case c: ColRef =>
        b.deferredSource(c.index)
      case _ =>
        val cols                 = new Array[Vec](b.numColumns)
        var size                 = 0
        def fill(x: VExpr): Unit =
          x match
            case c: ColRef =>
              val src = b.deferredSource(c.index)
              cols(c.index) = src
              size = src.size
            case _ =>
              x.children.foreach(fill)
        fill(e)
        e.eval(Batch(cols, size))

  /** The arguments of fused aggregates over the source rows, at `sel` */
  final class FusedArgs(val args: Array[Array[Vec]], val sel: Array[Int])

  /**
    * The arguments of the aggregates with `unfiltered(a)` (null for the others) over the source
    * rows of b, when they all qualify for [[eval]] at the same positions without -1; null otherwise
    */
  def fusedArgs(aggs: IndexedSeq[AggCall], unfiltered: Array[Boolean], b: Batch): FusedArgs =
    if !b.isDeferred then
      return null
    var sel: Array[Int] = null
    var a               = 0
    while a < aggs.size do
      if unfiltered(a) then
        val it = aggs(a).args.iterator
        while it.hasNext do
          val s = positions(it.next(), b)
          if s == null || (sel != null && (sel ne s)) then
            return null
          sel = s
      a += 1
    if sel == null || hasNullRows(sel, b.size) then
      return null
    val args =
      Array.tabulate(aggs.size) { a =>
        if unfiltered(a) then
          aggs(a).args.map(sourceValue(_, b)).toArray
        else
          null
      }
    FusedArgs(args, sel)

  end fusedArgs

  /**
    * Fold the rows of b into acc when its only argument `e` qualifies for [[eval]]: the accumulator
    * reads the value at the positions. False when the argument is evaluated as usual
    */
  def update(acc: Accumulator, e: VExpr, groups: Array[Int], b: Batch): Boolean =
    val d = eval(e, b)
    d != null && acc.updateSelected(groups, b.size, d.vec, d.sel)

  /**
    * The accumulators a fused fold handled (`folded`), and the gathered arguments of the others
    * (null where folded or with a FILTER)
    */
  final class Folded(val args: Array[Array[Vec]], val folded: Array[Boolean])

  /**
    * Fold the rows of b with [[FusedAccumulators]] reading the arguments of the aggregates without
    * FILTER at their positions ([[fusedArgs]]), and the other such aggregates over one argument
    * with [[Accumulator.updateSelected]]. Null when the arguments don't qualify, and nothing is
    * folded
    */
  def fold(
      aggs: IndexedSeq[AggCall],
      unfiltered: Array[Boolean],
      accs: IndexedSeq[Accumulator],
      groups: Array[Int],
      numGroups: Int,
      b: Batch
  ): Folded =
    val dense = fusedArgs(aggs, unfiltered, b)
    if dense == null then
      return null
    val n      = b.size
    val sel    = dense.sel
    var folded = FusedAccumulators.update(accs, unfiltered, dense.args, groups, n, numGroups, sel)
    if folded == null then
      folded = new Array[Boolean](aggs.size)
    val args =
      Array.tabulate(aggs.size) { a =>
        val vs = dense.args(a)
        if vs == null || folded(a) then
          null
        else if vs.length == 1 && accs(a).updateSelected(groups, n, vs(0), sel) then
          folded(a) = true
          null
        else
          vs.map(_.gather(sel, n))
      }
    Folded(args, folded)

  end fold

  /**
    * Group ids of the rows of b: string keys that are deferred columns at the same positions look
    * up their codes there ([[GroupTable.groupIdsAt]]); other keys evaluate as usual
    */
  def groupIds(table: GroupTable, keys: IndexedSeq[VExpr], b: Batch): Array[Int] =
    var sel: Array[Int] = null
    val srcs            = new Array[Vec](keys.size)
    var k               = 0
    while k < keys.size do
      keys(k) match
        case c: ColRef if c.ptype == PType.Str && b.deferredSource(c.index) != null =>
          val s = b.deferredPositions(c.index)
          if sel != null && (sel ne s) then
            return table.groupIds(keys.map(_.eval(b)).toArray, b.size)
          sel = s
          srcs(k) = b.deferredSource(c.index)
        case _ =>
          return table.groupIds(keys.map(_.eval(b)).toArray, b.size)
      k += 1
    if sel == null || hasNullRows(sel, b.size) then
      table.groupIds(keys.map(_.eval(b)).toArray, b.size)
    else
      table.groupIdsAt(srcs, sel, b.size)

  end groupIds

  /**
    * `exprs` over deferred batch b: deferred columns pass through still deferred, and expressions
    * that [[eval]] computes over the source rows are deferred at the same positions. The other
    * expressions evaluate as usual
    */
  def project(exprs: IndexedSeq[VExpr], b: Batch): Batch =
    val m                       = exprs.size
    val cols                    = new Array[Vec](m)
    var srcs: Array[Vec]        = null
    var sels: Array[Array[Int]] = null
    var i                       = 0
    while i < m do
      val d =
        exprs(i) match
          case c: ColRef =>
            val src = b.deferredSource(c.index)
            if src == null then
              null
            else
              Dense(src, b.deferredPositions(c.index))
          case e =>
            eval(e, b)
      if d == null then
        cols(i) = exprs(i).eval(b)
      else
        if srcs == null then
          srcs = new Array[Vec](m)
          sels = new Array[Array[Int]](m)
        srcs(i) = d.vec
        sels(i) = d.sel
      i += 1
    if srcs == null then
      Batch(cols, b.size)
    else
      Batch.withDeferred(cols, srcs, sels, b.size)

  end project

  /** Whether `e` evaluates on any row without failing */
  private def total(e: VExpr): Boolean =
    e match
      case _: ColRef =>
        true
      case _: Const =>
        true
      case a: Arith =>
        (a.op == ArithOp.Add || a.op == ArithOp.Sub || a.op == ArithOp.Mul) &&
        (a.ptype.isDecimal || a.ptype == PType.Float64) && operand(a.left.ptype, a.ptype) &&
        operand(a.right.ptype, a.ptype) && total(a.left) && total(a.right)
      case _ =>
        false

  /**
    * Whether a value of type t is an operand of arithmetic of type `out` that never fails: a double
    * operand of decimal arithmetic converts to BigDecimal, which throws on NaN and infinities
    */
  private def operand(t: PType, out: PType): Boolean =
    t match
      case PType.Int32 | PType.Int64 | PType.Decimal(_, _) =>
        true
      case PType.Float64 =>
        out == PType.Float64
      case _ =>
        false

  /** Whether some position of `sel` (its first n) is -1 */
  def hasNullRows(sel: Array[Int], n: Int): Boolean =
    var i = 0
    while i < n do
      if sel(i) < 0 then
        return true
      i += 1
    false

end DenseSelectedRows
