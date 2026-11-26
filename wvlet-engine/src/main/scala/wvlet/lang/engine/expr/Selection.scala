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
package wvlet.lang.engine.expr

import wvlet.lang.engine.vector.*

/**
  * The rows of a batch for which a filter condition is TRUE, computed with AND / OR short-circuited
  * over the surviving rows (`short_circuit_filters`, DuckDB's `ExpressionExecutor::Select`):
  *
  *   - An AND evaluates each conjunct only on the rows the earlier ones kept, and stops when none
  *     remain.
  *   - An OR evaluates each disjunct only on the rows the earlier ones did not accept.
  *   - Any other expression is evaluated over the rows still selected, gathered from the batch, or
  *     over the whole batch when most rows are still selected (a gather costs about as much as a
  *     cheap comparison).
  *
  * A filter keeps only the TRUE rows, so NULL and FALSE need not be told apart: AND is TRUE where
  * every conjunct is, OR where some disjunct is.
  *
  * With `reorder`, the parts of an AND or an OR run cheapest first (`reorder_conjuncts`, DuckDB's
  * REORDER_FILTER, DataFusion's `reorder_predicates`). A part that can throw (a cast or arithmetic
  * that fails on some values, a row function) is a barrier that no part moves across, so it never
  * sees rows the written order would have removed before it.
  *
  * With `dictionaryPositions` (`dictionary_selection`), a [[PerDictEntry]] part becomes a
  * [[DictLeaf]]: it runs once per dictionary entry of the rows still selected and returns their
  * positions, without a per-row mask over the whole batch, so the later parts of an AND over
  * dictionary-coded strings look at the surviving rows only (TPC-DS q41's eight-way OR of ANDs).
  */
object Selection:

  /** A filter condition prepared for selection */
  sealed trait Node

  /** AND of the parts, in evaluation order */
  case class Conj(parts: IndexedSeq[Node]) extends Node

  /** OR of the parts, in evaluation order */
  case class Disj(parts: IndexedSeq[Node]) extends Node

  /** An expression evaluated as a whole, with its estimated cost per row */
  case class Leaf(expr: VExpr, cost: Int) extends Node

  /**
    * A condition over one dictionary-coded string column, run per dictionary entry of the selected
    * rows ([[DictionaryPredicates.selectPerEntry]]), with the estimated cost per row of a [[Leaf]]
    * of it, for ordering and for the rows without codes
    */
  case class DictLeaf(expr: PerEntryCondition, cost: Int) extends Node

  /** The condition as nested ANDs and ORs over leaves, the parts reordered when `reorder` */
  def compile(cond: VExpr, reorder: Boolean, dictionaryPositions: Boolean = false): Node =
    def conjuncts(e: VExpr): List[VExpr] =
      e match
        case AndExpr(l, r) =>
          conjuncts(l) ++ conjuncts(r)
        case other =>
          List(other)
    def disjuncts(e: VExpr): List[VExpr] =
      e match
        case OrExpr(l, r) =>
          disjuncts(l) ++ disjuncts(r)
        case other =>
          List(other)
    def build(e: VExpr): Node =
      e match
        case _: AndExpr =>
          Conj(order(conjuncts(e).map(build)))
        case _: OrExpr =>
          Disj(order(disjuncts(e).map(build)))
        case p: PerEntryCondition if dictionaryPositions =>
          DictLeaf(p, cost(p))
        case other =>
          Leaf(other, cost(other))
    def order(parts: List[Node]): IndexedSeq[Node] =
      if !reorder then
        parts.toIndexedSeq
      else
        // Stable sort within the runs between parts that can throw, which stay in place
        val out           = IndexedSeq.newBuilder[Node]
        var run           = List.empty[Node]
        def flush(): Unit =
          out ++= run.reverse.sortBy(nodeCost)
          run = Nil
        parts.foreach { p =>
          if canThrow(p) then
            flush()
            out += p
          else
            run = p :: run
        }
        flush()
        out.result()
    build(cond)

  end compile

  private def nodeCost(n: Node): Int =
    n match
      case Leaf(_, c) =>
        c
      case DictLeaf(_, c) =>
        c
      case Conj(ps) =>
        ps.map(nodeCost).sum
      case Disj(ps) =>
        ps.map(nodeCost).sum

  private def canThrow(n: Node): Boolean =
    n match
      case Leaf(e, _) =>
        !isSafe(e)
      case DictLeaf(e, _) =>
        !isSafe(e)
      case Conj(ps) =>
        ps.exists(canThrow)
      case Disj(ps) =>
        ps.exists(canThrow)

  /** Whether evaluating `e` never throws, whatever the input values */
  def isSafe(e: VExpr): Boolean =
    val self =
      e match
        case _: ColRef | _: Const | _: Materialized =>
          true
        case _: Compare | _: NullSafeDistinct | _: IsNullExpr | _: InList | _: LikeExpr |
            _: AndExpr | _: OrExpr | _: NotExpr | _: InExprs | _: Coalesce | _: CaseWhen |
            _: PerEntryCondition =>
          true
        case Arith(op, _, _, PType.Float64) =>
          op == ArithOp.Add || op == ArithOp.Sub || op == ArithOp.Mul
        case CastExpr(child, t, tryCast) =>
          tryCast || child.ptype == t || isWidening(child.ptype, t)
        case _ =>
          // Integer and decimal arithmetic overflows and divides by zero; row functions are opaque
          false
    self && e.children.forall(isSafe)

  private def isWidening(from: PType, to: PType): Boolean =
    (from, to) match
      case (PType.Int32, PType.Int64 | PType.Float64) =>
        true
      case (PType.Int64, PType.Float64) =>
        true
      case (PType.Date, PType.Timestamp) =>
        true
      case _ =>
        false

  /**
    * A static estimate of the cost per row of evaluating `e`: primitive comparisons are cheap,
    * string comparisons, IN lists (boxed lookups), LIKE, and row functions are not
    */
  def cost(e: VExpr): Int =
    def isPrimitive(t: PType): Boolean =
      t match
        case PType.Bool | PType.Int32 | PType.Int64 | PType.Float64 | PType.Date | PType.Timestamp |
            PType.TimestampTz =>
          true
        case _ =>
          false
    val self =
      e match
        case _: ColRef | _: Const | _: Materialized | _: PerEntryCondition =>
          0
        case Compare(_, l, _) =>
          if isPrimitive(l.ptype) then
            1
          else if l.ptype.isDecimal then
            2
          else
            4
        case _: IsNullExpr | _: NotExpr | _: Negate =>
          1
        case a: Arith =>
          if isPrimitive(a.ptype) then
            1
          else
            3
        case _: FusedDecimalMul =>
          // Costs as the decimal Arith it replaces, so that fusing doesn't reorder conjuncts
          3
        case c: CastExpr =>
          if c.ptype == PType.Str || c.child.ptype == PType.Str then
            8
          else
            2
        case _: AndExpr | _: OrExpr =>
          1
        case _: NullSafeDistinct | _: Coalesce =>
          4
        case _: InList =>
          6
        case i: InExprs =>
          2 * i.list.size
        case _: LikeExpr =>
          8
        case _: CaseWhen =>
          10
        case _: RowFn =>
          20
        case _ =>
          10
    self + e.children.map(cost).sum

  end cost

  /**
    * The positions of the rows of `b` for which the condition is TRUE, in ascending order, or null
    * when every row is
    */
  def select(n: Node, b: Batch): Array[Int] = run(n, b, null)

  // The positions within `sel` (all rows of b when null) that the node keeps, or null for all of
  // `sel`. Positions are ascending and never modified after they are returned
  private def run(n: Node, b: Batch, sel: Array[Int]): Array[Int] =
    n match
      case Leaf(e, c) =>
        leaf(e, c, b, sel)
      case DictLeaf(e, c) =>
        dictLeaf(e, c, b, sel)
      case Conj(parts) =>
        var cur = sel
        var i   = 0
        while i < parts.length && (cur == null || cur.length > 0) do
          val r = run(parts(i), b, cur)
          if r != null then
            cur = r
          i += 1
        if cur eq sel then
          null
        else
          cur
      case Disj(parts) =>
        val total            = sizeOf(sel, b.size)
        var accepted         = Array.emptyIntArray
        var rest: Array[Int] = sel
        var i                = 0
        while i < parts.length && sizeOf(rest, b.size) > 0 do
          val r = run(parts(i), b, rest)
          if r == null then
            // Every remaining row is accepted
            accepted = merge(accepted, all(rest, b))
            rest = Array.emptyIntArray
          else if r.length > 0 then
            accepted = merge(accepted, r)
            rest = minus(rest, r, b.size)
          i += 1
        if accepted.length == total then
          null
        else
          accepted

  private def all(sel: Array[Int], b: Batch): Array[Int] =
    if sel == null then
      Array.range(0, b.size)
    else
      sel

  /**
    * The highest cost of a cheap leaf (a comparison of primitive values or strings): a vectorized
    * pass over the whole batch costs less than gathering the selected rows for it
    */
  val CheapCost = 4

  /**
    * Whether selecting is worth it for `n`: some leaf is costly, or an AND has two or more
    * [[DictLeaf]] parts (the later ones run on the surviving rows only). With cheap leaves only,
    * building position arrays costs more than the full-batch masks it saves (TPC-H q6, q10)
    */
  def worthSelecting(n: Node): Boolean =
    n match
      case _: Leaf | _: DictLeaf =>
        false
      case Conj(ps) =>
        ps.count(_.isInstanceOf[DictLeaf]) >= 2 ||
        ps.exists(p => worthSelecting(p) || leafCost(p) > CheapCost)
      case Disj(ps) =>
        ps.exists(p => worthSelecting(p) || leafCost(p) > CheapCost)

  private def leafCost(n: Node): Int =
    n match
      case Leaf(_, c) =>
        c
      case DictLeaf(_, c) =>
        c
      case _ =>
        0

  /**
    * The selected share of rows above which a leaf runs on the whole batch: cheap leaves always do,
    * costly ones gather the selected rows of their columns when few enough remain
    */
  private def denseShare(cost: Int): Double =
    if cost <= CheapCost then
      0.0
    else if cost <= 8 then
      0.5
    else
      0.8

  private def leaf(e: VExpr, c: Int, b: Batch, sel: Array[Int]): Array[Int] =
    if sel == null then
      b.maskPositions(Kernels.trueMask(e.eval(b)))
    else
      val n = sel.length
      if n == 0 then
        sel
      else if n > b.size * denseShare(c) then
        // Most rows are still selected: evaluate over the whole batch and keep the selected ones
        val mask  = Kernels.trueMask(e.eval(b))
        var count = 0
        var i     = 0
        while i < n do
          if mask(sel(i)) then
            count += 1
          i += 1
        keep(sel, count, i => mask(sel(i)))
      else
        val mask  = Kernels.trueMask(e.eval(b.take(sel, n)))
        val count = countTrue(mask, n)
        keep(sel, count, mask(_))

  end leaf

  private def dictLeaf(e: PerEntryCondition, c: Int, b: Batch, sel: Array[Int]): Array[Int] =
    if sel != null && sel.length == 0 then
      sel
    else
      e.selectPositions(b, sel) match
        case null =>
          // No usable codes: as a leaf of the same cost
          leaf(e, c, b, sel)
        case r if r.length == sizeOf(sel, b.size) =>
          null
        case r =>
          r

  private def countTrue(mask: Array[Boolean], n: Int): Int =
    var count = 0
    var i     = 0
    while i < n do
      if mask(i) then
        count += 1
      i += 1
    count

  // The positions of sel at which `kept` holds (`count` of them), or null when all of sel is kept
  private inline def keep(sel: Array[Int], count: Int, inline kept: Int => Boolean): Array[Int] =
    if count == sel.length then
      null
    else
      val out = new Array[Int](count)
      var j   = 0
      var i   = 0
      while i < sel.length do
        if kept(i) then
          out(j) = sel(i)
          j += 1
        i += 1
      out

  /** The union of two disjoint ascending position arrays */
  private def merge(a: Array[Int], b: Array[Int]): Array[Int] =
    if a.length == 0 then
      b
    else if b.length == 0 then
      a
    else
      val out = new Array[Int](a.length + b.length)
      var i   = 0
      var j   = 0
      var k   = 0
      while i < a.length && j < b.length do
        if a(i) < b(j) then
          out(k) = a(i)
          i += 1
        else
          out(k) = b(j)
          j += 1
        k += 1
      while i < a.length do
        out(k) = a(i)
        i += 1
        k += 1
      while j < b.length do
        out(k) = b(j)
        j += 1
        k += 1
      out

  /**
    * The positions of `a` (all of 0 until `size` when null) not in `b`, a subset of it; ascending
    */
  private def minus(a: Array[Int], b: Array[Int], size: Int): Array[Int] =
    val n   = sizeOf(a, size)
    val out = new Array[Int](n - b.length)
    var i   = 0
    var j   = 0
    var k   = 0
    while i < n do
      val p =
        if a == null then
          i
        else
          a(i)
      if j < b.length && p == b(j) then
        j += 1
      else
        out(k) = p
        k += 1
      i += 1
    out

  private def sizeOf(sel: Array[Int], size: Int): Int =
    if sel == null then
      size
    else
      sel.length

end Selection
