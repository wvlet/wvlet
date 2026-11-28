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

import wvlet.lang.compiler.QueryCancel
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

import scala.collection.mutable

/**
  * A pull-based physical operator. `next()` returns the next non-empty batch, or null at the end
  */
trait Operator:
  def schema: Schema
  def next(): Batch
  def children: Seq[Operator] = Nil
  def close(): Unit           = children.foreach(_.close())
  def name: String            = getClass.getSimpleName.stripSuffix("Op")

  /**
    * The physical plan for EXPLAIN, in flow style as the logical plan printer shows it: data flows
    * down, from the scans to this operator. The first child continues the flow (`↓ op`); the other
    * inputs (join build sides, shared CTE bodies) are nested above the operator that reads them (`↙
    * op`). Adding an operator to a plan thus adds one line instead of re-indenting its inputs
    */
  def explain(indent: Int = 0): String =
    val self =
      s"${name}${details} -> [${schema.fields.map(f => s"${f.name}:${f.ptype}").mkString(", ")}]"
    val inputs = children
    val flow   = inputs.headOption.map(_.explain(indent))
    val nested = inputs.drop(1).map(_.explain(indent + 1)) ++ extraInputs(indent + 1)
    val marker =
      if nested.nonEmpty then
        "↙ "
      else if flow.nonEmpty then
        "↓ "
      else
        ""
    (flow.toSeq ++ nested :+ s"${"  " * indent}${marker}${self}").mkString("\n")

  /**
    * The EXPLAIN text, at `indent`, of inputs that are not children (e.g. a shared CTE body that
    * runs once for every reader)
    */
  protected def extraInputs(indent: Int): Seq[String] = Nil

  /**
    * Inputs that are not children because they are computed once for several readers (e.g. the
    * right input of a parallel cross join), so that a walk over a plan can reach every operator
    */
  def sharedInputs: Seq[Operator] = Nil

  protected def details: String = ""

  /** Drain every remaining batch, compacted (see [[Batch]]) */
  def drain(): Seq[Batch] =
    val b = Seq.newBuilder[Batch]
    var x = next()
    while x != null do
      b += x.compact()
      x = next()
    b.result()

end Operator

object Operator:
  /** EXPLAIN marker of a filter or join that gathers its output eagerly (`late_gather` disabled) */
  private[exec] def eagerTag(lateGather: Boolean): String =
    if lateGather then
      ""
    else
      ", eager"

  /** Pull every row of the operator into one batch */
  def materialize(op: Operator): Batch = MemoryContext.NoOp.materialize(op)

/** Batches computed up front (VALUES, materialized subqueries, in-memory tables) */
class BatchesOp(val schema: Schema, batches: Seq[Batch], label: String = "Batches")
    extends Operator:
  private val it    = batches.iterator.filter(_.size > 0)
  def next(): Batch =
    if it.hasNext then
      QueryCancel.check() // Ctrl-C in the npm REPL (see ScanOp.next)
      it.next()
    else
      null

  override def name: String = label

/** Batches produced lazily (file scans) */
class IteratorOp(val schema: Schema, mk: () => Iterator[Batch], label: String, val rows: Long = -1L)
    extends Operator:
  private lazy val it       = mk()
  override def name: String = label
  def next(): Batch         =
    var b: Batch = null
    while b == null && it.hasNext do
      QueryCancel.check()
      val x = it.next()
      if x.size > 0 then
        b = x
    b

/** One row without columns: the input of a FROM-less SELECT */
class SingleRowOp extends Operator:
  private var done   = false
  val schema: Schema = Schema.empty
  def next(): Batch  =
    if done then
      null
    else
      done = true
      Batch.ofRows(1)

/**
  * Rows of the input where `cond` is true. With `keep`, only those input columns are output (in
  * that order), so columns only the condition reads aren't copied
  */
class FilterOp(val child: Operator, val cond: VExpr, val keep: Array[Int] = null) extends Operator:
  /** Whether the output columns are deferred instead of gathered (`late_gather`) */
  var lateGather: Boolean = true

  def withLateGather(enabled: Boolean): this.type =
    lateGather = enabled
    this

  /**
    * Whether AND / OR conditions run each part only on the surviving rows (`short_circuit_filters`)
    */
  var shortCircuit: Boolean = true

  /** Whether the parts of AND / OR run cheapest first (`reorder_conjuncts`) */
  var reorderConjuncts: Boolean = true

  /**
    * Whether parts of the condition over one dictionary-coded string column run once per dictionary
    * entry (`dictionary_filters`, [[DictionaryPredicates]])
    */
  var dictionaryFilters: Boolean = true

  /**
    * Whether per-entry parts of an AND / OR select positions over the surviving rows only
    * (`dictionary_selection`, [[Selection.DictLeaf]])
    */
  var dictionarySelection: Boolean = true

  /** The evaluation settings of `f`, for a copy of it over another input */
  def withSettingsOf(f: FilterOp): this.type =
    lateGather = f.lateGather
    shortCircuit = f.shortCircuit
    reorderConjuncts = f.reorderConjuncts
    dictionaryFilters = f.dictionaryFilters
    dictionarySelection = f.dictionarySelection
    this

  // The condition as evaluated, prepared on the first batch after the settings are final
  private lazy val evalCond: VExpr =
    if dictionaryFilters then
      DictionaryPredicates.wrap(cond, whole = dictionarySelection && shortCircuit)
    else
      cond

  // Prepared on the first batch, after the settings are final
  private lazy val selection: Selection.Node =
    if shortCircuit then
      val node = Selection.compile(evalCond, reorderConjuncts, dictionarySelection)
      if Selection.worthSelecting(node) then
        node
      else
        null
    else
      null

  val schema: Schema =
    if keep == null then
      child.schema
    else
      Schema(keep.toIndexedSeq.map(child.schema(_)))

  override def children: Seq[Operator] = Seq(child)
  override protected def details       =
    s"(${cond.sig}${
        if keep == null then
          ""
        else
          s", keep=${keep.mkString(",")}"
      }${Operator.eagerTag(lateGather)}${selectionTag})"

  // Marks a disabled short_circuit_filters / reorder_conjuncts on an AND / OR condition
  private def selectionTag: String =
    cond match
      case _: AndExpr | _: OrExpr if !shortCircuit =>
        ", full_eval"
      case _: AndExpr | _: OrExpr if !reorderConjuncts =>
        ", written_order"
      case _ =>
        ""

  def next(): Batch =
    var out: Batch = null
    while out == null do
      val b = child.next()
      if b == null then
        return null
      val in =
        if keep == null then
          b
        else
          b.select(keep)
      val r =
        selection match
          case null =>
            val mask = Kernels.trueMask(evalCond.eval(b))
            if lateGather then
              in.takeWhere(mask)
            else
              in.filter(mask)
          case node =>
            val idx = Selection.select(node, b)
            if idx == null then
              in
            else if lateGather then
              in.take(idx, idx.length)
            else
              in.gather(idx, idx.length)
      if r.size > 0 then
        out = r
    out

  end next

end FilterOp

class ProjectOp(val child: Operator, val exprs: IndexedSeq[VExpr], val schema: Schema)
    extends Operator:
  override def children: Seq[Operator] = Seq(child)
  override protected def details       = s"(${exprs.map(_.sig).mkString(", ")})"

  /**
    * Whether columns of a deferred input pass through deferred, and expressions over them compute
    * over their source rows when [[DenseSelectedRows]] allows (`dense_selected_rows`)
    */
  var denseSelected: Boolean = true

  /** Set [[denseSelected]] */
  def withDenseSelected(enabled: Boolean): this.type =
    denseSelected = enabled
    this

  def next(): Batch =
    val b = child.next()
    if b == null then
      null
    else if denseSelected && b.isDeferred then
      DenseSelectedRows.project(exprs, b)
    else
      Batch(exprs.map(_.eval(b)).toArray, b.size)

class LimitOp(val child: Operator, val limit: Long, val offset: Long) extends Operator:
  private var skipped                  = 0L
  private var emitted                  = 0L
  def schema: Schema                   = child.schema
  override def children: Seq[Operator] = Seq(child)
  override protected def details       = s"(${limit}, offset=${offset})"
  def next(): Batch                    =
    var out: Batch = null
    while out == null && emitted < limit do
      val b = child.next()
      if b == null then
        return null
      var from = 0
      if skipped < offset then
        val skip = math.min(offset - skipped, b.size.toLong).toInt
        skipped += skip
        from = skip
      if from < b.size then
        val take = math.min(limit - emitted, (b.size - from).toLong).toInt
        emitted += take
        out = b.slice(from, from + take)
    out

case class SortKey(expr: VExpr, ascending: Boolean, nullsFirst: Boolean)

/**
  * Full sort; with a limit only the top rows are kept. Inputs of at least `minParallelRows` rows
  * are sorted in parallel on `runner`: each of `runner.parallelism` contiguous chunks of the input
  * is sorted (and cut to the limit) as a task. The merge of the sorted runs is split at evenly
  * spaced output ranks ([[SortOp.splitRanks]]), and each range is merged and gathered as a task.
  * Ties in the merge go to the earlier chunk, so the output equals the sequential (stable) sort.
  */
class SortOp(
    val child: Operator,
    val keys: Seq[SortKey],
    val limit: Option[Long] = None,
    runner: TaskRunner = TaskRunner.Sequential,
    minParallelRows: Int = SortOp.MinParallelRows
) extends Operator:
  private var result: Iterator[Batch]  = null
  def schema: Schema                   = child.schema
  override def children: Seq[Operator] = Seq(child)
  override def name: String            =
    if runner.parallelism > 1 then
      s"Sort[parallelism=${runner.parallelism}]"
    else
      "Sort"

  override protected def details =
    s"(${keys
        .map(k =>
          s"${k.expr.sig}${
              if k.ascending then
                ""
              else
                " desc"
            }"
        )
        .mkString(", ")}${limit.map(l => s", limit=${l}").getOrElse("")})"

  def next(): Batch =
    if result == null then
      result = sortAll()
    if result.hasNext then
      result.next()
    else
      null

  private val memory = MemoryContext.current // captured where the sort is planned
  private def sortAll(): Iterator[Batch] =
    val all = memory.materialize(child)
    if all.size == 0 then
      return Iterator.empty
    val keyVecs = keys.map(k => k.expr.eval(all)).toArray
    val cmp     = SortOp.comparator(keyVecs, keys)
    val n       = limit.map(l => math.min(l, all.size.toLong).toInt).getOrElse(all.size)
    if all.size < minParallelRows || runner.parallelism <= 1 then
      val sorted = SortOp.sortRange(cmp, 0, all.size, n)
      val out    = all.gather(sorted, n)
      Iterator.range(0, n, Batch.DefaultSize).map(from => out.slice(from, from + Batch.DefaultSize))
    else
      val chunks = math.min(runner.parallelism, all.size)
      val runs   = (0 until chunks).map { c =>
        val lo = (all.size.toLong * c / chunks).toInt
        val hi = (all.size.toLong * (c + 1) / chunks).toInt
        runner.submit(() => SortOp.sortRange(cmp, lo, hi, n))
      }
      val sortedRuns = runs.map(_.get()).toArray
      // Split the merge into independent output ranges (one task each) that merge and gather
      val ranges = SortOp.mergeRangeCount(n, chunks, minParallelRows)
      val cuts   = SortOp.splitRanks(sortedRuns, cmp, n, ranges)
      val tasks  = (0 until ranges).map { t =>
        runner.submit { () =>
          val part = SortOp.mergeRange(sortedRuns, cmp, cuts(t), cuts(t + 1))
          Iterator
            .range(0, part.length, Batch.DefaultSize)
            .map { from =>
              val to = math.min(part.length, from + Batch.DefaultSize)
              all.gather(java.util.Arrays.copyOfRange(part, from, to), to - from)
            }
            .toIndexedSeq
        }
      }
      tasks.iterator.flatMap(_.get())
    end if

  end sortAll

end SortOp

object SortOp:
  /**
    * Below this many input rows a sort runs on the calling thread. The
    * wvlet.engine.parallelSortMinRows system property overrides it (tests use it to exercise the
    * parallel path on small data)
    */
  def MinParallelRows: Int = sys
    .props
    .get("wvlet.engine.parallelSortMinRows")
    .flatMap(_.toIntOption)
    .getOrElse(65536)

  /**
    * Row indexes lo until hi in (stable) sorted order, cut to the first `limit`. A limit of at most
    * 1/8 of the range keeps a bounded heap of the top rows instead of sorting the whole range
    */
  def sortRange(cmp: SortOp.RowComparator, lo: Int, hi: Int, limit: Int): Array[Int] =
    if limit <= 0 || hi <= lo then
      Array.emptyIntArray
    else if limit.toLong * 8 <= hi - lo then
      topRange(cmp, lo, hi, limit)
    else
      val idx = new Array[Int](hi - lo)
      var i   = 0
      while i < idx.length do
        idx(i) = lo + i
        i += 1
      IndexSort.sort(idx, cmp)
      if limit < idx.length then
        java.util.Arrays.copyOf(idx, limit)
      else
        idx

  /**
    * The first `limit` (< hi - lo) row indexes of lo until hi in stable sorted order, using a
    * max-heap of the `limit` smallest rows by (key, row index). A later row replaces the heap top
    * only when its key is strictly smaller, so ties keep the input order
    */
  private[exec] def topRange(cmp: SortOp.RowComparator, lo: Int, hi: Int, limit: Int): Array[Int] =
    val heap = new Array[Int](limit)
    var size = limit
    // a ranks after b in the stable order
    def after(a: Int, b: Int): Boolean =
      val c = cmp.compareRows(a, b)
      c > 0 || (c == 0 && a > b)
    def siftDown(start: Int): Unit =
      var i    = start
      var done = false
      while !done do
        val l = 2 * i + 1
        if l >= size then
          done = true
        else
          val r = l + 1
          val m =
            if r < size && after(heap(r), heap(l)) then
              r
            else
              l
          if after(heap(m), heap(i)) then
            val t = heap(i)
            heap(i) = heap(m)
            heap(m) = t
            i = m
          else
            done = true
    var i = 0
    while i < limit do
      heap(i) = lo + i
      i += 1
    var k = size / 2 - 1
    while k >= 0 do
      siftDown(k)
      k -= 1
    var j = lo + limit
    while j < hi do
      if cmp.compareRows(j, heap(0)) < 0 then
        heap(0) = j
        siftDown(0)
      j += 1
    // Pop the largest remaining row into the back of the output
    val out = new Array[Int](limit)
    var o   = limit - 1
    while o >= 0 do
      out(o) = heap(0)
      size -= 1
      heap(0) = heap(size)
      siftDown(0)
      o -= 1
    out

  end topRange

  /**
    * The number of independent output ranges to split the merge of `n` rows into: one (a merge on
    * the calling thread's task) below `minParallelRows` rows, else one per chunk
    */
  def mergeRangeCount(n: Int, chunks: Int, minParallelRows: Int): Int =
    if n < minParallelRows then
      1
    else
      math.max(1, math.min(chunks, n))

  /**
    * Cuts of sorted runs at `ranges + 1` evenly spaced ranks 0 .. n of their merge: `cuts(t)(r)` is
    * how many rows of run r come before rank `n * t / ranges`. Rows are ordered by (key, run,
    * position), so ties go to the earlier run as in [[merge]]
    */
  def splitRanks(
      runs: Array[Array[Int]],
      cmp: SortOp.RowComparator,
      n: Int,
      ranges: Int
  ): Array[Array[Int]] =
    val cuts = new Array[Array[Int]](ranges + 1)
    var t    = 0
    while t <= ranges do
      cuts(t) = cutAt(runs, cmp, (n.toLong * t / ranges).toInt)
      t += 1
    cuts

  /** How many rows of each run come before rank `k` of the merge of sorted `runs` */
  def cutAt(runs: Array[Array[Int]], cmp: SortOp.RowComparator, k: Int): Array[Int] =
    val p   = runs.length
    val lo  = new Array[Int](p)
    val hi  = new Array[Int](p)
    val cnt = new Array[Int](p)
    var j   = 0
    while j < p do
      hi(j) = runs(j).length
      j += 1
    // Invariant: lo(j) <= cut(j) <= hi(j). Each step halves the widest range with a pivot
    var done = false
    while !done do
      var r = 0
      j = 1
      while j < p do
        if hi(j) - lo(j) > hi(r) - lo(r) then
          r = j
        j += 1
      if hi(r) == lo(r) then
        done = true
      else
        val m = (lo(r) + hi(r)) >>> 1
        val x = runs(r)(m)
        // Rank of the pivot: rows ordered before (x, r, m) in each run
        var rank = 0L
        j = 0
        while j < p do
          cnt(j) =
            if j == r then
              m
            else
              countBefore(runs(j), cmp, x, inclusive = j < r)
          rank += cnt(j)
          j += 1
        if rank < k then
          // The pivot is among the first k rows, so are all rows before it
          j = 0
          while j < p do
            lo(j) = math.max(lo(j), cnt(j))
            j += 1
          lo(r) = m + 1
        else
          j = 0
          while j < p do
            hi(j) = math.min(hi(j), cnt(j))
            j += 1
        end if
      end if
    end while
    lo

  end cutAt

  /** Rows of a sorted run with a key below `x` (or at most `x` when `inclusive`) */
  private def countBefore(
      run: Array[Int],
      cmp: SortOp.RowComparator,
      x: Int,
      inclusive: Boolean
  ): Int =
    var lo = 0
    var hi = run.length
    while lo < hi do
      val mid = (lo + hi) >>> 1
      val c   = cmp.compareRows(run(mid), x)
      if c < 0 || (inclusive && c == 0) then
        lo = mid + 1
      else
        hi = mid
    lo

  /**
    * The first `limit` rows of sorted runs, merged with a binary heap of the runs' heads. Ties go
    * to the earlier run, so merging stable sorts of consecutive chunks gives the stable sort of all
    */
  def merge(runs: Array[Array[Int]], cmp: SortOp.RowComparator, limit: Int): Array[Int] =
    val to = new Array[Int](runs.length)
    var r  = 0
    while r < runs.length do
      to(r) = runs(r).length
      r += 1
    mergeRange(runs, cmp, new Array[Int](runs.length), to, limit)

  /**
    * Merges the rows `from(r)` until `to(r)` of each sorted run r (up to `limit` rows), ties going
    * to the earlier run
    */
  def mergeRange(
      runs: Array[Array[Int]],
      cmp: SortOp.RowComparator,
      from: Array[Int],
      to: Array[Int],
      limit: Int = Int.MaxValue
  ): Array[Int] =
    var total = 0L
    var r     = 0
    while r < runs.length do
      total += to(r) - from(r)
      r += 1
    val out = new Array[Int](math.min(limit.toLong, total).toInt)
    val pos = from.clone()
    // Min-heap of run ids by their current head row
    val heap                          = new Array[Int](runs.length)
    var size                          = 0
    def less(a: Int, b: Int): Boolean =
      val c = cmp.compareRows(runs(a)(pos(a)), runs(b)(pos(b)))
      c < 0 || (c == 0 && a < b)
    def siftDown(start: Int): Unit =
      var i    = start
      var done = false
      while !done do
        val l = 2 * i + 1
        if l >= size then
          done = true
        else
          val r = l + 1
          val m =
            if r < size && less(heap(r), heap(l)) then
              r
            else
              l
          if less(heap(m), heap(i)) then
            val t = heap(i)
            heap(i) = heap(m)
            heap(m) = t
            i = m
          else
            done = true
    r = 0
    while r < runs.length do
      if pos(r) < to(r) then
        heap(size) = r
        size += 1
      r += 1
    var k = size / 2 - 1
    while k >= 0 do
      siftDown(k)
      k -= 1
    var o = 0
    while o < out.length do
      val top = heap(0)
      out(o) = runs(top)(pos(top))
      o += 1
      pos(top) += 1
      if pos(top) == to(top) then
        size -= 1
        heap(0) = heap(size)
      siftDown(0)
    out

  end mergeRange

  /** Compares rows by index; `compareRows` avoids boxing the indexes */
  abstract class RowComparator extends java.util.Comparator[Integer]:
    def compareRows(i: Int, j: Int): Int
    final def compare(a: Integer, b: Integer): Int = compareRows(a.intValue(), b.intValue())

  def comparator(keyVecs: Array[Vec], keys: Seq[SortKey]): RowComparator =
    val asc        = keys.map(_.ascending).toArray
    val nullsFirst = keys.map(_.nullsFirst).toArray
    new RowComparator:
      def compareRows(i: Int, j: Int): Int =
        var c = 0
        var k = 0
        while c == 0 && k < keyVecs.length do
          val v  = keyVecs(k)
          val ni = v.isNull(i)
          val nj = v.isNull(j)
          c =
            if ni && nj then
              0
            else if ni then
              if nullsFirst(k) then
                -1
              else
                1
            else if nj then
              if nullsFirst(k) then
                1
              else
                -1
            else
              val r =
                v match
                  case l: LongVec =>
                    java.lang.Long.compare(l.values(i), l.values(j))
                  case x: IntVec =>
                    Integer.compare(x.values(i), x.values(j))
                  case d: DoubleVec =>
                    java.lang.Double.compare(d.values(i), d.values(j))
                  case s: StrVec =>
                    s.values(i).compareTo(s.values(j))
                  case other =>
                    ValueOps.compare(other.get(i), other.get(j))
              if asc(k) then
                r
              else
                -r
          k += 1
        end while
        c
      end compareRows
    end new

  end comparator

end SortOp

/** Concatenation of inputs with the same column count; values are cast to the output types */
class UnionAllOp(val inputs: Seq[Operator], val schema: Schema) extends Operator:
  private var current                  = 0
  override def children: Seq[Operator] = inputs
  def next(): Batch                    =
    while current < inputs.size do
      val b = inputs(current).next()
      if b != null then
        return Batch(
          b.columns.zipWithIndex.map((v, i) => Kernels.castTo(v, schema(i).ptype)),
          b.size
        )
      current += 1
    null

/** Removes duplicate rows */
class DistinctOp(val child: Operator) extends Operator:
  private var table: GroupIndex = null

  /** Whether several integer, date, or decimal columns group by one packed key */
  var packKeys: Boolean = true

  /** Whether decimal columns are grouped by their unscaled Long values */
  var longDecimalKeys: Boolean = true

  /** Set [[packKeys]] and [[longDecimalKeys]] */
  def withKeyOptions(pack: Boolean, longDecimals: Boolean): this.type =
    packKeys = pack
    longDecimalKeys = longDecimals
    this

  def schema: Schema                   = child.schema
  override def children: Seq[Operator] = Seq(child)
  def next(): Batch                    =
    var out: Batch = null
    while out == null do
      val b = child.next()
      if b == null then
        return null
      if table == null then
        table = PackedGroupTable.forKeys(
          child.schema.types,
          0,
          packKeys,
          longDecimalKeys,
          stableCodes = true
        )
      val before = table.numGroups
      val ids    = table.groupIds(b.columns, b.size)
      // Keep the first row of every new group
      val keep = new Array[Int](b.size)
      var k    = 0
      var next = before
      var i    = 0
      while i < b.size do
        if ids(i) == next then
          keep(k) = i
          k += 1
          next += 1
        i += 1
      if k > 0 then
        out = b.gather(keep, k)
    out

  end next

end DistinctOp

/** INTERSECT / EXCEPT with set (distinct) or bag (ALL) semantics */
class SetOpOp(
    val left: Operator,
    val right: Operator,
    val isIntersect: Boolean,
    val all: Boolean,
    val schema: Schema
) extends Operator:
  private var done                     = false
  override def children: Seq[Operator] = Seq(left, right)
  override def name: String            =
    if isIntersect then
      "Intersect"
    else
      "Except"

  def next(): Batch =
    if done then
      return null
    done = true
    val counts = mutable.HashMap.empty[AnyRef, Int]
    var b      = right.next()
    while b != null do
      val cols = b.columns.zipWithIndex.map((v, i) => Kernels.castTo(v, schema(i).ptype))
      var i    = 0
      while i < b.size do
        val k = GroupKey.of(cols, i)
        counts(k) = counts.getOrElse(k, 0) + 1
        i += 1
      b = right.next()
    val emitted = mutable.HashMap.empty[AnyRef, Int]
    val out     = schema.types.map(t => VecBuilder(t)).toArray
    var lb      = left.next()
    while lb != null do
      val cols = lb.columns.zipWithIndex.map((v, i) => Kernels.castTo(v, schema(i).ptype))
      var i    = 0
      while i < lb.size do
        val k       = GroupKey.of(cols, i)
        val inRight = counts.getOrElse(k, 0)
        val already = emitted.getOrElse(k, 0)
        val emit    =
          if isIntersect then
            if all then
              already < inRight
            else
              inRight > 0 && already == 0
          else if all then
            // EXCEPT ALL: emit occurrences beyond the right count
            val seenLeft = emitted.getOrElse(k, 0)
            emitted(k) = seenLeft + 1
            seenLeft + 1 > inRight
          else
            inRight == 0 && already == 0
        if emit then
          if isIntersect || !all then
            emitted(k) = already + 1
          var c = 0
          while c < cols.length do
            out(c).add(cols(c).get(i))
            c += 1
        else if isIntersect && !all then
          ()
        i += 1
      lb = left.next()
    end while
    val vecs = out.map(_.result())
    val n    =
      if vecs.isEmpty then
        0
      else
        vecs(0).size
    if n == 0 then
      null
    else
      Batch(vecs, n)

  end next

end SetOpOp

enum JoinKind:
  case Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Cross,
    /**
      * Build (right) rows with a match in the probe input, each once: a semi join with swapped
      * inputs
      */
    RightSemi,
    /** Build (right) rows without a match in the probe input: an anti join with swapped inputs */
    RightAnti

  /** Whether the join tracks which build rows matched, and emits build rows at the end */
  def tracksBuildMatches: Boolean =
    this match
      case Right | Full | RightSemi | RightAnti =>
        true
      case _ =>
        false

  /** Whether the output has only the build (right) columns */
  def outputsBuildOnly: Boolean = this == RightSemi || this == RightAnti

  /** Whether the output has only the probe (left) columns */
  def outputsProbeOnly: Boolean = this == Semi || this == Anti

/** Nested-loop join for non-equi conditions and cross joins (right input materialized) */
class NestedLoopJoinOp(
    val left: Operator,
    val right: Operator,
    val cond: Option[VExpr],
    val kind: JoinKind,
    val schema: Schema
) extends Operator:
  override def children: Seq[Operator]     = Seq(left, right)
  override def name: String                = s"NestedLoopJoin[${kind}]"
  override protected def details           = cond.map(c => s"(${c.sig})").getOrElse("")
  private var build: Batch                 = null
  private var buildMatched: Array[Boolean] = null
  private var emittedUnmatched             = false

  def next(): Batch =
    if build == null then
      build = Operator.materialize(right)
      if kind == JoinKind.Right || kind == JoinKind.Full then
        buildMatched = new Array[Boolean](build.size)
    var out: Batch = null
    while out == null do
      val probe = left.next()
      if probe == null then
        return emitUnmatchedBuild()
      out = probeBatch(probe)
    out

  private def emitUnmatchedBuild(): Batch =
    if buildMatched == null || emittedUnmatched then
      return null
    emittedUnmatched = true
    val idx = HashJoinOp.positionsOf(buildMatched, build.size, false)
    if idx.isEmpty then
      return null
    val n       = idx.length
    val leftPad = left.schema.types.map(t => Vec.nullVec(t, n)).toArray
    Batch(leftPad ++ build.gather(idx, n).columns, n)

  private def probeBatch(probe: Batch): Batch =
    val nl = probe.size
    val nr = build.size
    // Process the cross product in chunks of left rows to bound memory
    val li    = mutable.ArrayBuilder.make[Int]
    val ri    = mutable.ArrayBuilder.make[Int]
    val chunk = math.max(1, 65536 / math.max(1, nr))
    var start = 0
    while start < nl do
      val end = math.min(nl, start + chunk)
      val n   = (end - start) * nr
      if n > 0 then
        val lIdx = new Array[Int](n)
        val rIdx = new Array[Int](n)
        var k    = 0
        var i    = start
        while i < end do
          var j = 0
          while j < nr do
            lIdx(k) = i
            rIdx(k) = j
            k += 1
            j += 1
          i += 1
        cond match
          case Some(c) =>
            val joined = Batch(probe.gather(lIdx, n).columns ++ build.gather(rIdx, n).columns, n)
            val mask   = Kernels.trueMask(c.eval(joined))
            var x      = 0
            while x < n do
              if mask(x) then
                li += lIdx(x)
                ri += rIdx(x)
              x += 1
          case None =>
            li ++= lIdx
            ri ++= rIdx
      start = end
    end while
    val lIdx = li.result()
    val rIdx = ri.result()
    kind match
      case JoinKind.Inner | JoinKind.Cross | JoinKind.Right =>
        if buildMatched != null then
          rIdx.foreach(r => buildMatched(r) = true)
        if lIdx.isEmpty then
          null
        else
          Batch(
            probe.gather(lIdx, lIdx.length).columns ++ build.gather(rIdx, rIdx.length).columns,
            lIdx.length
          )
      case JoinKind.Left | JoinKind.Full =>
        if buildMatched != null then
          rIdx.foreach(r => buildMatched(r) = true)
        val matched = new Array[Boolean](nl)
        lIdx.foreach(l => matched(l) = true)
        val unmatched = (0 until nl).filter(!matched(_)).toArray
        val allL      = lIdx ++ unmatched
        val allR      = rIdx ++ Vec.filledInts(unmatched.length, -1)
        if allL.isEmpty then
          null
        else
          Batch(
            probe.gather(allL, allL.length).columns ++ build.gather(allR, allR.length).columns,
            allL.length
          )
      case JoinKind.Semi =>
        val matched = new Array[Boolean](nl)
        lIdx.foreach(l => matched(l) = true)
        val r = probe.filter(matched)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.Anti =>
        val keep = Vec.allTrue(nl)
        lIdx.foreach(l => keep(l) = false)
        val r = probe.filter(keep)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.RightSemi | JoinKind.RightAnti =>
        // Only hash joins swap semi / anti inputs
        throw IllegalStateException(s"Unsupported nested-loop join kind: ${kind}")
    end match

  end probeBatch

end NestedLoopJoinOp
