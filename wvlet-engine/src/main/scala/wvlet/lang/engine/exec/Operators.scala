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
  def materialize(op: Operator): Batch =
    val batches = op.drain()
    if batches.isEmpty then
      Batch.empty(op.schema)
    else
      Batch.concat(op.schema, batches)

/** Batches computed up front (VALUES, materialized subqueries, in-memory tables) */
class BatchesOp(val schema: Schema, batches: Seq[Batch], label: String = "Batches")
    extends Operator:
  private val it    = batches.iterator.filter(_.size > 0)
  def next(): Batch =
    if it.hasNext then
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

  /** The evaluation settings of `f`, for a copy of it over another input */
  def withSettingsOf(f: FilterOp): this.type =
    lateGather = f.lateGather
    shortCircuit = f.shortCircuit
    reorderConjuncts = f.reorderConjuncts
    dictionaryFilters = f.dictionaryFilters
    this

  // The condition as evaluated, prepared on the first batch after the settings are final
  private lazy val evalCond: VExpr =
    if dictionaryFilters then
      DictionaryPredicates.wrap(cond)
    else
      cond

  // Prepared on the first batch, after the settings are final
  private lazy val selection: Selection.Node =
    if shortCircuit then
      val node = Selection.compile(evalCond, reorderConjuncts)
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
  def next(): Batch                    =
    val b = child.next()
    if b == null then
      null
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

  private def sortAll(): Iterator[Batch] =
    val all = Operator.materialize(child)
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

/**
  * Hash aggregation. Output columns: grouping keys, then one column per aggregate. Without keys,
  * exactly one row is produced, even for an empty input.
  *
  * `expectedGroups` (0 when unknown) pre-sizes the group table and the aggregate state, so they
  * grow less often; it never changes the result. `onGroups`, when given, receives the number of
  * groups once the input is consumed (or once grouping stops).
  *
  * With `skip` (the partial aggregate of a two-phase aggregation, whose output is aggregated
  * again), grouping stops when [[PartialAggSkip]] says the groups don't reduce the rows: the groups
  * so far are emitted, then each remaining input batch as one-row partial results (a key may appear
  * in several output rows).
  */
class HashAggOp(
    val child: Operator,
    val keys: IndexedSeq[VExpr],
    val aggs: IndexedSeq[AggCall],
    val schema: Schema,
    val expectedGroups: Int = 0,
    onGroups: Int => Unit = null,
    val skip: PartialAggSkip = null
) extends Operator:
  private var done                     = false
  private var passing                  = false
  override def children: Seq[Operator] = Seq(child)
  override protected def details       =
    s"(keys=[${keys.map(_.sig).mkString(", ")}], aggs=[${aggs.map(_.sig).mkString(", ")}])"

  /**
    * Whether several integer, date, or decimal keys group by one packed key
    * (`stream_pack_agg_keys`)
    */
  var packKeys: Boolean = true

  /** Whether decimal keys are grouped by their unscaled Long values (`long_decimal_keys`) */
  var longDecimalKeys: Boolean = true

  /** Set [[packKeys]] and [[longDecimalKeys]] */
  def withKeyOptions(pack: Boolean, longDecimals: Boolean): this.type =
    packKeys = pack
    longDecimalKeys = longDecimals
    this

  /**
    * Whether counts and decimal sums are folded in one pass over each batch ([[FusedAccumulators]],
    * `fused_accumulators`)
    */
  var fuseAccumulators: Boolean = true

  /** Set [[fuseAccumulators]] */
  def withFusedAccumulators(enabled: Boolean): this.type =
    fuseAccumulators = enabled
    this

  // Aggregates without FILTER, which FusedAccumulators may fold
  private lazy val unfiltered: Array[Boolean] = aggs.map(_.filter.isEmpty).toArray

  // Scratch arrays reused across batches: accumulators read group ids only during update
  private var idx: Array[Int] = Array.emptyIntArray
  private var gs: Array[Int]  = Array.emptyIntArray
  // 0, 1, 2, ...: each passed-through row is its own group
  private var rowIds: Array[Int] = Array.emptyIntArray

  /** Folds the rows of b, in groups `groups`, into `accs` sized for `numGroups` groups */
  private def update(accs: IndexedSeq[Accumulator], groups: Array[Int], numGroups: Int, b: Batch) =
    // With fusion, arguments are evaluated up front and the fusable accumulators fold first
    var args: Array[Array[Vec]] = null
    var folded: Array[Boolean]  = null
    if fuseAccumulators && accs.size > 1 then
      accs.foreach(_.ensureGroups(numGroups))
      args = aggs.map(_.args.map(_.eval(b)).toArray).toArray
      folded = FusedAccumulators.update(accs, unfiltered, args, groups, b.size, numGroups)
    var a = 0
    while a < accs.size do
      if folded == null || !folded(a) then
        updateOne(
          aggs(a),
          accs(a),
          groups,
          numGroups,
          b,
          if args == null then
            null
          else
            args(a)
        )
      a += 1

  end update

  /** Folds the rows of b into one accumulator, with its arguments if already evaluated */
  private def updateOne(
      agg: AggCall,
      acc: Accumulator,
      groups: Array[Int],
      numGroups: Int,
      b: Batch,
      evaluated: Array[Vec]
  ): Unit =
    acc.ensureGroups(numGroups)
    // Without a FILTER row, the arguments aren't evaluated
    def argVecs =
      if evaluated != null then
        evaluated
      else
        agg.args.map(_.eval(b)).toArray
    agg.filter match
      case Some(f) =>
        val mask = Kernels.trueMask(f.eval(b))
        var cnt  = 0
        var i    = 0
        while i < b.size do
          if mask(i) then
            cnt += 1
          i += 1
        if cnt == b.size then
          acc.update(groups, b.size, argVecs)
        else if cnt > 0 then
          gatherUpdate(acc, groups, b.size, mask, cnt, argVecs)
      case None =>
        acc.update(groups, b.size, argVecs)

  end updateOne

  /** Folds the `cnt` rows of a batch of n rows where mask holds into acc */
  private def gatherUpdate(
      acc: Accumulator,
      groups: Array[Int],
      n: Int,
      mask: Array[Boolean],
      cnt: Int,
      argVecs: Array[Vec]
  ): Unit =
    if idx.length < cnt then
      idx = new Array[Int](cnt)
      gs = new Array[Int](cnt)
    var j = 0
    var i = 0
    while i < n do
      if mask(i) then
        idx(j) = i
        gs(j) = groups(i)
        j += 1
      i += 1
    acc.update(gs, cnt, argVecs.map(_.gather(idx, cnt)))

  /** Row i of b as its own group: the keys of b and each aggregate over one row */
  private def passThrough(b: Batch): Batch =
    val n = b.size
    if rowIds.length < n then
      rowIds = Array.tabulate(n)(identity)
    val accs = aggs.map(_.newAccumulator())
    update(accs, rowIds, n, b)
    Batch(keys.map(_.eval(b)).toArray ++ accs.map(_.result(n)), n)

  def next(): Batch =
    if passing then
      val b = child.next()
      return if b == null then
        null
      else
        passThrough(b)
    if done then
      return null
    done = true
    val adaptive = skip != null && keys.nonEmpty
    if adaptive && skip.skipping then
      // Grouping stopped before this part started
      passing = true
      return next()
    val presize =
      if keys.isEmpty then
        0
      else
        expectedGroups
    val table = PackedGroupTable.forKeys(keys.map(_.ptype), presize, packKeys, longDecimalKeys)
    val accs  = aggs.map(_.newAccumulator())
    if presize > 0 then
      accs.foreach(_.ensureGroups(presize))
    var zeros: Array[Int] = Array.emptyIntArray
    var rows              = 0L
    // Whether grouping stopped before the input ended: the rest of it passes through
    var stopped = false
    var b       = child.next()
    while b != null do
      val groups =
        if keys.isEmpty then
          if zeros.length < b.size then
            zeros = new Array[Int](b.size)
          zeros
        else
          table.groupIds(keys.map(_.eval(b)).toArray, b.size)
      val numGroups =
        if keys.isEmpty then
          1
        else
          table.numGroups
      update(accs, groups, numGroups, b)
      rows += b.size
      stopped = adaptive && skip.shouldStop(rows, table.numGroups)
      b =
        if stopped then
          null
        else
          child.next()
    end while
    passing = stopped
    if adaptive && !stopped then
      skip.report(rows, table.numGroups)
    val numGroups =
      if keys.isEmpty then
        1
      else
        table.numGroups
    if onGroups != null then
      onGroups(numGroups)
    if numGroups == 0 then
      return next()
    accs.foreach(_.ensureGroups(numGroups))
    val keyVecs =
      if keys.isEmpty then
        Array.empty[Vec]
      else
        table.keyVectors()
    val aggVecs = accs.map(_.result(numGroups))
    Batch(keyVecs ++ aggVecs, numGroups)

  end next

end HashAggOp

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
        table = PackedGroupTable.forKeys(child.schema.types, 0, packKeys, longDecimalKeys)
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

/**
  * The hash table of a join's build (right) input: its rows, and for each distinct key the chain of
  * rows holding it. Built once on first use, then only read, so probes may share it across threads
  */
final class JoinBuild(
    right: Operator,
    rightKeys: IndexedSeq[VExpr],
    trackMatches: Boolean = false,
    runner: TaskRunner = TaskRunner.Sequential,
    filters: Seq[RuntimeFilter] = Nil,
    packKeys: Boolean = true,
    flatTable: Boolean = true,
    nullsEqual: Boolean = false
):
  @volatile
  private var built = false

  // Runtime filters drop NULL probe keys, which match NULL build keys here
  require(!nullsEqual || filters.isEmpty, "A join with NULL-equal keys takes no runtime filters")

  // With `nullsEqual`, whether a build key has a NULL: NULL probe keys then look up the hash
  // tables as values, which the packed, direct, and flat paths cannot hold
  private var nullKeys = false

  var build: Batch = null

  // Rows are partitioned by the hash of their key, one hash table per partition; a single
  // partition for small builds. `heads(p)(g)` is the first row of group g of partition p, and
  // `chain(row)` the next row with the same key
  private var parts: Int                = 1
  private var hasher: GroupTable        = null
  private var tables: Array[GroupTable] = null
  private var heads: Array[Array[Int]]  = null
  private var chain: Array[Int]         = null
  // Multi-column integer keys packed into one Int64 key from the build key ranges, so the tables
  // compare one Long per candidate; null when the keys are not packed
  private var packer: KeyPacker = null
  // A single integer key over a dense range: `directHeads(key - directMin)` is the first row with
  // the key (DuckDB's perfect hash join, with chains for duplicate keys), so a probe neither hashes
  // nor compares keys. `directStore` reads probe keys in the build key's representation. Null when
  // the build uses hash tables
  private var directHeads: Array[Int]          = null
  private var directMin: Long                  = 0L
  private var directStore: GroupTable.KeyStore = null
  // A single integer key outside a dense range: one flat table over (key, first row) pairs, null
  // when the build uses a direct index or GroupTables. `directStore` normalizes its probe keys too
  private var flat: LongJoinTable = null

  /** Whether no two build rows share a key, so every chain ends at its first row */
  var uniqueKeys: Boolean = false

  /** The row with the same key after `row`, or -1 */
  inline def nextMatch(row: Int): Int = chain(row)

  /**
    * Whether a dense single integer key may use a direct index (`join_direct_index`); set before
    * the build is made
    */
  var directIndex: Boolean = true

  /**
    * Whether a large build concatenates its input, finds its valid rows and key ranges, and fills
    * its direct index on the runner instead of the calling thread (`parallel_join_build`); set
    * before the build is made
    */
  var parallelBuild: Boolean = true

  /** Whether the build probes through a direct index instead of hash tables (for tests) */
  def usesDirectIndex: Boolean = directHeads != null

  /** Whether the build probes through a [[LongJoinTable]] (for tests) */
  def usesFlatTable: Boolean = flat != null

  /**
    * Build rows some probe matched, shared by the probe copies of a right / full join. Copies only
    * ever set entries to true, and the unmatched rows are read after every copy has finished
    */
  var matched: Array[Boolean] = null

  def ensureBuilt(): Unit =
    if !built then
      synchronized {
        if !built then
          buildTable()
          built = true
      }

  private def buildTable(): Unit =
    val batches = right.drain()
    val rows    = batches.iterator.map(_.size.toLong).sum
    // The runner of the build steps after the input (`parallel_join_build`): small builds stay on
    // the calling thread
    val steps =
      if parallelBuild && runner.parallelism > 1 && rows >= JoinBuild.MinParallelRows then
        runner
      else
        TaskRunner.Sequential
    build =
      if batches.isEmpty then
        Batch.empty(right.schema)
      else if steps eq TaskRunner.Sequential then
        Batch.concat(right.schema, batches)
      else
        JoinBuildSteps.concat(right.schema, batches.toIndexedSeq, steps)
    val rawKeys  = rightKeys.map(_.eval(build)).toArray
    val rawTypes = rightKeys.map(_.ptype)
    // Rows with NULL keys never match, unless NULLs are equal (`IS NOT DISTINCT FROM`)
    nullKeys = nullsEqual && rawKeys.exists(k => k.nulls != null && k.nulls.contains(true))
    // With NULL keys equal, every row is valid: no key columns to check for NULLs
    val (valid, nv) = JoinBuildSteps.validRows(
      if nullKeys then
        Array.empty
      else
        rawKeys
      ,
      build.size,
      steps
    )
    filters
      .groupBy(_.key)
      .foreach((k, fs) => RuntimeFilter.setAll(fs, rawKeys(k), valid, nv, steps))
    if packKeys && !nullKeys then
      packer = KeyPacker.of(rawTypes, rawKeys, valid, nv)
    // The key columns the tables hold
    val (keyVecs, types) =
      if packer == null then
        (rawKeys, rawTypes)
      else
        (Array[Vec](packer.pack(rawKeys, build.size)), IndexedSeq(PType.Int64))
    chain = JoinBuildSteps.filledInts(build.size, -1, steps)
    if directIndex && types.size == 1 && !nullKeys then
      buildDirect(keyVecs(0), types(0), valid, nv, steps)
    if directHeads == null && flatTable && types.size == 1 && !nullKeys then
      buildFlat(keyVecs(0), types(0), valid, nv)
    if directHeads == null && flat == null then
      buildHashed(keyVecs, types, valid, nv)
    if trackMatches then
      matched = new Array[Boolean](build.size)

  end buildTable

  /**
    * The direct index of a single Int32 / Date / Int64 key whose range over the valid rows spans at
    * most [[JoinBuild.DirectRangeFactor]] slots per row, or at most [[JoinBuild.DirectMinSlots]];
    * leaves `directHeads` null otherwise
    */
  private def buildDirect(key: Vec, t: PType, valid: Array[Int], nv: Int, steps: TaskRunner): Unit =
    if nv == 0 || !(t == PType.Int32 || t == PType.Date || t == PType.Int64) then
      return
    val store = GroupTable.KeyStore(t)
    val norm  = store.normalize(key)
    // The key of row r as a Long; valid rows have no NULL key
    val ints: Array[Int] =
      norm match
        case v: IntVec =>
          v.values
        case _ =>
          null
    val longs: Array[Long] =
      norm match
        case v: LongVec =>
          v.values
        case _ =>
          null
    if ints == null && longs == null then
      return
    def keyAt(r: Int): Long =
      if ints != null then
        ints(r).toLong
      else
        longs(r)
    val (lo, hi) = JoinBuildSteps.keyRange(norm, valid, nv, steps).get
    val range    = hi - lo
    // A negative range overflowed a Long
    if range < 0 ||
      range >=
        math.min(
          JoinBuild.DirectMaxSlots,
          math.max(JoinBuild.DirectMinSlots, nv.toLong * JoinBuild.DirectRangeFactor)
        )
    then
      return
    val heads = JoinBuildSteps.filledInts(range.toInt + 1, -1, steps)
    // Insert rows(from until until) in reverse, so chains enumerate rows in input order; whether
    // two of them share a key
    def insert(rows: Array[Int], from: Int, until: Int): Boolean =
      var dup = false
      var j   = until - 1
      while j >= from do
        val row = rows(j)
        val k   = (keyAt(row) - lo).toInt
        val h   = heads(k)
        if h >= 0 then
          dup = true
        chain(row) = h
        heads(k) = row
        j -= 1
      dup
    val dup =
      if steps eq TaskRunner.Sequential then
        insert(valid, 0, nv)
      else
        // Split the key range into slices, one task each, so that no two tasks write the same
        // `heads` slot. Row chunks scatter their rows by slice into `order`, chunk after chunk, so
        // each slice lists its rows in input order and its chains come out as in a serial build
        val tasks  = JoinBuildSteps.taskCount(steps)
        val shift  = math.max(0, 64 - java.lang.Long.numberOfLeadingZeros(range / tasks))
        val slices = (range >>> shift).toInt + 1
        val chunks = JoinBuildSteps.chunks(nv, tasks)
        val counts = Array.ofDim[Int](chunks.length - 1, slices)
        JoinBuildSteps.runAll(chunks.length - 1, steps) { c =>
          val cnt = counts(c)
          var j   = chunks(c)
          while j < chunks(c + 1) do
            cnt(((keyAt(valid(j)) - lo) >>> shift).toInt) += 1
            j += 1
        }
        // Where each slice starts in `order`, and where each chunk writes within it
        val sliceStart = new Array[Int](slices + 1)
        val pos        = Array.ofDim[Int](chunks.length - 1, slices)
        var off        = 0
        var s          = 0
        while s < slices do
          sliceStart(s) = off
          var c = 0
          while c < counts.length do
            pos(c)(s) = off
            off += counts(c)(s)
            c += 1
          s += 1
        sliceStart(slices) = off
        val order = new Array[Int](nv)
        JoinBuildSteps.runAll(chunks.length - 1, steps) { c =>
          val p = pos(c)
          var j = chunks(c)
          while j < chunks(c + 1) do
            val row = valid(j)
            val sl  = ((keyAt(row) - lo) >>> shift).toInt
            order(p(sl)) = row
            p(sl) += 1
            j += 1
        }
        val dups = new Array[Boolean](slices)
        JoinBuildSteps.runAll(slices, steps) { s =>
          dups(s) = insert(order, sliceStart(s), sliceStart(s + 1))
        }
        dups.contains(true)
    directMin = lo
    directStore = store
    uniqueKeys = !dup
    directHeads = heads

  end buildDirect

  /** The [[LongJoinTable]] of a single Int32 / Date / Int64 key; leaves `flat` null otherwise */
  private def buildFlat(key: Vec, t: PType, valid: Array[Int], nv: Int): Unit =
    if !(t == PType.Int32 || t == PType.Date || t == PType.Int64) then
      return
    val store = GroupTable.KeyStore(t)
    // Every key as a Long; the values of rows with a NULL key are never read
    val longs: Array[Long] =
      store.normalize(key) match
        case v: LongVec =>
          v.values
        case v: IntVec =>
          val a = new Array[Long](v.size)
          var i = 0
          while i < a.length do
            a(i) = v.values(i).toLong
            i += 1
          a
        case _ =>
          return
    if runner.parallelism > 1 && nv >= JoinBuild.MinParallelRows then
      parts = GroupTable.partitionCount(runner.parallelism)
    val rowsOf =
      if parts == 1 then
        Array(java.util.Arrays.copyOf(valid, nv))
      else
        val h = new Array[Int](nv)
        var j = 0
        while j < nv do
          h(j) = LongJoinTable.hash(longs(valid(j)))
          j += 1
        val byPos = GroupTable.partitionRows(h, nv, parts)
        // Positions among the valid rows -> build rows
        byPos.foreach { r =>
          var k = 0
          while k < r.length do
            r(k) = valid(r(k))
            k += 1
        }
        byPos
    val (table, unique) = LongJoinTable.build(
      longs,
      rowsOf,
      Integer.numberOfTrailingZeros(parts),
      chain,
      runner
    )
    // A null table: a skewed build, which GroupTables take
    if table != null then
      directStore = store
      uniqueKeys = unique
      flat = table

  end buildFlat

  /** Hash tables over the valid rows, one per hash partition */
  private def buildHashed(
      keyVecs: Array[Vec],
      types: IndexedSeq[PType],
      valid: Array[Int],
      nv: Int
  ): Unit =
    if runner.parallelism > 1 && nv >= JoinBuild.MinParallelRows then
      parts = GroupTable.partitionCount(runner.parallelism)
    // Rows of each partition, in input order, and (with several partitions) their key hashes, so
    // that partition tables do not hash the keys again
    var hashesOf: Array[Array[Int]] = null
    val rowsOf: Array[Array[Int]]   =
      if parts == 1 then
        Array(java.util.Arrays.copyOf(valid, nv))
      else
        hasher = GroupTable(types)
        val h     = hasher.keyHashes(keyVecs.map(_.gather(valid, nv)), nv)
        val byPos = GroupTable.partitionRows(h, nv, parts)
        hashesOf = new Array[Array[Int]](parts)
        // Positions among the valid rows -> build rows (a loop: Array.map would box every Int)
        var p = 0
        while p < parts do
          val r  = byPos(p)
          val ph = new Array[Int](r.length)
          var j  = 0
          while j < r.length do
            ph(j) = h(r(j))
            r(j) = valid(r(j))
            j += 1
          hashesOf(p) = ph
          p += 1
        byPos
    tables = new Array[GroupTable](parts)
    heads = new Array[Array[Int]](parts)
    val uniqueParts             = new Array[Boolean](parts)
    def buildPart(p: Int): Unit =
      val rows = rowsOf(p)
      val n    = rows.length
      // A partition has at most one group per row
      val table   = GroupTable(types, math.min(n, GroupTable.MaxPresizeGroups))
      val partKey = keyVecs.map(_.gather(rows, n))
      // Hashes passed in, so the table, which lives through the probes, keeps no hash buffer
      val hashes =
        if hashesOf == null then
          table.keyHashes(partKey, n)
        else
          hashesOf(p)
      val ids  = table.groupIds(partKey, n, hashes)
      val head = Vec.filledInts(table.numGroups, -1)
      // Insert in reverse so chains enumerate rows in input order. Partitions hold disjoint rows,
      // so they write disjoint chain entries
      var j = n - 1
      while j >= 0 do
        val row = rows(j)
        val g   = ids(j)
        chain(row) = head(g)
        head(g) = row
        j -= 1
      tables(p) = table
      heads(p) = head
      uniqueParts(p) = table.numGroups == n
    if parts == 1 then
      buildPart(0)
    else
      (0 until parts).map(p => runner.submit(() => buildPart(p))).foreach(_.get())
    uniqueKeys = !uniqueParts.contains(false)

  end buildHashed

  /**
    * The first build row with the key of each probe row, or -1 when none has it (or the probe key
    * has a NULL, unless `nullsEqual`). Safe to call from several threads once built
    */
  def firstMatches(probeKeys: Array[Vec], n: Int): Array[Int] =
    val out = new Array[Int](n)
    // Probe rows outside the build key ranges pack as NULL, which never match
    val keys =
      if packer == null then
        probeKeys
      else
        Array[Vec](packer.pack(probeKeys, n))
    if directHeads != null then
      directMatches(keys(0), n, out)
    else if flat != null then
      directStore.normalize(keys(0)) match
        case v: LongVec =>
          flat.firstRows(v.values, v.nulls, n, out)
        case v: IntVec =>
          flat.firstRows(v.values, v.nulls, n, out)
        case other =>
          throw IllegalStateException(
            s"Unexpected join key vector: ${other.getClass.getSimpleName}"
          )
    else if parts == 1 then
      val ids  = tables(0).lookup(keys, n, nullsMatch = nullKeys)
      val head = heads(0)
      var i    = 0
      while i < n do
        val g = ids(i)
        out(i) =
          if g >= 0 then
            head(g)
          else
            -1
        i += 1
    else
      // Hash once: the partition tables hash alike, so they take each partition's hashes
      val h      = hasher.keyHashes(keys, n)
      val rowsOf = GroupTable.partitionRows(h, n, parts)
      var p      = 0
      while p < parts do
        val rows = rowsOf(p)
        val m    = rows.length
        if m > 0 then
          // Gathered keys keep the table's key comparisons sequential
          val ph = new Array[Int](m)
          var j  = 0
          while j < m do
            ph(j) = h(rows(j))
            j += 1
          val ids  = tables(p).lookup(keys.map(_.gather(rows, m)), m, ph, nullKeys)
          val head = heads(p)
          j = 0
          while j < m do
            val g = ids(j)
            out(rows(j)) =
              if g >= 0 then
                head(g)
              else
                -1
            j += 1
        p += 1
    end if
    out

  end firstMatches

  /** [[firstMatches]] through the direct index */
  private def directMatches(key: Vec, n: Int, out: Array[Int]): Unit =
    val norm  = directStore.normalize(key)
    val nulls = norm.nulls
    val heads = directHeads
    val lo    = directMin
    val size  = heads.length.toLong
    var i     = 0
    norm match
      case v: IntVec =>
        val x = v.values
        while i < n do
          val k = x(i).toLong - lo
          out(i) =
            if k >= 0 && k < size && (nulls == null || !nulls(i)) then
              heads(k.toInt)
            else
              -1
          i += 1
      case v: LongVec =>
        val x = v.values
        while i < n do
          val k = x(i) - lo
          // k < 0 also catches a subtraction that overflowed past Long.MaxValue
          out(i) =
            if k >= 0 && k < size && (nulls == null || !nulls(i)) then
              heads(k.toInt)
            else
              -1
          i += 1
      case other =>
        throw IllegalStateException(s"Unexpected join key vector: ${other.getClass.getSimpleName}")

  end directMatches

end JoinBuild

object JoinBuild:
  /** Builds with fewer rows use one hash table, built on the calling thread */
  def MinParallelRows: Int = sys
    .props
    .get("wvlet.engine.partitionedJoinMinRows")
    .flatMap(_.toIntOption)
    .getOrElse(65536)

  /**
    * Beyond [[DirectMinSlots]], a direct index takes at most this many slots per build row: 4 bytes
    * each, so at most 16 bytes per row, under what a hash table at half load takes
    */
  inline val DirectRangeFactor = 4

  /**
    * A direct index may always take this many slots (1 MB), however few build rows it has: it stays
    * in cache, and a probe makes one load instead of hashing and comparing. Small builds over wide
    * key ranges (a dimension filtered to a few hundred rows) take it, so their probes, mostly
    * misses, are cheap
    */
  inline val DirectMinSlots = 1L << 18

  /** The most slots a direct index takes */
  inline val DirectMaxSlots = 1L << 27

end JoinBuild

/** How a right / full join tracks and emits build rows no probe row matched */
enum OuterMode:
  /** Tracks its own matches and emits the unmatched rows at the end of its input */
  case Own

  /** One part of a split probe: marks the shared build's matches, emits no unmatched rows */
  case Part

  /** The last run after every part: marks the shared matches, then emits the unmatched rows */
  case Tail

/**
  * Equi hash join building on the right input. Output: left columns then right columns (Semi /
  * Anti: left columns only). `residual` is an extra condition over the joined row.
  *
  * `shared` is a build already made for this join elsewhere: copies of the join that probe parts of
  * the left input in parallel share one build
  */
class HashJoinOp(
    val left: Operator,
    val right: Operator,
    val leftKeys: IndexedSeq[VExpr],
    val rightKeys: IndexedSeq[VExpr],
    val kind: JoinKind,
    val residual: Option[VExpr],
    val schema: Schema,
    shared: JoinBuild = null,
    outerMode: OuterMode = OuterMode.Own,
    buildRunner: TaskRunner = TaskRunner.Sequential,
    val runtimeFilters: Seq[RuntimeFilter] = Nil,
    val nullsEqual: Boolean = false
) extends Operator:
  override def children: Seq[Operator] = Seq(left, right)
  override def name: String            = s"HashJoin[${kind}]"
  // `hashed` marks `join_direct_index` disabled, as `eager` marks `late_gather`. Whether a build
  // actually uses a direct index depends on its key range, known only once it is built
  override protected def details =
    s"(${leftKeys.map(_.sig).mkString(",")} = ${rightKeys.map(_.sig).mkString(",")}${residual
        .map(r => s", residual=${r.sig}")
        .getOrElse("")}${Operator.eagerTag(lateGather)}${
        if directIndex then
          ""
        else
          ", hashed"
      }${
        if parallelBuild then
          ""
        else
          ", serial build"
      }${
        if nullsEqual then
          ", nulls equal"
        else
          ""
      })"

  /** The build this join probes */
  val joinBuild: JoinBuild =
    if shared != null then
      shared
    else
      JoinBuild(
        right,
        rightKeys,
        trackMatches = kind.tracksBuildMatches,
        runner = buildRunner,
        filters = runtimeFilters,
        nullsEqual = nullsEqual
      )

  /** The same join over another left input, sharing this join's build */
  def withLeft(newLeft: Operator, mode: OuterMode = OuterMode.Own): HashJoinOp = HashJoinOp(
    newLeft,
    right,
    leftKeys,
    rightKeys,
    kind,
    residual,
    schema,
    joinBuild,
    mode,
    nullsEqual = nullsEqual
  ).withLateGather(lateGather)

  override def close(): Unit =
    // A shared build's right input belongs to the join that made the build
    if shared == null then
      super.close()
    else
      left.close()

  private var built                        = false
  private var build: Batch                 = null
  private var buildMatched: Array[Boolean] = null
  private var emittedUnmatched             = false
  private val leftWidth                    = left.schema.size

  /** Whether the output columns are deferred instead of gathered (`late_gather`) */
  var lateGather: Boolean = true

  def withLateGather(enabled: Boolean): this.type =
    lateGather = enabled
    this

  /** Whether the build may use a direct index for a dense integer key (`join_direct_index`) */
  def directIndex: Boolean = joinBuild.directIndex

  def withDirectIndex(enabled: Boolean): this.type =
    joinBuild.directIndex = enabled
    this

  /** Whether a large build runs its steps on the build runner (`parallel_join_build`) */
  def parallelBuild: Boolean = joinBuild.parallelBuild

  def withParallelBuild(enabled: Boolean): this.type =
    joinBuild.parallelBuild = enabled
    this

  private def rowsOf(b: Batch, idx: Array[Int], n: Int): Batch =
    if lateGather then
      b.take(idx, n)
    else
      b.gather(idx, n)

  /** Probe rows `lIdx` next to build rows `rIdx` (-1 for NULLs) */
  private def joined(probe: Batch, lIdx: Array[Int], rIdx: Array[Int]): Batch =
    rowsOf(probe, lIdx, lIdx.length) ++ rowsOf(build, rIdx, rIdx.length)

  private def buildTable(): Unit =
    joinBuild.ensureBuilt()
    build = joinBuild.build
    if kind.tracksBuildMatches then
      buildMatched =
        outerMode match
          case OuterMode.Own if shared == null =>
            // This join's own build: its matched array is this join's alone
            joinBuild.matched
          case OuterMode.Own =>
            new Array[Boolean](build.size)
          case _ =>
            joinBuild.matched
    built = true

  def next(): Batch =
    if !built then
      buildTable()
    var out: Batch = null
    while out == null do
      val probe = left.next()
      if probe == null then
        return emitUnmatchedBuild()
      out = probeBatch(probe)
    out

  private def emitUnmatchedBuild(): Batch =
    if buildMatched == null || emittedUnmatched || outerMode == OuterMode.Part then
      return null
    emittedUnmatched = true
    // RIGHT SEMI emits the matched build rows; the other kinds the unmatched ones
    val wanted = kind == JoinKind.RightSemi
    val idx    = (0 until build.size).filter(i => buildMatched(i) == wanted).toArray
    if idx.isEmpty then
      return null
    val n = idx.length
    if kind.outputsBuildOnly then
      rowsOf(build, idx, n)
    else
      val leftPad = left.schema.types.map(t => Vec.nullVec(t, n)).toArray
      Batch(leftPad, n) ++ rowsOf(build, idx, n)

  private def probeBatch(probe: Batch): Batch =
    val keyVecs = leftKeys.map(_.eval(probe)).toArray
    val first   = joinBuild.firstMatches(keyVecs, probe.size)
    if residual.isEmpty && kind.outputsProbeOnly then
      // A semi / anti join without a residual only asks whether a match exists: the first one
      // answers it, without walking the chain of build rows sharing the key
      return probeExists(probe, first)
    val n = probe.size
    // Matching pairs, sized for one match per probe row and doubled on overflow
    var lIdx = new Array[Int](n)
    var rIdx = new Array[Int](n)
    var np   = 0
    var i    = 0
    if joinBuild.uniqueKeys then
      // At most one match per probe row: the pairs fit, and no chain is read
      while i < n do
        val r = first(i)
        if r >= 0 then
          lIdx(np) = i
          rIdx(np) = r
          np += 1
        i += 1
    while i < n do
      var r = first(i)
      while r >= 0 do
        if np == lIdx.length then
          val cap = math.max(16, np * 2)
          lIdx = java.util.Arrays.copyOf(lIdx, cap)
          rIdx = java.util.Arrays.copyOf(rIdx, cap)
        lIdx(np) = i
        rIdx(np) = r
        np += 1
        r = joinBuild.nextMatch(r)
      i += 1
    if np < lIdx.length then
      lIdx = java.util.Arrays.copyOf(lIdx, np)
      rIdx = java.util.Arrays.copyOf(rIdx, np)
    // The pairs the residual condition kept, reusing the columns it gathered (late_gather only)
    var kept: Batch = null
    // Apply the residual condition on candidate pairs
    residual.foreach { cond =>
      if lIdx.nonEmpty then
        // Only the columns the condition reads are gathered
        val candidates = joined(probe, lIdx, rIdx)
        val mask       = Kernels.trueMask(cond.eval(candidates))
        val keep       = HashJoinOp.trueRows(mask, lIdx.length)
        lIdx = HashJoinOp.pick(lIdx, keep)
        rIdx = HashJoinOp.pick(rIdx, keep)
        if lateGather then
          kept = candidates.take(keep, keep.length)
    }
    def matchedPairs: Batch =
      if kept != null then
        kept
      else
        joined(probe, lIdx, rIdx)
    kind match
      case JoinKind.Inner | JoinKind.Cross =>
        if lIdx.isEmpty then
          null
        else
          matchedPairs
      case JoinKind.Left | JoinKind.Full | JoinKind.Right =>
        if buildMatched != null then
          rIdx.foreach(r => buildMatched(r) = true)
        if kind == JoinKind.Right then
          if lIdx.isEmpty then
            null
          else
            matchedPairs
        else
          val matched = new Array[Boolean](probe.size)
          var j       = 0
          while j < lIdx.length do
            matched(lIdx(j)) = true
            j += 1
          // Matched pairs, then each unmatched probe row against NULLs
          var total = lIdx.length
          i = 0
          while i < probe.size do
            if !matched(i) then
              total += 1
            i += 1
          if total == 0 then
            null
          else if total == lIdx.length then
            joined(probe, lIdx, rIdx)
          else
            val allL = java.util.Arrays.copyOf(lIdx, total)
            val allR = java.util.Arrays.copyOf(rIdx, total)
            var k    = lIdx.length
            i = 0
            while i < probe.size do
              if !matched(i) then
                allL(k) = i
                allR(k) = -1
                k += 1
              i += 1
            joined(probe, allL, allR)
        end if
      case JoinKind.Semi =>
        val matched = new Array[Boolean](probe.size)
        lIdx.foreach(l => matched(l) = true)
        val r =
          if lateGather then
            probe.takeWhere(matched)
          else
            probe.filter(matched)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.Anti =>
        val keep = new Array[Boolean](probe.size)
        java.util.Arrays.fill(keep, true)
        lIdx.foreach(l => keep(l) = false)
        val r =
          if lateGather then
            probe.takeWhere(keep)
          else
            probe.filter(keep)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.RightSemi | JoinKind.RightAnti =>
        // Only mark the matched build rows; they are emitted at the end of the input
        rIdx.foreach(r => buildMatched(r) = true)
        null
    end match

  end probeBatch

  /** A semi (rows with a match) or anti (rows without one) join's output from first matches */
  private def probeExists(probe: Batch, first: Array[Int]): Batch =
    val wantMatch = kind == JoinKind.Semi
    val keep      = new Array[Boolean](probe.size)
    var kept      = 0
    var i         = 0
    while i < probe.size do
      if (first(i) >= 0) == wantMatch then
        keep(i) = true
        kept += 1
      i += 1
    if kept == 0 then
      null
    else if kept == probe.size then
      probe
    else if lateGather then
      probe.takeWhere(keep)
    else
      probe.filter(keep)

end HashJoinOp

object HashJoinOp:
  /** Positions 0 until n where mask is true */
  private[exec] def trueRows(mask: Array[Boolean], n: Int): Array[Int] =
    var count = 0
    var i     = 0
    while i < n do
      if mask(i) then
        count += 1
      i += 1
    val out = new Array[Int](count)
    var k   = 0
    i = 0
    while i < n do
      if mask(i) then
        out(k) = i
        k += 1
      i += 1
    out

  /** xs at each position in idx (Array.map would box every Int) */
  private[exec] def pick(xs: Array[Int], idx: Array[Int]): Array[Int] =
    val out = new Array[Int](idx.length)
    var i   = 0
    while i < idx.length do
      out(i) = xs(idx(i))
      i += 1
    out

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
    val idx = (0 until build.size).filter(i => !buildMatched(i)).toArray
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
