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
  private var done    = false
  private var passing = false
  // The query's memory budget, captured where the aggregation is planned
  private val memory                   = MemoryContext.current.child()
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

  /**
    * Whether the fused fold adds one accumulator at a time over the batch ([[ColumnMajorSums]],
    * `column_major_fused_sums`)
    */
  var columnMajorSums: Boolean = true

  /** Set [[columnMajorSums]] */
  def withColumnMajorSums(enabled: Boolean): this.type =
    columnMajorSums = enabled
    this

  /**
    * Whether group keys and aggregate arguments over deferred columns read the columns' sources at
    * the selected positions instead of gathering them first ([[SelectedInputs]],
    * `aggregate_selected_rows`)
    */
  var selectedInputs: Boolean = true

  /** Set [[selectedInputs]] */
  def withSelectedInputs(enabled: Boolean): this.type =
    selectedInputs = enabled
    this

  /**
    * Whether string group keys and aggregate arguments over the deferred columns of a filter's
    * output read or compute over the columns' source rows ([[DenseSelectedRows]],
    * `dense_selected_rows`)
    */
  var denseSelected: Boolean = true

  /** Set [[denseSelected]] */
  def withDenseSelected(enabled: Boolean): this.type =
    denseSelected = enabled
    this

  // Arrays [[denseSelected]] reuses across batches, or null (`agg_scratch_buffers`)
  private[engine] var scratch: LongScratch = LongScratch()

  /** Reuse scratch arrays across batches for dense selected rows (`agg_scratch_buffers`) */
  def withScratchBuffers(enabled: Boolean): this.type =
    scratch =
      if enabled then
        LongScratch()
      else
        null
    this

  /**
    * Whether a single Int64 key, packed or not, groups through a [[LongGroupTable]]
    * (`long_group_tables`)
    */
  var longGroupTables: Boolean = true

  /** Set [[longGroupTables]] */
  def withLongGroupTables(enabled: Boolean): this.type =
    longGroupTables = enabled
    this

  /**
    * Whether a [[LongGroupTable]] groups runs of non-decreasing keys without hashing them while the
    * keys come in that order (`sorted_group_runs`)
    */
  var sortedGroupRuns: Boolean = true

  /** Set [[sortedGroupRuns]] */
  def withSortedGroupRuns(enabled: Boolean): this.type =
    sortedGroupRuns = enabled
    this

  // Aggregates without FILTER, which FusedAccumulators may fold
  private lazy val unfiltered: Array[Boolean] = aggs.map(_.filter.isEmpty).toArray
  // FILTERs over one shared dictionary, routed by code in one pass (`routed_agg_filters`)
  private lazy val router: RoutedAggFilters = RoutedAggFilters.of(aggs)

  // Scratch arrays reused across batches: accumulators read group ids only during update
  private var idx: Array[Int] = Array.emptyIntArray
  private var gs: Array[Int]  = Array.emptyIntArray
  // 0, 1, 2, ...: each passed-through row is its own group
  private var rowIds: Array[Int] = Array.emptyIntArray

  /** Folds the rows of b, in groups `groups`, into `accs` sized for `numGroups` groups */
  private def update(accs: IndexedSeq[Accumulator], groups: Array[Int], numGroups: Int, b: Batch) =
    // With fusion, the arguments of aggregates without FILTER (the ones fusion may fold) are
    // evaluated up front and the fusable accumulators fold first
    var args: Array[Array[Vec]] = null
    var folded: Array[Boolean]  = null
    // With selected inputs, arguments are evaluated up front only when fusion can fold the batch,
    // so that the others can read deferred columns in place
    if fuseAccumulators && accs.size > 1 &&
      (!selectedInputs || FusedAccumulators.foldsBatch(numGroups, b.size))
    then
      accs.foreach(_.ensureGroups(numGroups))
      val dense =
        if denseSelected then
          DenseSelectedRows.fold(
            aggs,
            unfiltered,
            accs,
            groups,
            numGroups,
            b,
            columnMajorSums,
            scratch
          )
        else
          null
      if dense != null then
        args = dense.args
        folded = dense.folded
      else
        args =
          Array.tabulate(aggs.size)(a =>
            if unfiltered(a) then
              aggs(a).args.map(_.eval(b)).toArray
            else
              null
          )
        folded = FusedAccumulators.update(
          accs,
          unfiltered,
          args,
          groups,
          b.size,
          numGroups,
          columnMajor = columnMajorSums
        )
    end if
    val routed = RoutedAggFilters.update(router, accs, groups, numGroups, b)
    var a      = 0
    while a < accs.size do
      if (folded == null || !folded(a)) && (routed == null || !routed(a)) then
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
    if selectedInputs && evaluated == null && agg.filter.isEmpty && agg.args.size == 1 &&
      SelectedInputs.update(acc, agg.args(0), groups, b)
    then
      return
    if denseSelected && evaluated == null && agg.filter.isEmpty && agg.args.size == 1 &&
      DenseSelectedRows.update(acc, agg.args(0), groups, b)
    then
      return
    // Without a FILTER row, the arguments aren't evaluated
    def argVecs =
      if evaluated != null then
        evaluated
      else
        agg.args.map(_.eval(b)).toArray
    agg.evalFilter match
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

  /**
    * Whether passed-through rows take their partial results from the aggregates' arguments
    * ([[OneRowPartials]], `one_row_partials`) instead of folding each batch into new accumulators
    */
  var oneRowPartials: Boolean = true

  /** Set [[oneRowPartials]] */
  def withOneRowPartials(enabled: Boolean): this.type =
    oneRowPartials = enabled
    this

  /** Row i of b as its own group: the keys of b and each aggregate over one row */
  private def passThrough(b: Batch): Batch =
    val n = b.size
    if !oneRowPartials then
      if rowIds.length < n then
        rowIds = Array.tabulate(n)(identity)
      val accs = aggs.map(_.newAccumulator())
      update(accs, rowIds, n, b)
      return Batch(keys.map(_.eval(b)).toArray ++ accs.map(_.result(n)), n)
    if rowIds.length < n then
      rowIds = new Array[Int](n)
      var i = 0
      while i < n do
        rowIds(i) = i
        i += 1
    val values = aggs.map { a =>
      val args =
        a.args
          .map(e =>
            if selectedInputs then
              SelectedInputs.eval(e, b)
            else
              e.eval(b)
          )
          .toArray
      val d = OneRowPartials.of(a, args, b)
      if d != null then
        d
      else
        // No direct one-row result: fold the batch with one group per row
        val acc = a.newAccumulator()
        updateOne(a, acc, rowIds, n, b, args)
        acc.result(n)
    }
    Batch(keys.map(_.eval(b)).toArray ++ values, n)

  end passThrough

  def next(): Batch =
    if passing then
      val b = child.next()
      return if b == null then
        memory.release()
        null
      else
        passThrough(b)
    if done then
      // The groups were emitted: the table is dropped
      memory.release()
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
    val table = PackedGroupTable.forKeys(
      keys.map(_.ptype),
      presize,
      packKeys,
      longDecimalKeys,
      stableCodes = true,
      longTables = longGroupTables,
      sortedRuns = sortedGroupRuns
    )
    val accs = aggs.map(_.newAccumulator())
    // The group table's key and the aggregates' state of each group; one group without keys
    val groupMemory =
      if keys.isEmpty then
        MemoryContext.NoOp.growing(0L)
      else
        memory.growing(
          keys.map(k => MemoryContext.valueBytes(k.ptype)).sum + HashAggOp.GroupTableBytes +
            aggs.map(a => MemoryContext.valueBytes(a.ptype)).sum
        )
    groupMemory.ensure(presize)
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
          table match
            case p: PackedGroupTable if selectedInputs =>
              SelectedInputs.groupIds(p, keys, b)
            case g: GroupTable if denseSelected && b.isDeferred =>
              DenseSelectedRows.groupIds(g, keys, b, scratch != null)
            case _ =>
              table.groupIds(keys.map(_.eval(b)).toArray, b.size)
      val numGroups =
        if keys.isEmpty then
          1
        else
          table.numGroups
      groupMemory.ensure(numGroups)
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

object HashAggOp:
  /** Bytes per group a memory reservation counts for the group table's slot and id (~12) */
  private[exec] val GroupTableBytes = 12L
