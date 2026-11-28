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
package wvlet.lang.engine.plan

import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.runtime.Lowering
import wvlet.lang.engine.runtime.RuntimePlan
import wvlet.lang.engine.vector.*

/**
  * The physical steps of [[Planner]]: lowering a runtime plan with the toggles' flags, then
  * configuring the operators (column pruning, prefilters, the operator flags of the toggles, and
  * [[Parallelizer]]). A flag that has to hold on both sides of Parallelizer is cleared in both
  * passes of [[optimizePhysical]]
  */
trait PhysicalSteps:
  this: Planner =>

  /**
    * Lower a runtime plan into operators, with runtime join filters when enabled, under the query's
    * memory context (also when a lazily computed result lowers its plan on a worker thread)
    */
  private[engine] def lower(p: RuntimePlan, inSet: Boolean = false): Operator =
    MemoryContext.withCurrent(memory)(lowered(p, inSet))

  private def lowered(p: RuntimePlan, inSet: Boolean): Operator = Lowering(
    p,
    joinFilters = enabled(Optimization.RuntimeJoinFilters),
    rowJoinFilters = enabled(Optimization.RowJoinFilters),
    buildSideJoinFilters = enabled(Optimization.BuildSideJoinFilters),
    presizeAggs = enabled(Optimization.PresizeAggregates),
    inSet = inSet,
    accelerator = accelerator.filter(_ => enabled(Optimization.GpuOffload)),
    footerCache = enabled(Optimization.ParquetFooterCache),
    windowTopN = enabled(Optimization.WindowTopN),
    windowKernels = enabled(Optimization.RunningWindowKernels),
    commonSubexpressions = enabled(Optimization.CommonSubexpressions),
    cseHashedKeys = enabled(Optimization.CseHashedKeys),
    foldConstants = enabled(Optimization.FoldConstants),
    windowSortKernels = enabled(Optimization.WindowSortKernels),
    scanRowFilters = enabled(Optimization.ScanRowFilters),
    dictionaryCodes = enabled(Optimization.DictionaryGroupKeys),
    dictionaryEntries = enabled(Optimization.DictionaryEntryScans),
    lazyStrings = enabled(Optimization.LazyDictionaryStrings),
    plainStringBytes = enabled(Optimization.PlainStringBytes),
    selectiveRowFilters = enabled(Optimization.SelectiveScanRowFilters),
    maskedCaseWhen = enabled(Optimization.MaskedCaseWhen),
    windowSharePartitions = enabled(Optimization.WindowSharePartitions),
    windowTopNStream = enabled(Optimization.WindowTopNStream),
    constCaseSelect = enabled(Optimization.ConstCaseSelect),
    caseAggFilters = enabled(Optimization.CaseAggFilters),
    dictionaryAggFilters = enabled(Optimization.DictionaryAggFilters),
    routedAggFilters = enabled(Optimization.RoutedAggFilters),
    substringInLists = enabled(Optimization.SubstringInLists),
    valueSelectivity = enabled(Optimization.StringValueSelectivity),
    columnImpliedRowFilters = enabled(Optimization.ColumnImpliedRowFilters),
    deferredNullChecks = enabled(Optimization.DeferredNullChecks),
    fusedDecimalArith = enabled(Optimization.FusedDecimalArith),
    semiAntiOneProbe = enabled(Optimization.SemiAntiOneProbe)
  )

  /** Whether a join builds on the other input, as this one has much more rows than that */
  private[engine] def swapBuild(rows: Double, otherRows: Double): Boolean = JoinOrderOptimizer
    .swapBuild(options, rows, otherRows)

  /**
    * Whether a semi / anti join builds on its outer input, as the subquery input (`rows`) is larger
    * ([[JoinOrderOptimizer.swapSemiBuild]])
    */
  private[engine] def swapSemiBuild(rows: Double, otherRows: Double, aggregates: Boolean): Boolean =
    JoinOrderOptimizer.swapSemiBuild(options, rows, otherRows, aggregates)

  /**
    * The physical passes over a lowered subquery computed at plan time, as [[plan]] runs them for a
    * nested relation, so that it runs in parallel
    */
  private[engine] def physical(op: Operator): Operator =
    MemoryContext.withCurrent(memory)(optimizePhysical(op))

  /**
    * The join toggles of [[optimizePhysical]] for one hash join: the planned joins, and the
    * INTERSECT / EXCEPT joins [[Parallelizer]] makes after them (`set_op_joins`), so that turning a
    * toggle off reaches those too. With `probe_miss_filter`, joins whose probes mostly miss (anti
    * joins and unmatched-only LEFT joins) get a Bloom filter of their build keys
    */
  private def configureJoin(j: HashJoinOp): Unit =
    if !enabled(Optimization.LateGather) then
      j.lateGather = false
    if enabled(Optimization.ProbeMissFilter) && Planner.missFilterFits(j) then
      j.withMissFilter(true)
    if !enabled(Optimization.JoinDirectIndex) then
      j.withDirectIndex(false)
    if !enabled(Optimization.NarrowJoinEntries) then
      j.withNarrowEntries(false)
    if !enabled(Optimization.ParallelJoinBuild) then
      j.withParallelBuild(false)
    if !enabled(Optimization.ParallelJoinKeys) then
      j.withParallelKeys(false)
    if !enabled(Optimization.JoinBuildStringCodes) then
      j.withStringCodes(false)
    if !enabled(Optimization.StringCodeProbe) then
      j.withCodeProbe(false)

  /** The physical passes over a lowered plan: column pruning (with scan filters), parallelism */
  private[plan] def optimizePhysical(op: Operator): Operator =
    val prunedOnly = ColumnPruner(
      op,
      pruneColumns = enabled(Optimization.PruneColumns),
      scanFilters = enabled(Optimization.PushScanFilters),
      numericScanFilters = enabled(Optimization.DecimalFloatScanFilters)
    )
    // Right after pruning, before the passes that set the options of the operators it rebuilds
    val pruned =
      if enabled(Optimization.LeftMatchPrefilter) then
        LeftMatchPrefilter(prunedOnly)
      else
        prunedOnly
    if !enabled(Optimization.LateGather) then
      Planner.disableLateGather(pruned)
    if !enabled(Optimization.DenseSelectedRows) then
      Planner.disableDenseSelected(pruned)
    if !enabled(Optimization.AggScratchBuffers) then
      Planner.disableScratchBuffers(pruned)
    if !enabled(Optimization.ScanJoinRowFilters) then
      Planner.disableSourceJoinFilters(pruned)
    if !enabled(Optimization.JoinRowFiltersFirst) then
      Planner.disableJoinFiltersFirst(pruned)
    if !enabled(Optimization.SparseKeyPassRates) then
      Planner.disableSparseKeyPassRates(pruned)
    if !enabled(Optimization.KeySetRowGroups) then
      Planner.disableKeySetRowGroups(pruned)
    if !enabled(Optimization.ParallelColumnDecode) then
      Planner.disableParallelColumnDecode(pruned)
    if !enabled(Optimization.ParallelStringDecode) then
      Planner.disableParallelStringDecode(pruned)
    if !enabled(Optimization.ShortCircuitFilters) || !enabled(Optimization.ReorderConjuncts) ||
      !enabled(Optimization.DictionaryFilters) || !enabled(Optimization.DictionarySelection)
    then
      Planner.configureFilters(
        pruned,
        shortCircuit = enabled(Optimization.ShortCircuitFilters),
        reorder = enabled(Optimization.ReorderConjuncts),
        dictionary = enabled(Optimization.DictionaryFilters),
        dictionarySelection = enabled(Optimization.DictionarySelection)
      )
    if enabled(Optimization.UnmatchedLeftProbe) then
      Planner.markUnmatchedLeftProbes(pruned)
    // After the unmatched-only LEFT joins are known
    Planner.foreachHashJoin(pruned)(configureJoin)
    // Before parallelizing: the per-part aggregations take the option from these operators
    if !enabled(Optimization.ColumnMajorFusedSums) then
      Planner.disableColumnMajorSums(pruned)
    val physical =
      if enabled(Optimization.Parallelize) then
        Parallelizer(
          pruned,
          TaskRunner.default,
          aboveBreakers = enabled(Optimization.ParallelAboveBreakers),
          parallelPartitioning = enabled(Optimization.ParallelAggPartitioning),
          concurrentBuilds = enabled(Optimization.ConcurrentJoinBuilds),
          sharedPartials = enabled(Optimization.SharedAggPartials),
          adaptivePartials = enabled(Optimization.AdaptivePartialAggs),
          setOpJoins = enabled(Optimization.SetOpJoins),
          packAggKeys = enabled(Optimization.PackAggKeys),
          streamPackAggKeys = enabled(Optimization.StreamPackAggKeys),
          longDecimalKeys = enabled(Optimization.LongDecimalKeys),
          unionPipelines = enabled(Optimization.UnionAllPipelines),
          fusedAccumulators = enabled(Optimization.FusedAccumulators),
          oneRowPartials = enabled(Optimization.OneRowPartials),
          selectedInputs = enabled(Optimization.AggregateSelectedRows),
          longGroupTables = enabled(Optimization.LongGroupTables),
          sortedGroupRuns = enabled(Optimization.SortedGroupRuns),
          unionDistinctOnce = enabled(Optimization.UnionDistinctOnce),
          anyValuePartials = enabled(Optimization.DependentGroupKeys),
          parallelOuterTail = enabled(Optimization.ParallelOuterTail),
          sliceSingleBatches = enabled(Optimization.SliceSingleBatchProbes),
          configureJoin = configureJoin
        )
      else
        pruned
    val streamPack   = enabled(Optimization.StreamPackAggKeys)
    val longDecimals = enabled(Optimization.LongDecimalKeys)
    if !streamPack || !longDecimals then
      Planner.setAggKeyOptions(physical, streamPack, longDecimals)
    if !enabled(Optimization.FusedAccumulators) then
      Planner.disableFusedAccumulators(physical)
    if !enabled(Optimization.AggregateSelectedRows) then
      Planner.disableSelectedInputs(physical)
    if !enabled(Optimization.ColumnMajorFusedSums) then
      Planner.disableColumnMajorSums(physical)
    if !enabled(Optimization.DenseSelectedRows) then
      Planner.disableDenseSelected(physical)
    if !enabled(Optimization.AggScratchBuffers) then
      Planner.disableScratchBuffers(physical)
    if !enabled(Optimization.LongGroupTables) then
      Planner.disableLongGroupTables(physical)
    if !enabled(Optimization.SortedGroupRuns) then
      Planner.disableSortedGroupRuns(physical)
    if !enabled(Optimization.SortedAggSplits) then
      Planner.disableSortedAggSplits(physical)
    if !enabled(Optimization.PackedAggRouting) then
      Planner.disablePackedAggRouting(physical)
    if !enabled(Optimization.SizePartitionsByRows) then
      Planner.disableSizePartitionsByRows(physical)
    if !enabled(Optimization.SpillAggregation) then
      Planner.disableSpillAggregation(physical)
    physical

  end optimizePhysical

end PhysicalSteps

/**
  * The walks over a physical plan that set or clear operator flags for the toggles, run by
  * [[PhysicalSteps.optimizePhysical]]; `Planner.disableLateGather` and the others are these
  */
trait PhysicalWalkers:

  /** Make the filters and hash joins of a physical plan gather their output columns eagerly */
  private[engine] def disableLateGather(op: Operator): Unit =
    op match
      case f: FilterOp =>
        f.lateGather = false
      case j: HashJoinOp =>
        j.lateGather = false
      case s: ScanOp =>
        s.lateGather = false
      case _ =>
    op.children.foreach(disableLateGather)
    op.sharedInputs.foreach(disableLateGather)

  /**
    * Keep every per-row runtime filter of a physical plan's scans in the engine
    * (`scan_join_row_filters`)
    */
  private[engine] def disableSourceJoinFilters(op: Operator): Unit =
    op match
      case s: ScanOp =>
        s.sourceJoinFilters = false
      case _ =>
    op.children.foreach(disableSourceJoinFilters)
    op.sharedInputs.foreach(disableSourceJoinFilters)

  /** Keep the scans' own row filter conjuncts ahead of the per-row join filters */
  private[engine] def disableJoinFiltersFirst(op: Operator): Unit =
    op match
      case s: ScanOp =>
        s.joinFiltersFirst = false
      case _ =>
    op.children.foreach(disableJoinFiltersFirst)
    op.sharedInputs.foreach(disableJoinFiltersFirst)

  /** Estimate per-row join filters' pass rates over the width of the probe key's range */
  private[engine] def disableSparseKeyPassRates(op: Operator): Unit =
    op match
      case s: ScanOp =>
        s.sparseKeyPassRates = false
      case _ =>
    op.children.foreach(disableSparseKeyPassRates)
    op.sharedInputs.foreach(disableSparseKeyPassRates)

  /** Skip row groups by the per-row join filters' [min, max] ranges only (`key_set_row_groups`) */
  private[engine] def disableKeySetRowGroups(op: Operator): Unit =
    op match
      case s: ScanOp =>
        s.keySetRowGroups = false
      case _ =>
    op.children.foreach(disableKeySetRowGroups)
    op.sharedInputs.foreach(disableKeySetRowGroups)

  /** Decode a single row group's columns on one thread (`parallel_column_decode`) */
  private[engine] def disableParallelColumnDecode(op: Operator): Unit =
    op match
      case s: ScanOp =>
        s.parallelColumnDecode = false
      case _ =>
    op.children.foreach(disableParallelColumnDecode)
    op.sharedInputs.foreach(disableParallelColumnDecode)

  /** Build the Strings of a parallel decode with its column tasks (`parallel_string_decode`) */
  private[engine] def disableParallelStringDecode(op: Operator): Unit =
    op match
      case s: ScanOp =>
        s.parallelStringDecode = false
      case _ =>
    op.children.foreach(disableParallelStringDecode)
    op.sharedInputs.foreach(disableParallelStringDecode)

  /** Set how the filters of a physical plan evaluate AND / OR (`short_circuit_filters`) */
  private[engine] def configureFilters(
      op: Operator,
      shortCircuit: Boolean,
      reorder: Boolean,
      dictionary: Boolean,
      dictionarySelection: Boolean = true
  ): Unit =
    op match
      case f: FilterOp =>
        f.shortCircuit = shortCircuit
        f.reorderConjuncts = reorder
        f.dictionaryFilters = dictionary
        f.dictionarySelection = dictionarySelection
      case _ =>
    op.children.foreach(configureFilters(_, shortCircuit, reorder, dictionary, dictionarySelection))
    op.sharedInputs
      .foreach(configureFilters(_, shortCircuit, reorder, dictionary, dictionarySelection))

  /**
    * Make each LEFT hash join under a filter that has a conjunct `k IS NULL`, for `k` one of the
    * join's build key columns, emit only its unmatched probe rows (`unmatched_left_probe`): the
    * build key of a matched row equals the probe's non-NULL key, so the filter drops every matched
    * pair, and the NULL-padded unmatched rows are what it reads
    */
  private[engine] def markUnmatchedLeftProbes(op: Operator): Unit =
    op match
      case f: FilterOp =>
        f.child match
          case j: HashJoinOp
              if j.kind == JoinKind.Left && j.residual.isEmpty && !j.nullsEqual &&
                j.schema.size == j.left.schema.size + j.right.schema.size =>
            val lw = j.left.schema.size
            // Build columns the join matches on, by their position in the join output
            val keyCols =
              j.rightKeys
                .collect { case c: ColRef =>
                  c.index + lw
                }
                .toSet
            def testsKey(e: VExpr): Boolean =
              e match
                case AndExpr(a, b) =>
                  testsKey(a) || testsKey(b)
                case IsNullExpr(c: ColRef, false, _) =>
                  keyCols.contains(c.index)
                case _ =>
                  false
            if testsKey(f.cond) then
              j.withUnmatchedOnly(true)
          case _ =>
      case _ =>
    op.children.foreach(markUnmatchedLeftProbes)
    op.sharedInputs.foreach(markUnmatchedLeftProbes)

  /** Apply f to every hash join of a physical plan */
  private[engine] def foreachHashJoin(op: Operator)(f: HashJoinOp => Unit): Unit =
    op match
      case j: HashJoinOp =>
        f(j)
      case _ =>
    op.children.foreach(foreachHashJoin(_)(f))
    op.sharedInputs.foreach(foreachHashJoin(_)(f))

  /**
    * Whether a join's probes mostly miss: an anti join, an unmatched-only LEFT join, or a LEFT join
    * whose build a `left_match_prefilter` filter probes
    */
  private[engine] def missFilterFits(j: HashJoinOp): Boolean =
    j.kind == JoinKind.Anti || j.unmatchedOnly || LeftMatchPrefilter.feedsPrefilter(j)

  /**
    * Set how the aggregations and DISTINCTs of a physical plan key their groups
    * (`stream_pack_agg_keys`, `long_decimal_keys`). The partial aggregates that two-phase
    * aggregations create per part get the options from [[Parallelizer]]
    */
  private[engine] def setAggKeyOptions(op: Operator, pack: Boolean, longDecimals: Boolean): Unit =
    op match
      case a: HashAggOp =>
        a.withKeyOptions(pack, longDecimals)
      case d: DistinctOp =>
        d.withKeyOptions(pack, longDecimals)
      case a: PartitionedAggOp =>
        a.withLongDecimalKeys(longDecimals)
      case _ =>
    op.children.foreach(setAggKeyOptions(_, pack, longDecimals))
    op.sharedInputs.foreach(setAggKeyOptions(_, pack, longDecimals))

  /**
    * Make the hash aggregations of a physical plan, and those of its partitioned aggregations,
    * update each aggregate on its own (`fused_accumulators`). The partial aggregates that two-phase
    * aggregations create per part get the option from [[Parallelizer]]
    */
  private[engine] def disableFusedAccumulators(op: Operator): Unit =
    op match
      case a: HashAggOp =>
        a.withFusedAccumulators(false)
      case a: PartitionedAggOp =>
        a.withFusedAccumulators(false)
      case _ =>
    op.children.foreach(disableFusedAccumulators)
    op.sharedInputs.foreach(disableFusedAccumulators)

  /**
    * Make the fused folds of hash aggregations update every accumulator per row
    * (`column_major_fused_sums`). The partial aggregates that two-phase aggregations create per
    * part take the option from the hash aggregations they split
    */
  private[engine] def disableColumnMajorSums(op: Operator): Unit =
    op match
      case a: HashAggOp =>
        a.withColumnMajorSums(false)
      case a: PartitionedAggOp =>
        a.withColumnMajorSums(false)
      case _ =>
    op.children.foreach(disableColumnMajorSums)
    op.sharedInputs.foreach(disableColumnMajorSums)

  /**
    * Make the hash aggregations of a physical plan gather deferred input columns before reading
    * them (`aggregate_selected_rows`). The partial aggregates that two-phase aggregations create
    * per part get the option from [[Parallelizer]]
    */
  private[engine] def disableSelectedInputs(op: Operator): Unit =
    op match
      case a: HashAggOp =>
        a.withSelectedInputs(false)
      case _ =>
    op.children.foreach(disableSelectedInputs)
    op.sharedInputs.foreach(disableSelectedInputs)

  /**
    * Make the projections and hash aggregations of a plan gather the deferred columns of a filter's
    * output before computing over them (`dense_selected_rows`). Their per-part copies in
    * [[Parallelizer]] take the option from these operators
    */
  private[engine] def disableDenseSelected(op: Operator): Unit =
    op match
      case p: ProjectOp =>
        p.withDenseSelected(false)
      case a: HashAggOp =>
        a.withDenseSelected(false)
      case _ =>
    op.children.foreach(disableDenseSelected)
    op.sharedInputs.foreach(disableDenseSelected)

  /** Make the hash aggregations of a physical plan allocate their arrays per batch */
  private[engine] def disableScratchBuffers(op: Operator): Unit =
    op match
      case a: HashAggOp =>
        a.withScratchBuffers(false)
      case _ =>
    op.children.foreach(disableScratchBuffers)
    op.sharedInputs.foreach(disableScratchBuffers)

  /**
    * Make the hash aggregations of a physical plan group single Int64 keys through [[GroupTable]]s
    * (`long_group_tables`). The partial aggregates that two-phase aggregations create per part get
    * the option from [[Parallelizer]]
    */
  private[engine] def disableLongGroupTables(op: Operator): Unit =
    op match
      case a: HashAggOp =>
        a.withLongGroupTables(false)
      case a: PartitionedAggOp =>
        a.withLongGroupTables(false)
      case _ =>
    op.children.foreach(disableLongGroupTables)
    op.sharedInputs.foreach(disableLongGroupTables)

  /**
    * Make the hash aggregations of a physical plan hash every key (`sorted_group_runs`). The
    * partial aggregates that two-phase aggregations create per part get the option from
    * [[Parallelizer]]
    */
  private[engine] def disableSortedGroupRuns(op: Operator): Unit =
    op match
      case a: HashAggOp =>
        a.withSortedGroupRuns(false)
      case a: PartitionedAggOp =>
        a.withSortedGroupRuns(false)
      case _ =>
    op.children.foreach(disableSortedGroupRuns)
    op.sharedInputs.foreach(disableSortedGroupRuns)

  /**
    * Make the partitioned aggregations of a physical plan hash-route every row
    * (`sorted_agg_splits`)
    */
  private[engine] def disableSortedAggSplits(op: Operator): Unit =
    op match
      case a: PartitionedAggOp =>
        a.withSortedSplits(false)
      case _ =>
    op.children.foreach(disableSortedAggSplits)
    op.sharedInputs.foreach(disableSortedAggSplits)

  /**
    * Make the partitioned aggregations of a physical plan copy every column of each partition's
    * rows and pack their keys per partition (`packed_agg_routing`)
    */
  private[engine] def disablePackedAggRouting(op: Operator): Unit =
    op match
      case a: PartitionedAggOp =>
        a.withPackedRouting(false)
      case _ =>
    op.children.foreach(disablePackedAggRouting)
    op.sharedInputs.foreach(disablePackedAggRouting)

  /**
    * Make the partitions of partitioned aggregations size their tables for an even share of the
    * capped group estimate (`size_partitions_by_rows`)
    */
  private[engine] def disableSizePartitionsByRows(op: Operator): Unit =
    op match
      case a: PartitionedAggOp =>
        a.withSizeByRows(false)
      case _ =>
    op.children.foreach(disableSizePartitionsByRows)
    op.sharedInputs.foreach(disableSizePartitionsByRows)

  /** Make partitioned aggregations hold their whole input past the spill threshold */
  private[engine] def disableSpillAggregation(op: Operator): Unit =
    op match
      case a: PartitionedAggOp =>
        a.withSpill(false)
      case _ =>
    op.children.foreach(disableSpillAggregation)
    op.sharedInputs.foreach(disableSpillAggregation)

end PhysicalWalkers
