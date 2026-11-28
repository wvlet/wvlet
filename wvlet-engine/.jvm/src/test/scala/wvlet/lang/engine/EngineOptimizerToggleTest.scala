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
package wvlet.lang.engine

import wvlet.lang.compiler.Context
import wvlet.lang.engine.catalog.BenchmarkKeys
import wvlet.lang.engine.exec.TaskRunner
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/**
  * Turning off any one planner optimization may change plans but never results: spec/basic and
  * TPC-H, with each optimization disabled in turn, must still return the rows DuckDB returns (as
  * they do with every optimization on, see [[EngineBasicSpecTest]] and [[EngineTPCHTest]]). This
  * catches a query whose correct result secretly depends on an optimization.
  *
  * The queries are compiled, and their DuckDB rows computed, once for all optimizations: only the
  * engine runs again for each one
  */
class EngineOptimizerToggleTest extends UniTest:

  /** One spec folder's queries, compiled once. `skip` names the files left out */
  private class ToggleSuite(open: => EngineConformance, skip: Set[String]):
    private var opened = false
    private lazy val c =
      val conformance = open
      opened = true
      conformance

    private case class Query(name: String, fileName: String, relation: Relation, ctx: Context)

    private lazy val queries: Seq[Query] = c
      .units
      .filterNot(u => skip.contains(u.sourceFile.fileName))
      .flatMap { u =>
        c.queries(u)((name, q) => Query(name, u.sourceFile.fileName, q, summon[Context]))
      }

    /**
      * Fail unless every query returns DuckDB's rows with `options`, skipping the files in
      * `tooSlow`
      */
    def check(options: OptimizerOptions, tooSlow: Set[String]): Unit =
      val engine   = c.engine.withOptimizer(options)
      val failures = queries
        .filterNot(q => tooSlow.contains(q.fileName))
        .map(q => c.compare(q.name, q.relation, engine)(using q.ctx))
        .filterNot(_.matched)
      if failures.nonEmpty then
        fail(failures.map(r => s"${r.name}: ${r.message}").mkString("\n"))

    def close(): Unit =
      if opened then
        c.close()

  end ToggleSuite

  private val basic = ToggleSuite(
    EngineConformance("spec/basic", Nil, _ => ()),
    EngineBasicSpecTest.knownGaps
  )

  private val tpch =
    val tables = EngineTPCHBench.tables
    ToggleSuite(
      EngineConformance(
        "spec/tpch",
        tables.map(t => s"create view ${t} as select * from 'spec/tpch/data/${t}.parquet'"),
        engine =>
          tables.foreach(t =>
            engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet", BenchmarkKeys.tpch(t))
          )
      ),
      Set.empty
    )

  // The plans with every optimization on, to compare with the plans without one
  private lazy val baselinePlans = EnginePlanGolden.suites.map(_.generate())

  /**
    * Queries too slow to run without an optimization: without OR factoring, q19's join has no
    * equality key and becomes a cross join of lineitem and part
    */
  private val tpchTooSlow: Map[Optimization, Set[String]] = Map(
    Optimization.FactorOr -> Set("q19.wv")
  )

  /**
    * Optimizations whose TPC-H and TPC-DS plans `count_in_subqueries` replaces: the shared CTEs
    * they re-plan or rewrite are read by IN subqueries that it computes at plan time (TPC-DS q14,
    * q95). They are compared with it disabled on both sides
    */
  private val withoutCountIn: Set[Optimization] = Set(
    Optimization.ReplanSemiJoinBuilds,
    Optimization.ReplanSharedInputs,
    Optimization.SelfJoinAggregate,
    // TPC-H q20's aggregation is in an IN subquery computed at plan time
    Optimization.LeftGroupKeySemiJoins
  )

  /**
    * Optimizations that change no TPC-H or TPC-DS plan, with the reason. The comments name the
    * tests that check that they take effect
    */
  private val noPlanChange: Map[Optimization, String] = Map(
    // EnginePlanningMemoTest checks that a subquery's correlation is checked once
    Optimization.PlanningMemo ->
      "Changes how often planning checks a subquery's correlation, not the plan",
    // CommonSubexpressionsTest checks that both ways share the same subexpressions
    Optimization.CseHashedKeys ->
      "Changes how common subexpressions are counted, not which ones are shared",
    // EngineLateColumnsTest checks the rewrite and its rows; it needs 64x the limit's rows at the
    // sort, which the golden scales don't reach (TPC-H q10 sorts at most 150 customers)
    Optimization.LateColumns ->
      "Fires only above 64 rows per kept row at the sort, beyond the golden plans' scales",
    // The subqueries become the same constants either way, one query per group or per subquery
    Optimization.PrefetchScalarSubqueries ->
      "Changes how scalar subqueries are evaluated while planning, not the plan",
    // EnginePresizeAggregatesTest checks that the flag sets the aggregations' group estimates
    Optimization.PresizeAggregates ->
      "Changes the initial size of aggregation tables, not the plan",
    // EngineMaterializedCTEStatsTest checks that the flag changes the plan
    Optimization.ReplanCTEBodies -> "No TPC-H or TPC-DS shared CTE body reads another shared CTE",
    // StatsTest checks that both ways count the same distinct values
    Optimization.PrimitiveResultDistinct ->
      "Changes how a computed CTE's distinct values are counted, not the counts",
    // ParallelResultStatsTest checks that both ways count the same values and rows
    Optimization.ParallelResultStats ->
      "Changes which threads compute a computed CTE's statistics, not the plans",
    // PartitionedAggOpTest checks that both ways give the same partitions
    Optimization.ParallelAggPartitioning ->
      "Changes how aggregation input is routed to partitions, not the plan",
    // DeferredNullChecksTest checks that both ways give the same null checks
    Optimization.DeferredNullChecks ->
      "Changes how IS NULL reads a column not gathered yet, not the plan",
    // EnginePlanAcceleratorTest checks that the flag offers plans to an accelerator
    Optimization.GpuOffload ->
      "Changes plans only with a PlanAccelerator, which these engines don't have",
    // EngineConcurrentSharedCTEsTest checks that the flag starts the results concurrently
    Optimization.ConcurrentSharedCTEs ->
      "Changes when shared CTE results are computed, not the plan",
    // EngineConcurrentJoinBuildsTest checks that the flag runs a pipeline's builds as one step
    Optimization.ConcurrentJoinBuilds ->
      "Changes when the hash join builds of a pipeline run, not the plan",
    // EngineWindowTopNTest checks that the flag reaches the window operators, and WindowOpTest that
    // both folds give the same values
    Optimization.RunningWindowKernels ->
      "Changes how running window aggregates are folded, not the plan",
    // EngineRuntimeJoinFiltersTest and EngineScanRowFiltersTest check that the flag puts per-row
    // join filters into the probe scans' row filters, with the same rows
    Optimization.ScanJoinRowFilters ->
      "Changes where probe scans test per-row join filters, not the plan",
    // EngineScanRowFiltersTest checks that the flag orders the probe scans' row filters, with the
    // same rows
    Optimization.JoinRowFiltersFirst ->
      "Changes the order of a probe scan's row filter conjuncts, not the plan",
    // EngineScanRowFiltersTest checks that the flag orders a sparse key's filter after a dense one
    Optimization.SparseKeyPassRates ->
      "Changes the estimated pass rates that order a probe scan's row filters, not the plan",
    // JoinBuildKeyStepsTest checks that the parallel key steps find the same rows
    Optimization.ParallelJoinKeys ->
      "Changes which threads compute a join build's keys and table, not the plan",
    // LongJoinTableTest checks that both entry layouts find the same rows
    Optimization.NarrowJoinEntries ->
      "Changes how a flat join table lays out its entries, not the plan",
    // EngineScanRowFiltersTest checks that the flag skips the row groups holding no build key
    Optimization.KeySetRowGroups ->
      "Changes which row groups a probe scan reads by its join filters' keys, not the plan",
    // ParquetFolderTest checks that a folder reads the same rows as its files, split and with stats
    Optimization.FolderFileScans -> "Changes how a folder of Parquet files is read, not the plan",
    // IcebergFilesTest and ParquetFolderTest check that Iceberg tables and schema-evolved folders
    // read the same rows file by file; the golden plans read single files
    Optimization.DatasetFileScans ->
      "Changes how an Iceberg table or schema-evolved folder is read, not the plan",
    // ParquetFolderTest checks that a partitioned folder's row filters return the same rows
    Optimization.PartitionRowFilters ->
      "Changes which partitioned folders take row filters, which the golden sources aren't",
    // JoinEnumeratorTest and ParquetFolderTest check that the credit reorders the joins over
    // partitions holding one key value each, which the generated golden sources have none of
    Optimization.PartitionProbeScanCost ->
      "Changes join order only over Parquet parts holding one key value each",
    // IcebergFilesTest checks that an Iceberg table of plain files splits into row groups
    Optimization.IcebergRowGroups -> "Changes how an Iceberg table's files are read, not the plan",
    // EngineParallelColumnDecodeTest checks that the flag decodes a single row group's columns as
    // tasks, with the same rows
    Optimization.ParallelColumnDecode ->
      "Changes how a single row group's columns are decoded, not the plan",
    // EngineParallelColumnDecodeTest checks that the flag builds PLAIN strings by row ranges, with
    // the same rows
    Optimization.ParallelStringDecode ->
      "Changes how a parallel decode builds its strings, not the plan",
    // MorselOpTest and EngineSliceSingleBatchProbesTest check that a run over one batch slices,
    // with the same rows
    Optimization.SliceSingleBatchProbes ->
      "Changes how a pipeline over one batch is split into tasks, not the plan",
    // EngineSharedScansTest checks that TPC-H q22's subquery reads the shared customer scan
    Optimization.MatchCopiedSubqueryScans ->
      "Changes which scan a plan-time subquery reads, which the root plan doesn't show",
    // GroupTableTest checks that nearly unique columns give up their codes after a few rows
    Optimization.StringCodeProbe ->
      "Changes when a join build gives up its string codes, not the plan",
    // EngineWindowTopNTest checks that the flag reaches the window operators, and WindowOpTest that
    // shared partitions give the same values
    Optimization.WindowSharePartitions ->
      "Changes how often window partitions are sorted, not the plan",
    // EngineWindowTopNTest checks that the flag reaches the window operators, and
    // WindowSortKernelTest that both sorts give the same row order
    Optimization.WindowSortKernels -> "Changes how window partitions are sorted, not the plan",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the partial aggregates, and
    // PartialAggSkipTest that passing rows through gives the same final groups
    Optimization.AdaptivePartialAggs ->
      "Changes when partial aggregates stop grouping, not the plan",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the partitioned aggregations, and
    // AggKeyPackingTest that packed keys give the same groups
    Optimization.PackAggKeys -> "Changes how partitioned aggregations compare keys, not the plan",
    // EngineAdaptivePartialAggsTest checks that the flags reach the aggregations, and
    // PackedGroupTableTest that packed and unscaled keys give the same groups
    Optimization.StreamPackAggKeys -> "Changes how aggregations compare keys, not the plan",
    Optimization.LongDecimalKeys   -> "Changes how aggregations compare keys, not the plan",
    // EngineJoinBuildStringCodesTest checks that the flag reaches the joins, and GroupTableTest the
    // grouping
    Optimization.JoinBuildStringCodes ->
      "Changes how join builds hold strings and aggregations compare them, not the plan",
    // EngineDictionaryGroupKeysTest checks that the flag reaches the scans, and GroupTableTest that
    // grouping by dictionary codes gives the same groups
    Optimization.DictionaryGroupKeys ->
      "Changes how scans decode strings and aggregations compare them, not the plan",
    // EngineDictionaryEntryScansTest checks that the flag reaches the scans and that reading
    // dictionary entries only gives the same strings
    Optimization.DictionaryEntryScans ->
      "Changes how scans copy dictionary-encoded strings, not the plan",
    // EnginePlainStringBytesTest checks that the flag reaches the scans and that the results
    // match String vectors
    Optimization.PlainStringBytes -> "Changes how scans hold PLAIN strings, not the plan",
    // EngineLazyDictionaryStringsTest checks that the flag reaches the scans and that the
    // entries-backed vectors give the same strings
    Optimization.LazyDictionaryStrings ->
      "Changes how scans hold dictionary-encoded strings, not the plan",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the hash aggregations, and
    // FusedAccumulatorsTest that the fused loop gives the same counts and sums
    Optimization.FusedAccumulators ->
      "Changes how hash aggregations fold their aggregates, not the plan",
    // FusedDecimalMulTest checks that the fused products equal Arith's, and that the flag reaches
    // the projections
    Optimization.FusedDecimalArith ->
      "Changes how decimal products of a constant sum evaluate; they print as before",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the hash aggregations, and
    // FusedAccumulatorsTest that the column-major fold gives the same counts and sums
    Optimization.ColumnMajorFusedSums ->
      "Changes the order in which fused hash aggregations fold their sums, not the plan",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the partial aggregates, and
    // PartialAggSkipTest that one-row partial results give the same groups
    Optimization.OneRowPartials ->
      "Changes how partial aggregates build their one-row results, not the plan",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the hash aggregations, and
    // SelectedInputsTest that reading deferred columns in place gives the same groups
    Optimization.AggregateSelectedRows ->
      "Changes how hash aggregations read deferred input columns, not the plan",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the projections and hash
    // aggregations, and DenseSelectedRowsTest that computing over the source rows gives the same
    // results
    Optimization.DenseSelectedRows ->
      "Changes how projections and aggregations read a filter's deferred columns",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the hash aggregations, and
    // AggScratchBuffersTest that reused arrays give the same results
    Optimization.AggScratchBuffers ->
      "Changes which arrays hash aggregations compute into, not the plan",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the hash aggregations, and
    // LongGroupTableTest that the table gives the same groups as a GroupTable
    Optimization.LongGroupTables ->
      "Changes how hash aggregations look up single Int64 keys, not the plan",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the hash aggregations, and
    // LongGroupTableTest that grouping runs gives the same groups as hashing
    Optimization.SortedGroupRuns ->
      "Changes how hash aggregations assign ids to sorted single Int64 keys, not the plan",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the partitioned aggregations, and
    // SortedAggSplitsTest that splitting sorted input gives the same groups as hash routing
    Optimization.SortedAggSplits ->
      "Changes how partitioned aggregations split sorted input, not the plan",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the partitioned aggregations, and
    // PartitionedAggOpTest that routing by the packed key gives the same groups
    Optimization.PackedAggRouting ->
      "Changes how partitioned aggregations route rows to partitions, not the plan",
    // EngineAdaptivePartialAggsTest checks that the flag reaches the partitioned aggregations, and
    // PartitionedAggOpTest that sizing by rows gives the same groups
    Optimization.SizePartitionsByRows ->
      "Changes how partitioned aggregations size their group tables, not the plan",
    // EngineDictionaryFiltersTest checks that the flag reaches the filters and that per-entry
    // evaluation keeps the same rows
    Optimization.DictionaryFilters ->
      "Changes how filters evaluate dictionary-coded strings, not the plan",
    // EngineDictionaryFiltersTest checks that the flag reaches the filters, and SelectionTest that
    // per-entry selection keeps the same rows
    Optimization.DictionarySelection ->
      "Changes how filters select rows of dictionary-coded strings, not the plan",
    // MemoryAccountingTest checks that the flag turns reservations and limits off
    Optimization.MemoryAccounting ->
      "Changes whether queries count their memory against a limit, not the plan",
    // ParquetFooterCacheTest checks that the flag reuses parsed footers
    Optimization.ParquetFooterCache -> "Changes how often Parquet footers are parsed, not the plan",
    // EngineMaskedCaseWhenTest checks that the flag reaches the projections, and CaseWhenTest that
    // masking gives the same values as the branch choice
    Optimization.MaskedCaseWhen -> "Changes how CASE expressions build their output, not the plan",
    // EngineCaseAggFiltersTest checks that the flag reaches the CASE expressions, and CaseWhenTest
    // that picking constants gives the same values as the branch choice
    Optimization.ConstCaseSelect -> "Changes how CASE expressions build their output, not the plan",
    // EngineDictionaryAggFiltersTest checks that the flag reaches the aggregates, and
    // PerSharedDictEntryTest that per-entry results give the same values as per-row evaluation
    Optimization.DictionaryAggFilters ->
      "Changes how aggregate FILTERs evaluate dictionary-coded strings, not the plan",
    // EngineRoutedAggFiltersTest checks that the flag reaches the aggregates, and
    // RoutedAggFiltersTest that routing gives the same results as each FILTER's own mask
    Optimization.RoutedAggFilters ->
      "Changes how aggregate FILTERs over one dictionary fold their rows, not the plan",
    // EngineSubstringInListsTest checks that the flag reaches the filters, and SubstringInListTest
    // that region lookups give the same rows as built substrings
    Optimization.SubstringInLists -> "Changes how string IN lists test substrings, not the plan",
    // EngineRuntimeJoinFiltersTest checks that the flag sets per-row filters on probe scans
    Optimization.RowJoinFilters -> "Changes which rows the probe scans emit, not the plan",
    // RankFilterRowsTest checks the estimates. TPC-DS q44 already builds on its rankings at SF0.1;
    // the toggle flips its build side at SF10
    Optimization.RankFilterRows ->
      "Changes TPC-DS q44's join order at SF10, not at the golden plans' SF0.1",
    // SampledStringDistinctTest checks the join order; golden plans' string columns all have counts
    Optimization.SampledStringDistinct ->
      "Changes join order only over string keys of files past 1M rows (TPC-DS q85 at SF100)",
    // FairTaskQueueTest and ThreadPoolRunnerFairTest check the scheduling
    Optimization.FairTaskScheduling -> "Changes which query's pool task runs next, not the plan",
    Optimization.FairCoordinators   ->
      "Changes which query's coordinator task runs next, not the plan",
    // AggSpillTest checks that the flag keeps a spilling aggregation's input in memory
    Optimization.SpillAggregation ->
      "Changes whether aggregations past the spill threshold spill their input, not the plan",
    // JoinSpillTest checks that the flag keeps a spilling join's build in memory
    Optimization.SpillJoin ->
      "Changes whether join builds past the spill threshold spill partitions, not the plan",
    // SpillThresholdTest checks the default threshold with and without the flag
    Optimization.SpillHeadroom -> "Changes the default spill threshold, not the plan",
    // SortSpillTest checks that the flag keeps a spilling sort's input in memory
    Optimization.SpillSort ->
      "Changes whether sorts past the spill threshold write sorted runs, not the plan",
    // WindowSpillTest checks that the flag keeps a spilling window's input in memory
    Optimization.SpillWindow ->
      "Changes whether windows past the spill threshold spill their input, not the plan",
    // MemoryAccountingTest checks that shared results return their memory after their last reader
    Optimization.ReleaseCTEResults ->
      "Changes when shared CTE results drop their rows, not the plan",
    // CTESpillTest checks spilled results against in-memory runs
    Optimization.SpillCTEResults ->
      "Changes whether shared CTE results past the spill threshold go to a file, not the plan",
    // ScanDecodeBudgetTest checks that the flag lets every part run at once
    Optimization.BoundScanDecode -> "Changes how many scan parts decode at once, not the plan"
  )

  override def afterAll: Unit =
    basic.close()
    tpch.close()

  for o <- Optimization.values do
    val options = OptimizerOptions().withDisabled(o)

    test(s"spec/basic without ${o.name}") {
      basic.check(options, Set.empty)
    }

    test(s"spec/tpch without ${o.name}") {
      tpch.check(options, tpchTooSlow.getOrElse(o, Set.empty))
    }

    test(s"disabling ${o.name} changes some TPC-H or TPC-DS plan") {
      if TaskRunner.default.parallelism <= 1 && o == Optimization.Parallelize then
        ignore("Plans are not parallelized on one processor")
      noPlanChange.get(o).foreach(ignore(_))
      val changed =
        if o == Optimization.PrefixLikeSelectivity then
          // Sampled string values estimate the prefix LIKEs of Parquet scans first
          val base = OptimizerOptions().withDisabled(Optimization.StringValueSelectivity)
          EnginePlanGolden.suites.exists(s => s.generate(base.withDisabled(o)) != s.generate(base))
        else if o == Optimization.NestedGroupEstimates then
          // TPC-DS q65's nested aggregation reads its shared CTE's exact row count otherwise
          val base = OptimizerOptions().withDisabled(Optimization.ShareDuplicateSubqueries)
          EnginePlanGolden.suites.exists(s => s.generate(base.withDisabled(o)) != s.generate(base))
        else if withoutCountIn.contains(o) then
          val base0 = OptimizerOptions().withDisabled(Optimization.CountInSubqueries)
          // TPC-H q20's aggregation is on the right of its join once probe scans are charged
          val base =
            if o == Optimization.LeftGroupKeySemiJoins then
              base0.withDisabled(Optimization.ProbeScanCost)
            else
              base0
          EnginePlanGolden.suites.exists(s => s.generate(base.withDisabled(o)) != s.generate(base))
        else
          EnginePlanGolden
            .suites
            .zip(baselinePlans)
            .exists((s, baseline) => s.generate(options) != baseline)
      if !changed then
        fail(s"Disabling ${o.name} changed no plan: is the flag wired to the optimization?")
    }

  end for

end EngineOptimizerToggleTest
