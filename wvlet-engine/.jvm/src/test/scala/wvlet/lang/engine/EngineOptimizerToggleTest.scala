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

import wvlet.lang.engine.exec.TaskRunner
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/**
  * Turning off any one planner optimization may change plans but never results: spec/basic and
  * TPC-H, with each optimization disabled in turn, must still return the rows DuckDB returns (as
  * they do with every optimization on, see [[EngineBasicSpecTest]] and [[EngineTPCHTest]]). This
  * catches a query whose correct result secretly depends on an optimization
  */
class EngineOptimizerToggleTest extends UniTest:

  private def check(c: EngineConformance, skip: Set[String]): Unit =
    try
      val failures = c
        .units
        .filterNot(u => skip.contains(u.sourceFile.fileName))
        .flatMap(c.run)
        .filterNot(_.matched)
      if failures.nonEmpty then
        fail(failures.map(r => s"${r.name}: ${r.message}").mkString("\n"))
    finally
      c.close()

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

  for o <- Optimization.values do
    val options = OptimizerOptions().withDisabled(o)

    test(s"spec/basic without ${o.name}") {
      check(EngineConformance("spec/basic", Nil, _ => (), options), EngineBasicSpecTest.knownGaps)
    }

    test(s"spec/tpch without ${o.name}") {
      val tables = EngineTPCHBench.tables
      check(
        EngineConformance(
          "spec/tpch",
          tables.map(t => s"create view ${t} as select * from 'spec/tpch/data/${t}.parquet'"),
          engine =>
            tables.foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet")),
          options
        ),
        tpchTooSlow.getOrElse(o, Set.empty)
      )
    }

    test(s"disabling ${o.name} changes some TPC-H or TPC-DS plan") {
      if TaskRunner.default.parallelism <= 1 && o == Optimization.Parallelize then
        ignore("Plans are not parallelized on one processor")
      if o == Optimization.PrefetchScalarSubqueries then
        // The subqueries become the same constants either way, one query per group or per subquery
        ignore("Changes how scalar subqueries are evaluated while planning, not the plan")
      if o == Optimization.PresizeAggregates then
        // EnginePresizeAggregatesTest checks that the flag sets the aggregations' group estimates
        ignore("Changes the initial size of aggregation tables, not the plan")
      if o == Optimization.ReplanCTEBodies then
        // EngineMaterializedCTEStatsTest checks that the flag changes the plan
        ignore("No TPC-H or TPC-DS shared CTE body reads another shared CTE")
      if o == Optimization.ParallelAggPartitioning then
        // PartitionedAggOpTest checks that both ways give the same partitions
        ignore("Changes how aggregation input is routed to partitions, not the plan")
      if o == Optimization.GpuOffload then
        // EnginePlanAcceleratorTest checks that the flag offers plans to an accelerator
        ignore("Changes plans only with a PlanAccelerator, which these engines don't have")
      if o == Optimization.ConcurrentSharedCTEs then
        // EngineConcurrentSharedCTEsTest checks that the flag starts the results concurrently
        ignore("Changes when shared CTE results are computed, not the plan")
      if o == Optimization.ConcurrentJoinBuilds then
        // EngineConcurrentJoinBuildsTest checks that the flag runs a pipeline's builds as one step
        ignore("Changes when the hash join builds of a pipeline run, not the plan")
      if o == Optimization.RunningWindowKernels then
        // EngineWindowTopNTest checks that the flag reaches the window operators, and WindowOpTest
        // that both folds give the same values
        ignore("Changes how running window aggregates are folded, not the plan")
      if o == Optimization.ScanJoinRowFilters then
        // EngineRuntimeJoinFiltersTest and EngineScanRowFiltersTest check that the flag puts
        // per-row join filters into the probe scans' row filters, with the same rows
        ignore("Changes where probe scans test per-row join filters, not the plan")
      if o == Optimization.WindowSortKernels then
        // EngineWindowTopNTest checks that the flag reaches the window operators, and
        // WindowSortKernelTest that both sorts give the same row order
        ignore("Changes how window partitions are sorted, not the plan")
      if o == Optimization.AdaptivePartialAggs then
        // EngineAdaptivePartialAggsTest checks that the flag reaches the partial aggregates, and
        // PartialAggSkipTest that passing rows through gives the same final groups
        ignore("Changes when partial aggregates stop grouping, not the plan")
      if o == Optimization.PackAggKeys then
        // EngineAdaptivePartialAggsTest checks that the flag reaches the partitioned
        // aggregations, and AggKeyPackingTest that packed keys give the same groups
        ignore("Changes how partitioned aggregations compare keys, not the plan")
      if o == Optimization.StreamPackAggKeys || o == Optimization.LongDecimalKeys then
        // EngineAdaptivePartialAggsTest checks that the flags reach the aggregations, and
        // PackedGroupTableTest that packed and unscaled keys give the same groups
        ignore("Changes how aggregations compare keys, not the plan")
      if o == Optimization.DictionaryGroupKeys then
        // EngineDictionaryGroupKeysTest checks that the flag reaches the scans, and GroupTableTest
        // that grouping by dictionary codes gives the same groups
        ignore("Changes how scans decode strings and aggregations compare them, not the plan")
      if o == Optimization.FusedAccumulators then
        // EngineAdaptivePartialAggsTest checks that the flag reaches the hash aggregations, and
        // FusedAccumulatorsTest that the fused loop gives the same counts and sums
        ignore("Changes how hash aggregations fold their aggregates, not the plan")
      if o == Optimization.ParquetFooterCache then
        // ParquetFooterCacheTest checks that the flag reuses parsed footers
        ignore("Changes how often Parquet footers are parsed, not the plan")
      if o == Optimization.MaskedCaseWhen then
        // EngineMaskedCaseWhenTest checks that the flag reaches the projections, and CaseWhenTest
        // that masking gives the same values as the branch choice
        ignore("Changes how CASE expressions build their output, not the plan")
      if o == Optimization.RowJoinFilters then
        // EngineRuntimeJoinFiltersTest checks that the flag sets per-row filters on probe scans
        ignore("Changes which rows the probe scans emit, not the plan")
      val changed =
        if withoutCountIn.contains(o) then
          val base = OptimizerOptions().withDisabled(Optimization.CountInSubqueries)
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
