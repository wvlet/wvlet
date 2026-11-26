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

import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.engine.exec.HashAggOp
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.exec.PartitionedAggOp
import wvlet.lang.engine.exec.ProjectOp
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/**
  * `adaptive_partial_aggs` reaches the partial aggregates of two-phase aggregations, and
  * `pack_agg_keys` the partitioned aggregations, and `stream_pack_agg_keys` / `long_decimal_keys`
  * every aggregation, and `fused_accumulators` the hash aggregations, `one_row_partials` the
  * partial aggregates; their results are checked in PartialAggSkipTest, AggKeyPackingTest,
  * PackedGroupTableTest, and FusedAccumulatorsTest
  */
class EngineAdaptivePartialAggsTest extends UniTest:
  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))
  private val options  = OptimizerOptions()

  /** `flag` of each operator it applies to in the physical plan of a grouped lineitem query */
  private def flags(o: OptimizerOptions, flag: PartialFunction[Operator, Boolean]): Seq[Boolean] =
    val unit = CompilationUnit.fromWvletString("""from 'spec/tpch/data/lineitem.parquet'
        |group by l_orderkey, l_linenumber
        |agg sum(l_quantity) as q""".stripMargin)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val query     = EngineTPCHBench.queryOf(unit).getOrElse(fail("No query"))
    def collect(op: Operator): Seq[Boolean] = flag.lift(op).toSeq ++ op.children.flatMap(collect)
    collect(WvletEngine(optimizer = o).physicalPlan(GenSQL.expand(query, summon[Context])))

  test("give the partial aggregates of a two-phase aggregation a skip decision") {
    val skips: PartialFunction[Operator, Boolean] = { case a: HashAggOp =>
      a.skip != null
    }
    flags(options, skips) shouldContain true
    flags(options.withDisabled(Optimization.AdaptivePartialAggs), skips).contains(true) shouldBe
      false
  }

  test("pack the keys of partitioned aggregations with pack_agg_keys") {
    val packs: PartialFunction[Operator, Boolean] = { case a: PartitionedAggOp =>
      a.packKeys
    }
    flags(options, packs) shouldBe Seq(true)
    flags(options.withDisabled(Optimization.PackAggKeys), packs) shouldBe Seq(false)
  }

  test("pack the keys of streaming aggregations with stream_pack_agg_keys") {
    val packs: PartialFunction[Operator, Boolean] = { case a: HashAggOp =>
      a.packKeys
    }
    flags(options, packs) shouldContain true
    flags(options, packs).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.StreamPackAggKeys), packs).contains(true) shouldBe false
  }

  test("key decimals by unscaled values with long_decimal_keys") {
    val decimals: PartialFunction[Operator, Boolean] = {
      case a: HashAggOp =>
        a.longDecimalKeys
      case a: PartitionedAggOp =>
        a.longDecimalKeys
    }
    flags(options, decimals).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.LongDecimalKeys), decimals).contains(true) shouldBe
      false
  }

  test("fold the aggregates of hash aggregations in one pass with fused_accumulators") {
    val fused: PartialFunction[Operator, Boolean] = {
      case a: HashAggOp =>
        a.fuseAccumulators
      case a: PartitionedAggOp =>
        a.fuseAccumulators
    }
    flags(options, fused) shouldContain true
    flags(options, fused).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.FusedAccumulators), fused).contains(true) shouldBe false
  }

  test("fold fused sums one at a time with column_major_fused_sums") {
    val columnMajor: PartialFunction[Operator, Boolean] = {
      case a: HashAggOp =>
        a.columnMajorSums
      case a: PartitionedAggOp =>
        a.columnMajorSums
    }
    flags(options, columnMajor) shouldContain true
    flags(options, columnMajor).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.ColumnMajorFusedSums), columnMajor).contains(
      true
    ) shouldBe false
  }

  test("build one-row partial results with one_row_partials") {
    val oneRows: PartialFunction[Operator, Boolean] = {
      case a: HashAggOp if a.skip != null =>
        a.oneRowPartials
    }
    flags(options, oneRows) shouldContain true
    flags(options, oneRows).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.OneRowPartials), oneRows).contains(true) shouldBe false
  }

  test("read deferred aggregation inputs in place with aggregate_selected_rows") {
    val selected: PartialFunction[Operator, Boolean] = { case a: HashAggOp =>
      a.selectedInputs
    }
    flags(options, selected) shouldContain true
    flags(options, selected).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.AggregateSelectedRows), selected).contains(
      true
    ) shouldBe false
  }

  test("compute over a filter's source rows with dense_selected_rows") {
    val dense: PartialFunction[Operator, Boolean] = {
      case a: HashAggOp =>
        a.denseSelected
      case p: ProjectOp =>
        p.denseSelected
    }
    flags(options, dense) shouldContain true
    flags(options, dense).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.DenseSelectedRows), dense).contains(true) shouldBe false
  }

  test("group single Int64 keys through long group tables with long_group_tables") {
    val long: PartialFunction[Operator, Boolean] = {
      case a: HashAggOp =>
        a.longGroupTables
      case a: PartitionedAggOp =>
        a.longGroupTables
    }
    flags(options, long) shouldContain true
    flags(options, long).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.LongGroupTables), long).contains(true) shouldBe false
  }

  test("group runs of sorted keys without hashing with sorted_group_runs") {
    val runs: PartialFunction[Operator, Boolean] = {
      case a: HashAggOp =>
        a.sortedGroupRuns
      case a: PartitionedAggOp =>
        a.sortedGroupRuns
    }
    flags(options, runs) shouldContain true
    flags(options, runs).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.SortedGroupRuns), runs).contains(true) shouldBe false
  }

  test("split sorted input of partitioned aggregations with sorted_agg_splits") {
    val splits: PartialFunction[Operator, Boolean] = { case a: PartitionedAggOp =>
      a.sortedSplits
    }
    flags(options, splits) shouldContain true
    flags(options, splits).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.SortedAggSplits), splits).contains(true) shouldBe false
  }

  test("route partitioned aggregation rows by a packed key with packed_agg_routing") {
    val packed: PartialFunction[Operator, Boolean] = { case a: PartitionedAggOp =>
      a.packedRouting
    }
    flags(options, packed) shouldContain true
    flags(options, packed).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.PackedAggRouting), packed).contains(true) shouldBe false
  }

  test("size partitioned aggregation tables by rows with size_partitions_by_rows") {
    val sized: PartialFunction[Operator, Boolean] = { case a: PartitionedAggOp =>
      a.sizeByRows
    }
    flags(options, sized) shouldContain true
    flags(options, sized).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.SizePartitionsByRows), sized).contains(true) shouldBe
      false
  }

  test("build lean aggregation results with lean_agg_results") {
    val lean: PartialFunction[Operator, Boolean] = {
      case a: HashAggOp =>
        a.leanResults
      case a: PartitionedAggOp =>
        a.leanResults
    }
    flags(options, lean) shouldContain true
    flags(options, lean).contains(false) shouldBe false
    flags(options.withDisabled(Optimization.LeanAggResults), lean).contains(true) shouldBe false
  }

end EngineAdaptivePartialAggsTest
