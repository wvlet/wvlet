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
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/**
  * Filters bounding a row_number() or rank() keep the top rows of each partition (`window_top_n`),
  * with the rows DuckDB returns, on the sequential and parallel window paths, with and without
  * streamed chunks (`window_top_n_stream`)
  */
class EngineWindowTopNTest extends UniTest:
  private val dir      = "wvlet-engine/.jvm/src/test/resources/engine-spec/window-top-n"
  private val options  = OptimizerOptions().withValidatePlans(true)
  private val minRows  = "wvlet.engine.parallelWindowMinRows"
  private val variants = Seq(
    ""                      -> EngineConformance(dir, Nil, _ => (), options),
    " without window_top_n" ->
      EngineConformance(dir, Nil, _ => (), options.withDisabled(Optimization.WindowTopN)),
    " without window_top_n_stream" ->
      EngineConformance(dir, Nil, _ => (), options.withDisabled(Optimization.WindowTopNStream))
  )

  override def afterAll: Unit =
    sys.props.remove(minRows)
    variants.foreach(_._2.close())

  for
    (label, c) <- variants
    parallel   <- Seq(false, true)
    unit       <- c.units
  do
    val mode =
      if parallel then
        " (parallel)"
      else
        ""
    test(s"${unit.sourceFile.fileName}${label}${mode}") {
      if parallel then
        sys.props(minRows) = "1"
      else
        sys.props.remove(minRows)
      val results = c.run(unit)
      results.size shouldBe 8
      results.foreach { r =>
        debug(s"${r.name}: ${r.message}")
        if !r.matched || r.message.startsWith("skipped") then
          fail(s"${r.name}: ${r.message}")
      }
    }

  test("keep the top rows of each partition only under a bound on a ranking call") {
    val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))
    def explain(q: String, o: OptimizerOptions = options): String =
      val unit      = CompilationUnit.fromWvletString(q)
      val r         = compiler.compileSingleUnit(unit)
      given Context = r.context
      val query     = EngineTPCHBench.queryOf(unit).getOrElse(fail("No query"))
      WvletEngine(optimizer = o).explain(query)
    val ranked =
      """from 'spec/tpch/data/lineitem.parquet'
        |select l_returnflag, l_quantity,
        |  rank() over (partition by l_returnflag order by l_quantity desc) as rk
        |where rk <= 3""".stripMargin
    explain(ranked) shouldContain "top=3 with ties, streamed"
    explain(ranked, options.withDisabled(Optimization.WindowTopNStream)) shouldNotContain "streamed"
    explain(ranked, options.withDisabled(Optimization.WindowTopN)) shouldNotContain "top="
    // Window expressions repeated across calls aren't shared, which would hide the bound's window
    explain("""from 'spec/tpch/data/lineitem.parquet'
        |select l_returnflag,
        |  row_number() over (partition by l_returnflag order by l_quantity * l_tax) as rn,
        |  rank() over (partition by l_returnflag order by l_quantity * l_tax) as rk
        |where rn <= 3""".stripMargin) shouldContain "top=3"
    // A window aggregate needs every row of the partition
    explain("""from 'spec/tpch/data/lineitem.parquet'
        |select l_returnflag, rank() over (partition by l_returnflag order by l_quantity) as rk,
        |  sum(l_quantity) over (partition by l_returnflag) as total
        |where rk <= 3""".stripMargin) shouldNotContain "top="
    // dense_rank() <= k may keep more than k distinct rows' worth of ties: not bounded
    explain("""from 'spec/tpch/data/lineitem.parquet'
        |select l_returnflag, dense_rank() over (order by l_quantity) as rk
        |where rk <= 3""".stripMargin) shouldNotContain "top="
  }

  test("pass the window kernel and partition sharing flags to the window operators") {
    val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))
    def windowKernels(o: OptimizerOptions): Seq[(Boolean, Boolean, Boolean)] =
      val unit = CompilationUnit.fromWvletString(
        """from 'spec/tpch/data/lineitem.parquet'
          |select l_orderkey,
          |  sum(l_quantity) over (partition by l_orderkey order by l_linenumber) as running"""
          .stripMargin
      )
      val r         = compiler.compileSingleUnit(unit)
      given Context = r.context
      val query     = EngineTPCHBench.queryOf(unit).getOrElse(fail("No query"))
      def collect(op: wvlet.lang.engine.exec.Operator): Seq[(Boolean, Boolean, Boolean)] =
        (
          op match
            case w: wvlet.lang.engine.exec.WindowOp =>
              Seq((w.runningKernels, w.sortKernels, w.sharePartitions))
            case _ =>
              Nil
        ) ++ op.children.flatMap(collect)
      collect(WvletEngine(optimizer = o).physicalPlan(GenSQL.expand(query, summon[Context])))
    windowKernels(options) shouldBe Seq((true, true, true))
    windowKernels(options.withDisabled(Optimization.RunningWindowKernels)) shouldBe
      Seq((false, true, true))
    windowKernels(options.withDisabled(Optimization.WindowSortKernels)) shouldBe
      Seq((true, false, true))
    windowKernels(options.withDisabled(Optimization.WindowSharePartitions)) shouldBe
      Seq((true, true, false))
  }

  test("share the partitions of window calls over the same window after lowering") {
    val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))
    val unit     = CompilationUnit.fromWvletString("""from 'spec/tpch/data/lineitem.parquet'
        |select l_orderkey,
        |  max(l_quantity) over (partition by l_orderkey order by l_linenumber) as q,
        |  max(l_extendedprice) over (partition by l_orderkey order by l_linenumber) as p,
        |  rank() over (partition by l_orderkey order by l_quantity) as r""".stripMargin)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val query     = EngineTPCHBench.queryOf(unit).getOrElse(fail("No query"))
    def windows(op: wvlet.lang.engine.exec.Operator): Seq[wvlet.lang.engine.exec.WindowOp] =
      (
        op match
          case w: wvlet.lang.engine.exec.WindowOp =>
            Seq(w)
          case _ =>
            Nil
      ) ++ op.children.flatMap(windows)
    val ws = windows(
      WvletEngine(optimizer = options).physicalPlan(GenSQL.expand(query, summon[Context]))
    )
    ws.map(w =>
      wvlet.lang.engine.exec.WindowOp.callGroups(w.calls, w.sharePartitions).map(_.size)
    ) shouldBe Seq(Seq(2, 1))
  }

end EngineWindowTopNTest
