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
import wvlet.lang.engine.exec.MorselOp
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
  * `slice_single_batch_probes` reaches the engine's morsels, and a query of TPC-DS q65's shape
  * returns the same rows with it on and off. SliceSingleBatchProbesTest checks the row ranges
  */
class EngineSliceSingleBatchProbesTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(options.withDisabled(Optimization.SliceSingleBatchProbes))

  // 100K rows in row groups of 10K: 25 groups of k, each matching 4000 rows
  private lazy val fact: String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve("slice_single_batch_probes.parquet")
    Files.deleteIfExists(path)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(s"""copy (
             |  select range::bigint as id, (range % 25)::bigint as k,
             |    (range % 1000)::decimal(15, 2) as amount
             |  from range(100000)
             |) to '${path}' (format parquet, row_group_size 10000)""".stripMargin)
      finally st.close()
    finally
      conn.close()
    path.toString

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  // TPC-DS q65's shape: each row's revenue in a CTE (100K rows), each k's average revenue over it,
  // and the CTE's rows of k below a tenth of it, joined with each id's row (as q65 joins item)
  private val query =
    """with r as { from $f group by k, id select k, id, rev = amount.sum }
      |from { from r group by k select k, ave = rev.avg } as b
      |join r on b.k = r.k
      |join $f as d on r.id = d.id
      |where r.rev < b.ave * 0.1
      |select r.id, r.k, r.rev, d.amount""".stripMargin

  private def morsels(op: Operator): Seq[MorselOp] =
    val self =
      op match
        case m: MorselOp =>
          Seq(m)
        case _ =>
          Nil
    self ++ (op.children ++ op.sharedInputs).flatMap(morsels)

  /** The rows, sorted, and the morsels of the plan once drained */
  private def run(e: WvletEngine): (Seq[String], Seq[MorselOp]) =
    val unit           = CompilationUnit.fromWvletString(query.replace("$f", s"'${fact}'"))
    val r              = compiler.compileSingleUnit(unit)
    given ctx: Context = r.context
    val q              = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val op             = e.physicalPlan(GenSQL.expand(q, ctx))
    try
      val rows = op
        .drain()
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).mkString(",")))
      debug(op.explain())
      (rows.sorted, morsels(op))
    finally
      op.close()

  test("return the same rows with the toggle on and off") {
    val (rowsOn, morselsOn)   = run(on)
    val (rowsOff, morselsOff) = run(off)
    // Revenues (each id's amount) below a tenth of each k's average (~499.5): 0 to 49, 4 per k
    rowsOn.size shouldBe 25 * 50 * 4
    rowsOn shouldBe rowsOff
    morselsOn.exists(_.slicesOneBatch) shouldBe true
    morselsOff.exists(_.slicesOneBatch) shouldBe false
    morselsOff.map(_.slicedParts).sum shouldBe 0
  }

end EngineSliceSingleBatchProbesTest
