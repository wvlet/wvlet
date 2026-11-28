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

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.engine.EngineResult
import wvlet.lang.engine.WvletEngine
import wvlet.lang.engine.catalog.TpchGen
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.*
import wvlet.lang.model.plan.ExecuteQuery
import wvlet.lang.model.plan.ExecuteTasks
import wvlet.lang.model.plan.ExecutionPlan
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

import java.io.File
import java.nio.file.Files

/** Sorts past the spill threshold write sorted runs and merge them (`spill_sort`) */
class SortSpillTest extends UniTest:
  private val tables    = TpchGen.tables(0.01)
  private val spillRoot = Files.createTempDirectory("wvlet-sort-spill-test").toFile

  override def afterAll: Unit =
    def rm(f: File): Unit =
      Option(f.listFiles()).foreach(_.foreach(rm))
      f.delete()
    rm(spillRoot)

  private def engineOf(
      opts: OptimizerOptions = OptimizerOptions(),
      tables: Seq[wvlet.lang.engine.catalog.MemTable] = tables
  ) =
    val e = WvletEngine(optimizer = opts)
      .withMemoryPool(MemoryPool())
      .withSpillDirectory(spillRoot.getPath)
    tables.foreach(e.catalog.register)
    e

  private val compiler = Compiler(
    CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv("."), dbType = DBType.Wvlet)
  )

  private def run(engine: WvletEngine, query: String): EngineResult =
    withRelation(query)(engine.execute(_))

  private def withRelation[A](query: String)(f: Context ?=> Relation => A): A =
    val unit = CompilationUnit.fromWvletString(query)
    val r    = compiler.compileSingleUnit(unit)
    r.reportAllErrors
    given ctx: Context                           = r.context
    def find(e: ExecutionPlan): Option[Relation] =
      e match
        case ExecuteQuery(q: Relation) =>
          Some(q)
        case ExecuteTasks(ts) =>
          ts.iterator.flatMap(find).nextOption()
        case _ =>
          None
    f(find(unit.executionPlan).get)

  private def rowsOf(r: EngineResult): Seq[String] = r.rows.map(_.mkString("|")).toSeq

  /** Runs written, top cuts, and merge passes of the sorts of `body` */
  private def spills[A](body: => A): (A, Long, Long, Long) =
    val runs   = SortSpill.runsWritten.get()
    val cuts   = SortSpill.topCuts.get()
    val passes = SortSpill.mergePasses.get()
    val a      = body
    (
      a,
      SortSpill.runsWritten.get() - runs,
      SortSpill.topCuts.get() - cuts,
      SortSpill.mergePasses.get() - passes
    )

  private def spillFolders: Seq[String] = Option(spillRoot.list()).map(_.toSeq).getOrElse(Nil)

  // Total orders, so the rows must come back in the same order
  private val queries = Seq(
    "from lineitem order by l_shipdate desc, l_orderkey, l_linenumber " +
      "select l_orderkey, l_linenumber, l_shipdate, l_comment",
    "from orders order by o_comment, o_orderkey select o_orderkey, o_comment, o_totalprice",
    "from lineitem order by l_extendedprice desc, l_discount, l_orderkey, l_linenumber " +
      "select l_orderkey, l_linenumber, l_extendedprice, l_discount",
    "from lineitem select l_orderkey, l_linenumber, " +
      "x = if l_quantity > 25 then null else l_quantity " +
      "order by x desc, l_orderkey, l_linenumber"
  )

  test("should return the same rows in the same order when sorts spill as when they fit") {
    val inMemory = engineOf()
    // Under a 256 KB threshold every sort writes runs
    val spilling = engineOf().withSpillThreshold(256L * 1024)
    queries.foreach { q =>
      val expected             = rowsOf(run(inMemory, q))
      val (actual, runs, _, _) = spills(rowsOf(run(spilling, q)))
      debug(s"${q}: ${runs} runs")
      runs > 1 shouldBe true
      actual shouldBe expected
    }
    spillFolders shouldBe Nil
  }

  test("should keep ties in input order across runs") {
    // The sort key has three values, so most rows tie: the stable order must hold across runs
    val q = "from lineitem select l_returnflag, l_orderkey, l_linenumber order by l_returnflag"
    val expected             = rowsOf(run(engineOf(), q))
    val (actual, runs, _, _) = spills(rowsOf(run(engineOf().withSpillThreshold(256L * 1024), q)))
    runs > 1 shouldBe true
    actual.map(_.takeWhile(_ != '|')) shouldBe expected.map(_.takeWhile(_ != '|'))
    actual.sorted shouldBe expected.sorted
  }

  // 10 batches of 1000 rows: k has many ties and NULLs, id is the input position
  private val opSchema = Schema(
    IndexedSeq(Field("k", PType.Int64), Field("s", PType.Str), Field("id", PType.Int64))
  )

  private val opBatches: Seq[Batch] = (0 until 10).map { b =>
    val n = 1000
    val r = Array.tabulate(n)(i => b * n + i)
    Batch(
      Array(
        LongVec(PType.Int64, r.map(i => ((i * 7919) % 37).toLong), r.map(_ % 17 == 0)),
        StrVec(r.map(i => s"s${(i * 31) % 11}"), r.map(_ % 23 == 0)),
        LongVec(PType.Int64, r.map(_.toLong), null)
      ),
      n
    )
  }

  private val opKeys = Seq(
    SortKey(ColRef(0, PType.Int64, "k"), ascending = false, nullsFirst = false),
    SortKey(ColRef(1, PType.Str, "s"), ascending = true, nullsFirst = true)
  )

  // The rows of a SortOp over the fixture, planned under a query spilling past `threshold`
  private def sortOp(limit: Option[Long], threshold: Long, runner: TaskRunner): Seq[String] =
    val ctx = MemoryPool().newQuery().withSpill(threshold, spillRoot.getPath)
    try
      val op =
        MemoryContext.withCurrent(ctx)(
          SortOp(BatchesOp(opSchema, opBatches), opKeys, limit, runner, minParallelRows = 2000)
        )
      try op
          .drain()
          .flatMap(b => (0 until b.size).map(i => b.row(i).map(String.valueOf).mkString(",")))
      finally op.close()
    finally
      ctx.close()

  test("should sort ties and NULLs stably when the sort spills, with and without a limit") {
    val runner = ThreadPoolRunner(4)
    try Seq(None, Some(10L), Some(3000L)).foreach { limit =>
        Seq(TaskRunner.Sequential, runner).foreach { r =>
          val expected                = sortOp(limit, Long.MaxValue - 1, r)
          val (actual, runs, cuts, _) = spills(sortOp(limit, 16L * 1024, r))
          debug(s"limit ${limit}, parallelism ${r.parallelism}: ${runs} runs, ${cuts} cuts")
          actual shouldBe expected
          limit match
            case Some(10L) =>
              // A small limit keeps its top rows in memory
              cuts > 0 shouldBe true
              runs shouldBe 0L
            case _ =>
              runs > 1 shouldBe true
        }
      }
    finally runner.shutdown()
    spillFolders shouldBe Nil
  }

  test("should write runs cut to a large limit") {
    val q =
      "from lineitem order by l_extendedprice desc, l_orderkey, l_linenumber " +
        "select l_orderkey, l_linenumber, l_extendedprice limit 20000"
    val expected             = rowsOf(run(engineOf(), q))
    val (actual, runs, _, _) = spills(rowsOf(run(engineOf().withSpillThreshold(256L * 1024), q)))
    runs > 1 shouldBe true
    actual shouldBe expected
  }

  test("should merge more than MaxFanIn runs in passes") {
    // A 1-byte threshold makes every input batch a run
    val big = TpchGen.tables(0.05)
    val q   =
      "from lineitem order by l_partkey, l_orderkey, l_linenumber " +
        "select l_partkey, l_orderkey, l_linenumber"
    val expected                  = rowsOf(run(engineOf(tables = big), q))
    val (actual, runs, _, passes) = spills(
      rowsOf(run(engineOf(tables = big).withSpillThreshold(1), q))
    )
    debug(s"${runs} runs, ${passes} merge passes")
    runs > SortSpill.MaxFanIn shouldBe true
    passes > 0 shouldBe true
    actual shouldBe expected
    spillFolders shouldBe Nil
  }

  test("should return every reservation of a spilled sort once drained") {
    val engine             = engineOf()
    val memory             = MemoryPool().newQuery().withSpill(256L * 1024, spillRoot.getPath)
    val (rows, runs, _, _) = spills {
      withRelation(queries.head) { r =>
        MemoryContext.withCurrent(memory) {
          val op = engine.physicalPlan(GenSQL.expand(r, summon[Context]))
          try op.drain().map(_.size).sum
          finally op.close()
        }
      }
    }
    runs > 1 shouldBe true
    rows > 0 shouldBe true
    memory.reservedBytes shouldBe 0L
    memory.close()
    spillFolders shouldBe Nil
  }

  test("should hold the input in memory with spill_sort off") {
    val engine = engineOf(OptimizerOptions().withDisabled(Optimization.SpillSort))
      .withSpillThreshold(1)
    val (r, runs, cuts, _) = spills(run(engine, queries.head))
    runs shouldBe 0L
    cuts shouldBe 0L
    r.numRows > 0 shouldBe true
  }

  test("should delete a query's spill files when it fails") {
    // Spills past 1 byte, then fails at the 1 MB limit
    val engine = engineOf().withSpillThreshold(1).withQueryMemoryLimit(1L << 20)
    val e      = intercept[WvletLangException](run(engine, queries.head))
    e.statusCode shouldBe StatusCode.RESOURCE_EXHAUSTED
    spillFolders shouldBe Nil
  }

end SortSpillTest
