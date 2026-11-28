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
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.engine.EngineResult
import wvlet.lang.engine.WvletEngine
import wvlet.lang.engine.catalog.TpchGen
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

/**
  * Shared CTE results past the spill threshold go to one spill file read back by their readers
  * (`spill_cte_results`)
  */
class CTESpillTest extends UniTest:
  private val tables    = TpchGen.tables(0.01)
  private val spillRoot = Files.createTempDirectory("wvlet-cte-spill-test").toFile

  override def afterAll: Unit =
    def rm(f: File): Unit =
      Option(f.listFiles()).foreach(_.foreach(rm))
      f.delete()
    rm(spillRoot)

  // Joins and aggregations keep their input in memory, so only shared results spill
  private val onlyCTEs = OptimizerOptions()
    .withDisabled(Optimization.SpillAggregation)
    .withDisabled(Optimization.SpillJoin)

  private def engineOf(opts: OptimizerOptions = onlyCTEs) =
    val e = WvletEngine(optimizer = opts)
      .withMemoryPool(MemoryPool())
      .withSpillDirectory(spillRoot.getPath)
    tables.foreach(e.catalog.register)
    e

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def run(engine: WvletEngine, query: String): EngineResult =
    val unit = CompilationUnit.fromSqlString(query)
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
    engine.execute(find(unit.executionPlan).get)

  private def sortedRows(r: EngineResult): Seq[String] = r.rows.map(_.mkString("|")).toSeq.sorted

  /** The shared results that `body` wrote to spill files */
  private def spills[A](body: => A): (A, Long) =
    val before = SharedRows.resultsSpilled.get()
    val a      = body
    (a, SharedRows.resultsSpilled.get() - before)

  private def spillFolders: Seq[String] = Option(spillRoot.list()).map(_.toSeq).getOrElse(Nil)

  private val queries = Seq(
    // Two scans of one result, joined: parallel split reads of its blocks
    "with t as (select o_orderkey, o_custkey, o_totalprice, o_orderdate from orders " +
      "where o_totalprice > 1000) " +
      "select a.o_orderkey, b.o_orderkey, b.o_orderdate from t a join t b " +
      "on a.o_custkey = b.o_custkey where a.o_totalprice > b.o_totalprice * 2",
    // Strings and decimals, read by a union of its readers
    "with t as (select c_custkey, c_name, c_acctbal, c_mktsegment from customer) " +
      "select c_name, c_acctbal from t where c_acctbal > 5000 " +
      "union all select c_mktsegment, c_acctbal from t where c_custkey % 3 = 0",
    // A result read by a scalar subquery and by the outer query
    "with t as (select l_orderkey, l_extendedprice, l_shipmode from lineitem " +
      "where l_quantity > 10) " + "select l_shipmode, count(*), sum(l_extendedprice) from t " +
      "where l_extendedprice > (select avg(l_extendedprice) from t) group by l_shipmode",
    // A CTE whose body reads another shared CTE
    "with a as (select o_orderkey, o_custkey from orders where o_orderkey % 2 = 0), " +
      "b as (select a1.o_orderkey, a2.o_custkey from a a1 join a a2 on a1.o_orderkey = " +
      "a2.o_orderkey) select x.o_custkey, count(*) from b x join b y on x.o_orderkey = " +
      "y.o_orderkey group by x.o_custkey",
    // Grouping sets read their input as a shared result
    "select o_orderpriority, o_orderstatus, count(*), sum(o_totalprice) from orders " +
      "group by grouping sets ((o_orderpriority), (o_orderstatus), ())"
  )

  test("should give the same rows when shared results spill as when they fit") {
    val inMemory = engineOf()
    val spilling = Seq(1L, 64L * 1024).map(engineOf().withSpillThreshold(_))
    queries.foreach { q =>
      val expected = sortedRows(run(inMemory, q))
      expected.nonEmpty shouldBe true
      val spilled = spilling.map { e =>
        val (actual, n) = spills(sortedRows(run(e, q)))
        debug(s"${q}: ${n} results spilled")
        actual shouldBe expected
        n
      }
      spilled.head > 0 shouldBe true
    }
    spillFolders shouldBe Nil
  }

  test("should keep shared results in memory with spill_cte_results off") {
    val engine     = engineOf(onlyCTEs.withDisabled(Optimization.SpillCTEResults))
    val (r, count) = spills(run(engine.withSpillThreshold(1), queries.head))
    count shouldBe 0L
    r.numRows > 0 shouldBe true
  }

  test("should spill results with release_cte_results off and delete them at query end") {
    val engine     = engineOf(onlyCTEs.withDisabled(Optimization.ReleaseCTEResults))
    val expected   = sortedRows(run(engineOf(), queries(1)))
    val (r, count) = spills(sortedRows(run(engine.withSpillThreshold(1), queries(1))))
    count > 0 shouldBe true
    r shouldBe expected
    spillFolders shouldBe Nil
  }

  test("should delete a query's spill files when it fails") {
    // The result spills past 1 byte, then the join over its readers meets the 64 KB limit
    val engine     = engineOf().withSpillThreshold(1).withQueryMemoryLimit(64L << 10)
    val (e, count) = spills(intercept[WvletLangException](run(engine, queries.head)))
    e.statusCode shouldBe StatusCode.RESOURCE_EXHAUSTED
    count > 0 shouldBe true
    spillFolders shouldBe Nil
  }

  test("should spill one large batch as several blocks that readers split") {
    val schema = Schema(IndexedSeq(Field("k", PType.Int64)))
    val n      = 2 << 20
    val big    = Batch(Array[Vec](LongVec(PType.Int64, Array.tabulate(n)(_.toLong), null)), n)
    val small  = Batch(Array[Vec](LongVec(PType.Int64, Array(-1L, -2L), null)), 2)
    val memory = MemoryPool().newQuery().withSpill(1, spillRoot.getPath)
    try
      val rows = SharedRows.drain(
        memory.child(),
        IteratorOp(schema, () => Iterator(small, big, small), "input"),
        spill = true
      )
      rows.isSpilled shouldBe true
      rows.rows shouldBe n + 4L
      rows.parts.size > 2 shouldBe true
      rows.parts.map(_._2).sum shouldBe n + 4L
      val keys = rows.iterator.flatMap(b => (0 until b.size).map(b.column(0).get(_))).toSeq
      keys shouldBe (Seq(-1L, -2L) ++ (0L until n) ++ Seq(-1L, -2L))
      memory.reservedBytes shouldBe 0L
    finally
      memory.close()
    spillFolders shouldBe Nil
  }

  test("should read ranges of spill blocks from several threads at once") {
    val schema = Schema(IndexedSeq(Field("k", PType.Int64), Field("s", PType.Str)))
    val blocks = (0 until 8).map { b =>
      val n = 100 + b
      Batch(
        Array[Vec](
          LongVec(PType.Int64, Array.tabulate(n)(i => (b * 1000 + i).toLong), null),
          StrVec(Array.tabulate(n)(i => s"v${b}-${i}"), null)
        ),
        n
      )
    }
    val space = SpillCompat.newSpace(spillRoot.getPath)
    try
      val file = space.newFile(schema)
      blocks.foreach(file.append)
      file.finish()
      file.blockRows shouldBe blocks.map(_.size)
      def keys(bs: Iterator[Batch]): Seq[Long] =
        bs.flatMap(b => (0 until b.size).map(i => b.column(0).get(i).asInstanceOf[Long])).toSeq
      val parts = (0 until 8)
        .map(i =>
          java.util.concurrent.CompletableFuture.supplyAsync(() => keys(file.readBlocks(i, i + 1)))
        )
        .map(_.get())
      parts shouldBe blocks.map(b => keys(Iterator.single(b)))
      keys(file.readBlocks(2, 5)) shouldBe parts.slice(2, 5).flatten
      keys(file.readBlocks(3, 3)) shouldBe Nil
      file.delete()
      intercept[WvletLangException](file.readBlocks(0, 8)).statusCode shouldBe
        StatusCode.UNEXPECTED_STATE
    finally
      space.close()
    spillFolders shouldBe Nil
  }

end CTESpillTest
