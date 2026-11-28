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

import scala.concurrent.duration.*

/** Per-query memory reservations from a shared pool (`memory_accounting`), on JVM and Scala.js */
class MemoryAccountingTest extends UniTest:
  private val tables = TpchGen.tables(0.01)

  private def engineOf(pool: MemoryPool, opts: OptimizerOptions = OptimizerOptions()) =
    val e = WvletEngine(optimizer = opts).withMemoryPool(pool)
    tables.foreach(e.catalog.register)
    e

  private val compiler = Compiler(
    CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv("."), dbType = DBType.Wvlet)
  )

  /** Compile a query and pass its relation to `f` */
  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
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

  private def run(engine: WvletEngine, query: String): EngineResult =
    withQuery(query)(engine.execute(_))

  /**
    * The bytes the operators of a query reserve while it runs, without its result batches: planned
    * and drained under a context of the test's own
    */
  private def reservedBy(query: String): Long =
    val engine = engineOf(MemoryPool())
    val memory = MemoryPool().newQuery()
    withQuery(query) { r =>
      MemoryContext.withCurrent(memory) {
        val op = engine.physicalPlan(GenSQL.expand(r, summon[Context]))
        try op.drain()
        finally op.close()
      }
    }
    memory.reservedBytes

  private val joinQuery =
    """from lineitem
      |join orders on l_orderkey = o_orderkey
      |select l_orderkey, o_orderdate""".stripMargin

  private lazy val lineitemRows: Long =
    run(engineOf(MemoryPool()), "from lineitem select cnt = _.count").rows.next()(0) match
      case n: Long =>
        n
      case other =>
        fail(s"Unexpected count: ${other}")

  test("should return every reservation to the pool after a query") {
    val pool = MemoryPool()
    val r    = run(engineOf(pool), joinQuery)
    r.numRows.toLong shouldBe lineitemRows
    pool.reservedBytes shouldBe 0L
  }

  test("should fail a query over its memory limit and release its reservation") {
    val pool   = MemoryPool()
    val engine = engineOf(pool).withQueryMemoryLimit(64 * 1024)
    val e      = intercept[WvletLangException] {
      run(engine, joinQuery)
    }
    e.statusCode shouldBe StatusCode.RESOURCE_EXHAUSTED
    e.getMessage shouldContain "memory limit"
    pool.reservedBytes shouldBe 0L
    // The engine still runs queries within the limit
    run(engine, "from orders select o_orderkey limit 3").numRows shouldBe 3
    pool.reservedBytes shouldBe 0L
  }

  test("should count nothing with memory_accounting off") {
    val pool   = MemoryPool()
    val engine = engineOf(pool, OptimizerOptions().withDisabled(Optimization.MemoryAccounting))
      .withQueryMemoryLimit(1024)
    run(engine, joinQuery).numRows.toLong shouldBe lineitemRows
    pool.reservedBytes shouldBe 0L
  }

  test("should count a context's reservations in its pool until it closes") {
    val pool = MemoryPool(capacityBytes = 100)
    val a    = pool.newQuery()
    val b    = pool.newQuery(limitBytes = 50)
    a.reserve(60)
    b.reserve(30)
    pool.reservedBytes shouldBe 90L
    pool.isOverCapacity shouldBe false
    a.reserve(20)
    pool.isOverCapacity shouldBe true
    val e = intercept[WvletLangException] {
      b.reserve(21)
    }
    e.statusCode shouldBe StatusCode.RESOURCE_EXHAUSTED
    b.close()
    b.close()
    pool.reservedBytes shouldBe 80L
    a.close()
    pool.reservedBytes shouldBe 0L
  }

  test("should kill the largest query once the pool stays over its capacity") {
    val pool = MemoryPool(capacityBytes = 100, killDelay = Some(Duration.Zero))
    val a    = pool.newQuery()
    val b    = pool.newQuery()
    a.reserve(80)
    // Over the capacity: the largest query is killed, not the one reserving
    b.reserve(30)
    a.isKilled shouldBe true
    b.isKilled shouldBe false
    val e = intercept[WvletLangException] {
      a.checkKilled()
    }
    e.statusCode shouldBe StatusCode.RESOURCE_EXHAUSTED
    e.getMessage shouldContain "killed to free memory"
    // One victim at a time: none while the killed query is still open
    b.reserve(100)
    b.isKilled shouldBe false
    a.close()
    pool.openQueries shouldBe 1
    // A single query over the capacity frees nothing for others
    b.reserve(100)
    b.isKilled shouldBe false
    b.close()
    pool.reservedBytes shouldBe 0L
    pool.openQueries shouldBe 0
  }

  test("should not kill before the delay or without a delay") {
    Seq(Some(1.hour), None).foreach { delay =>
      val pool = MemoryPool(capacityBytes = 100, killDelay = delay)
      val a    = pool.newQuery()
      val b    = pool.newQuery()
      a.reserve(80)
      b.reserve(30)
      pool.killLargestIfStillOver() shouldBe None
      a.isKilled shouldBe false
      a.close()
      b.close()
    }
  }

  test("should fail a running query the low-memory killer chose") {
    val pool  = MemoryPool(capacityBytes = 1, killDelay = Some(Duration.Zero))
    val other = pool.newQuery()
    other.reserve(1)
    val e = intercept[WvletLangException] {
      run(engineOf(pool), joinQuery)
    }
    e.statusCode shouldBe StatusCode.RESOURCE_EXHAUSTED
    e.getMessage shouldContain "killed to free memory"
    other.isKilled shouldBe false
    pool.reservedBytes shouldBe 1L
    other.close()
  }

  test("should let a query finish when the killer chose another one") {
    val pool  = MemoryPool(capacityBytes = 1, killDelay = Some(Duration.Zero))
    val other = pool.newQuery()
    other.reserve(1L << 40)
    run(engineOf(pool), joinQuery).numRows.toLong shouldBe lineitemRows
    other.isKilled shouldBe true
    other.close()
    pool.reservedBytes shouldBe 0L
  }

  test("should give new engines the process pool and its per-query limit") {
    val prev = MemoryPool.process
    val pool = MemoryPool(queryLimitBytes = 64 * 1024)
    MemoryPool.setProcessPool(pool)
    try
      val engine = WvletEngine()
      tables.foreach(engine.catalog.register)
      engine.memoryPool shouldBeTheSameInstanceAs pool
      intercept[WvletLangException] {
        run(engine, joinQuery)
      }.statusCode shouldBe StatusCode.RESOURCE_EXHAUSTED
      pool.reservedBytes shouldBe 0L
    finally
      MemoryPool.setProcessPool(prev)
  }

  test("should scope the current context to a block") {
    val c = MemoryPool().newQuery()
    MemoryContext.current shouldBeTheSameInstanceAs MemoryContext.NoOp
    MemoryContext.withCurrent(c) {
      MemoryContext.current shouldBeTheSameInstanceAs c
    }
    MemoryContext.current shouldBeTheSameInstanceAs MemoryContext.NoOp
    MemoryContext.NoOp.reserve(Long.MaxValue)
    MemoryContext.NoOp.reservedBytes shouldBe 0L
  }

  test("should estimate vector sizes by width and sampled string lengths") {
    MemoryContext.estimatedBytes(IntVec(PType.Int32, Array(1, 2, 3), null)) shouldBe 12L
    MemoryContext.estimatedBytes(LongVec(PType.Int64, Array(1L, 2L), Array(false, true))) shouldBe
      18L
    // Two 8-byte references plus two strings of 40 bytes of overhead and 5 characters on average
    MemoryContext.estimatedBytes(StrVec(Array("abcd", "efghij"), null)) shouldBe 2L * (8 + 45)
  }

  test("should reserve nothing for a query without large holders") {
    reservedBy("from orders select o_orderkey") shouldBe 0L
  }

  test("should reserve bytes for join builds, aggregations, sorts, windows, and shared CTEs") {
    reservedBy(joinQuery) shouldMatch {
      case n: Long if n > 0 =>
    }
    reservedBy("from orders group by o_custkey select o_custkey, n = _.count") shouldMatch {
      case n: Long if n > 0 =>
    }
    reservedBy("from orders select o_orderkey order by o_orderdate") shouldMatch {
      case n: Long if n > 0 =>
    }
    reservedBy(
      "from orders select o_orderkey, rank() over (partition by o_custkey order by o_orderdate) as r"
    ) shouldMatch {
      case n: Long if n > 0 =>
    }
    reservedBy("""with c as { from orders where o_orderkey < 100 select o_orderkey }
        |from c
        |concat { from c }""".stripMargin) shouldMatch {
      case n: Long if n > 0 =>
    }
  }

  test("should count the groups of a partitioned aggregation's partitions") {
    // Runs tasks on the calling thread, but partitions as a parallel runner does
    object PartitioningRunner extends TaskRunner:
      def parallelism: Int                               = 4
      def submit[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)
    val n      = 10000
    val keys   = LongVec(PType.Int64, Array.tabulate(n)(_.toLong), null)
    val schema = Schema(IndexedSeq(Field("k", PType.Int64)))
    val input  = Batch(Array[Vec](keys), n)
    val memory = MemoryPool().newQuery()
    val op     =
      MemoryContext.withCurrent(memory) {
        PartitionedAggOp(
          BatchesOp(schema, Seq(input)),
          IndexedSeq(ColRef(0, PType.Int64, "k")),
          IndexedSeq(AggCall("count_star", Nil, false, None, PType.Int64)),
          Schema(IndexedSeq(Field("k", PType.Int64), Field("n", PType.Int64))),
          PartitioningRunner,
          minParallelRows = 1
        )
      }
    op.drain().map(_.size).sum shouldBe n
    // The input, and at least 28 bytes per group (key, slot, and count) in the partitions
    memory.reservedBytes shouldMatch {
      case b: Long if b >= MemoryContext.estimatedBytes(input) + 28L * n =>
    }
  }

end MemoryAccountingTest
