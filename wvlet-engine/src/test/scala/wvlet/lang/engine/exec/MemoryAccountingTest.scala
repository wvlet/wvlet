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
import wvlet.lang.compiler.DBType
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

  private def run(engine: WvletEngine, query: String): EngineResult =
    val unit = CompilationUnit.fromWvletString(query)
    val r    = compiler.compileSingleUnit(unit)
    r.reportAllErrors
    given ctx: wvlet.lang.compiler.Context       = r.context
    def find(e: ExecutionPlan): Option[Relation] =
      e match
        case ExecuteQuery(q: Relation) =>
          Some(q)
        case ExecuteTasks(ts) =>
          ts.iterator.flatMap(find).nextOption()
        case _ =>
          None
    engine.execute(find(unit.executionPlan).get)

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

end MemoryAccountingTest
