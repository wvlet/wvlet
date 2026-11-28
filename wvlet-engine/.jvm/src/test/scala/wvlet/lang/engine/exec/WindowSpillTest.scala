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

/** Windows past the spill threshold split their input into spill files (`spill_window`) */
class WindowSpillTest extends UniTest:
  private val tables    = TpchGen.tables(0.01)
  private val spillRoot = Files.createTempDirectory("wvlet-window-spill-test").toFile

  override def afterAll: Unit =
    def rm(f: File): Unit =
      Option(f.listFiles()).foreach(_.foreach(rm))
      f.delete()
    rm(spillRoot)

  private def engineOf(opts: OptimizerOptions = OptimizerOptions()) =
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

  private def sortedRows(r: EngineResult): Seq[String] = r.rows.map(_.mkString("|")).toSeq.sorted

  /** Window inputs the windows of `body` routed to spill files */
  private def spills[A](body: => A): (A, Long) =
    val inputs = WindowSpill.inputsSpilled.get()
    val a      = body
    (a, WindowSpill.inputsSpilled.get() - inputs)

  private def spillFolders: Seq[String] = Option(spillRoot.list()).map(_.toSeq).getOrElse(Nil)

  private val queries = Seq(
    // Running and whole-partition aggregates, ranks, two call groups sharing l_orderkey
    "from lineitem select l_orderkey, l_linenumber, " +
      "sum(l_quantity) over (partition by l_orderkey order by l_linenumber) as s, " +
      "avg(l_extendedprice) over (partition by l_orderkey) as a, " +
      "rank() over (partition by l_orderkey, l_returnflag order by l_shipdate, l_linenumber) as r",
    // String partition keys and a sliding frame
    "from orders select o_orderkey, o_orderpriority, " +
      "row_number() over (partition by o_orderpriority, o_orderstatus order by o_orderkey) as rn, " +
      "sum(o_totalprice) over (partition by o_orderpriority order by o_orderkey " +
      "rows [-2, 0]) as t",
    // A few large partitions (q47/q89 shape: partitions by a low-cardinality key)
    "from lineitem select l_orderkey, l_linenumber, " +
      "max(l_extendedprice) over (partition by l_shipmode order by l_orderkey, l_linenumber) as m"
  )

  test("should give the same rows when windows spill as when they fit") {
    val inMemory = engineOf()
    val spilling = engineOf().withSpillThreshold(256L * 1024)
    queries.foreach { q =>
      val expected         = sortedRows(run(inMemory, q))
      val (actual, inputs) = spills(sortedRows(run(spilling, q)))
      debug(s"${q}: ${inputs} inputs spilled")
      inputs > 0 shouldBe true
      actual shouldBe expected
    }
    spillFolders shouldBe Nil
  }

  test("should split a spilled file again when it does not fit") {
    // No headroom at all: the window over each file spills once more, then holds its input
    val q                = queries.head
    val expected         = sortedRows(run(engineOf(), q))
    val (actual, inputs) = spills(sortedRows(run(engineOf().withSpillThreshold(1), q)))
    inputs > 1 shouldBe true
    actual shouldBe expected
    spillFolders shouldBe Nil
  }

  test("should hold windows without a shared PARTITION BY key in memory") {
    val q =
      "from orders select o_orderkey, " + "row_number() over (order by o_orderkey) as rn, " +
        "rank() over (partition by o_custkey order by o_orderdate) as r"
    val expected         = sortedRows(run(engineOf(), q))
    val (actual, inputs) = spills(sortedRows(run(engineOf().withSpillThreshold(1), q)))
    inputs shouldBe 0L
    actual shouldBe expected
  }

  test("should return every reservation of a spilled window once drained") {
    val engine         = engineOf()
    val memory         = MemoryPool().newQuery().withSpill(256L * 1024, spillRoot.getPath)
    val (rows, inputs) = spills {
      withRelation(queries.head) { r =>
        MemoryContext.withCurrent(memory) {
          val op = engine.physicalPlan(GenSQL.expand(r, summon[Context]))
          try op.drain().map(_.size).sum
          finally op.close()
        }
      }
    }
    inputs > 0 shouldBe true
    rows > 0 shouldBe true
    memory.reservedBytes shouldBe 0L
    memory.close()
    spillFolders shouldBe Nil
  }

  test("should hold the input in memory with spill_window off") {
    val engine = engineOf(OptimizerOptions().withDisabled(Optimization.SpillWindow))
      .withSpillThreshold(1)
    val (r, inputs) = spills(run(engine, queries.head))
    inputs shouldBe 0L
    r.numRows > 0 shouldBe true
  }

  test("should delete a query's spill files when it fails") {
    val engine = engineOf().withSpillThreshold(1).withQueryMemoryLimit(1L << 20)
    val e      = intercept[WvletLangException](run(engine, queries.head))
    e.statusCode shouldBe StatusCode.RESOURCE_EXHAUSTED
    spillFolders shouldBe Nil
  }

end WindowSpillTest
