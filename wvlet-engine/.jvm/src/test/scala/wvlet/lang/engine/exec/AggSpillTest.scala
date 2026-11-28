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

/** Partitioned aggregations past the spill threshold spill their input (`spill_aggregation`) */
class AggSpillTest extends UniTest:
  private val tables    = TpchGen.tables(0.01)
  private val spillRoot = Files.createTempDirectory("wvlet-agg-spill-test").toFile

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

  /** Spill files and splits the aggregations of `body` made */
  private def spills[A](body: => A): (A, Long, Long) =
    val files  = AggSpill.filesWritten.get()
    val splits = AggSpill.filesSplit.get()
    val a      = body
    (a, AggSpill.filesWritten.get() - files, AggSpill.filesSplit.get() - splits)

  private def spillFolders: Seq[String] = Option(spillRoot.list()).map(_.toSeq).getOrElse(Nil)

  private val queries = Seq(
    "from lineitem group by l_orderkey select l_orderkey, n = _.count, q = l_quantity.sum, " +
      "p = l_extendedprice.avg",
    "from lineitem group by l_shipinstruct, l_shipmode, l_returnflag select l_shipinstruct, " +
      "l_shipmode, l_returnflag, n = _.count, c = l_comment.max",
    "from lineitem group by l_shipdate, l_linestatus select l_shipdate, l_linestatus, " +
      "d = l_discount.sum, s = l_suppkey.min",
    "from orders group by o_custkey, o_orderpriority select o_custkey, o_orderpriority, " +
      "n = _.count, t = o_totalprice.sum"
  )

  test("should give the same groups when aggregations spill as when they fit") {
    val inMemory = engineOf()
    // Under a 256 KB threshold every aggregation spills, and the partitions fit in waves
    val spilling = engineOf().withSpillThreshold(256L * 1024)
    queries.foreach { q =>
      val expected               = sortedRows(run(inMemory, q))
      val (actual, files, split) = spills(sortedRows(run(spilling, q)))
      debug(s"${q}: ${files} spill files, ${split} split")
      files > 0 shouldBe true
      actual shouldBe expected
    }
    spillFolders shouldBe Nil
  }

  test("should split a spilled partition that does not fit alone") {
    val inMemory = engineOf()
    // No headroom at all: every partition is split once, then runs anyway
    val spilling               = engineOf().withSpillThreshold(1)
    val q                      = queries.head
    val expected               = sortedRows(run(inMemory, q))
    val (actual, files, split) = spills(sortedRows(run(spilling, q)))
    split > 0 shouldBe true
    files > split shouldBe true
    actual shouldBe expected
    spillFolders shouldBe Nil
  }

  test("should return every reservation of a spilled aggregation once drained") {
    val engine           = engineOf()
    val memory           = MemoryPool().newQuery().withSpill(256L * 1024, spillRoot.getPath)
    val (rows, files, _) = spills {
      withRelation(queries.head) { r =>
        MemoryContext.withCurrent(memory) {
          val op = engine.physicalPlan(GenSQL.expand(r, summon[Context]))
          try op.drain().map(_.size).sum
          finally op.close()
        }
      }
    }
    files > 0 shouldBe true
    rows > 0 shouldBe true
    memory.reservedBytes shouldBe 0L
    memory.close()
    spillFolders shouldBe Nil
  }

  test("should hold the input in memory with spill_aggregation off") {
    val engine = engineOf(OptimizerOptions().withDisabled(Optimization.SpillAggregation))
      .withSpillThreshold(1)
    val (r, files, _) = spills(run(engine, queries.head))
    files shouldBe 0L
    r.numRows > 0 shouldBe true
  }

  test("should delete a query's spill files when it fails") {
    // Spills past 1 byte, then fails at the 1 MB limit while routing or reading back
    val engine = engineOf().withSpillThreshold(1).withQueryMemoryLimit(1L << 20)
    val e      = intercept[WvletLangException](run(engine, queries.head))
    e.statusCode shouldBe StatusCode.RESOURCE_EXHAUSTED
    spillFolders shouldBe Nil
  }

  test("should delete the spill files when the query's memory context closes") {
    val ctx    = MemoryPool().newQuery().withSpill(1, spillRoot.getPath)
    val schema = Schema(IndexedSeq(Field("i", PType.Int64)))
    val f      = ctx.spillSpace.newFile(schema)
    f.append(Batch(Array(LongVec(PType.Int64, Array(1L, 2L), null)), 2))
    f.finish()
    f.read().map(_.size).sum shouldBe 2
    ctx.spilledBytes > 0 shouldBe true
    spillFolders.size shouldBe 1
    ctx.close()
    spillFolders shouldBe Nil
    intercept[WvletLangException](ctx.spillSpace)
    intercept[WvletLangException](f.read())
  }

  test("should make the spill folder readable by its owner only") {
    val space = FileSpillSpace(spillRoot.getPath)
    val f     = space.newFile(Schema(IndexedSeq.empty))
    val dir   = spillRoot.listFiles().find(_.getName.startsWith("wvlet-spill-")).get
    val perms = Files.getPosixFilePermissions(dir.toPath)
    perms.toString shouldBe "[OWNER_READ, OWNER_WRITE, OWNER_EXECUTE]"
    f.finish()
    space.close()
    space.close()
    spillFolders shouldBe Nil
    intercept[WvletLangException](space.newFile(Schema(IndexedSeq.empty)))
  }

end AggSpillTest
