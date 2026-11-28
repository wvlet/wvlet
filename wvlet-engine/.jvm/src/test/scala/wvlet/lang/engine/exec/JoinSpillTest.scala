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
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.engine.EngineResult
import wvlet.lang.engine.WvletEngine
import wvlet.lang.engine.catalog.TpchGen
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.VExpr
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
  * Hash join builds past the spill threshold spill their partitions that don't fit (`spill_join`)
  */
class JoinSpillTest extends UniTest:
  private val tables    = TpchGen.tables(0.01)
  private val spillRoot = Files.createTempDirectory("wvlet-join-spill-test").toFile

  override def afterAll: Unit =
    def rm(f: File): Unit =
      Option(f.listFiles()).foreach(_.foreach(rm))
      f.delete()
    rm(spillRoot)

  // Aggregations keep their input in memory, so only joins spill
  private def engineOf(
      opts: OptimizerOptions = OptimizerOptions().withDisabled(Optimization.SpillAggregation)
  ) =
    val e = WvletEngine(optimizer = opts)
      .withMemoryPool(MemoryPool())
      .withSpillDirectory(spillRoot.getPath)
    tables.foreach(e.catalog.register)
    e

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def run(engine: WvletEngine, query: String): EngineResult =
    withRelation(query)(engine.execute(_))

  private def withRelation[A](query: String)(f: Context ?=> Relation => A): A =
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
    f(find(unit.executionPlan).get)

  private def sortedRows(r: EngineResult): Seq[String] = r.rows.map(_.mkString("|")).toSeq.sorted

  /** The partitions the joins of `body` wrote out and joined */
  private def spills[A](body: => A): (A, Long, Long) =
    val evicted = JoinSpill.partitionsEvicted.get()
    val joined  = JoinSpill.partitionsJoined.get()
    val a       = body
    (a, JoinSpill.partitionsEvicted.get() - evicted, JoinSpill.partitionsJoined.get() - joined)

  private def spillFolders: Seq[String] = Option(spillRoot.list()).map(_.toSeq).getOrElse(Nil)

  private val queries = Seq(
    // Inner, one key
    "select o_orderkey, o_custkey, l_linenumber, l_quantity from lineitem join orders " +
      "on l_orderkey = o_orderkey",
    // Two keys and a residual condition
    "select l_orderkey, l_linenumber, ps_availqty from lineitem join partsupp " +
      "on l_partkey = ps_partkey and l_suppkey = ps_suppkey and ps_availqty > l_quantity * 50",
    // A string key
    "select p1.p_partkey, p2.p_partkey from part p1 join part p2 on p1.p_type = p2.p_type " +
      "where p1.p_size < 5",
    // Several joins in one pipeline
    "select c_name, o_orderdate, l_extendedprice from lineitem join orders on l_orderkey = " +
      "o_orderkey join customer on o_custkey = c_custkey where l_quantity > 30",
    // Outer joins
    "select c_custkey, o_orderkey from customer left join orders on c_custkey = o_custkey",
    "select c_custkey, o_orderkey from orders right join customer on o_custkey = c_custkey",
    "select c_custkey, o_orderkey from (select * from customer where c_custkey % 2 = 0) c " +
      "full join (select * from orders where o_orderkey % 3 = 0) o on c_custkey = o_custkey",
    // Semi and anti joins
    "select o_orderkey from orders where o_custkey in (select c_custkey from customer " +
      "where c_acctbal > 0)",
    "select o_orderkey from orders where not exists (select 1 from lineitem " +
      "where l_orderkey = o_orderkey and l_shipdate > date '1998-06-01')",
    // NULL probe keys
    "select c_custkey, k from customer left join (select case when o_orderkey % 2 = 0 then " +
      "null else o_custkey end k from orders) on c_custkey = k"
  )

  test("should give the same rows when join builds spill as when they fit") {
    val inMemory = engineOf()
    // Under these thresholds the builds keep some partitions and write the rest
    val spilling = Seq(256L * 1024, 16L * 1024).map(engineOf().withSpillThreshold(_))
    queries.foreach { q =>
      val expected = sortedRows(run(inMemory, q))
      val evicted  = spilling.map { e =>
        val (actual, evicted, joined) = spills(sortedRows(run(e, q)))
        debug(s"${q}: ${evicted} partitions evicted, ${joined} joined")
        actual shouldBe expected
        evicted
      }
      // Every join kind spills under one of them
      evicted.sum > 0 shouldBe true
    }
    spillFolders shouldBe Nil
  }

  test("should split a spilled partition again when it does not fit") {
    val inMemory = engineOf()
    // No headroom: every partition is written, and every partition's build splits once more
    val spilling = engineOf().withSpillThreshold(1)
    queries.foreach { q =>
      val expected                  = sortedRows(run(inMemory, q))
      val (actual, evicted, joined) = spills(sortedRows(run(spilling, q)))
      actual shouldBe expected
    }
    val (_, evicted, joined) = spills(run(spilling, queries.head))
    // The first split's partitions, then each one's own partitions
    evicted > JoinSpill.Partitions shouldBe true
    joined > JoinSpill.Partitions shouldBe true
    spillFolders shouldBe Nil
  }

  test("should match NULL keys of a NULL-equal join across spilled partitions") {
    val schema = Schema(IndexedSeq(Field("k", PType.Int64), Field("v", PType.Int64)))
    // Keys 0 until `keys`, NULL every `nullEvery` rows
    def input(rows: Int, keys: Int, nullEvery: Int): Seq[Batch] = (0 until rows)
      .grouped(1000)
      .map { r =>
        val n = r.size
        val k = LongVec(
          PType.Int64,
          r.map(i => (i % keys).toLong).toArray,
          r.map(_ % nullEvery == 0).toArray
        )
        val v = LongVec(PType.Int64, r.map(_.toLong).toArray, null)
        Batch(Array[Vec](k, v), n)
      }
      .toSeq
    val probe = input(20000, 3000, 7)
    val build = input(8000, 5000, 11)
    val key   = IndexedSeq[VExpr](ColRef(0, PType.Int64, "k"))
    def joined(kind: JoinKind, memory: MemoryContext): Seq[String] =
      MemoryContext
        .withCurrent(memory) {
          val j = HashJoinOp(
            IteratorOp(schema, () => probe.iterator, "probe"),
            IteratorOp(schema, () => build.iterator, "build"),
            key,
            key,
            kind,
            None,
            schema,
            nullsEqual = true
          ).withSpill(true)
          try j
              .drain()
              .flatMap(b =>
                (0 until b.size).map(i => s"${b.column(0).get(i)}|${b.column(1).get(i)}")
              )
          finally j.close()
        }
        .sorted
    Seq(JoinKind.Semi, JoinKind.Anti).foreach { kind =>
      val expected             = joined(kind, MemoryContext.NoOp)
      val memory               = MemoryPool().newQuery().withSpill(1, spillRoot.getPath)
      val (actual, evicted, _) = spills(joined(kind, memory))
      memory.close()
      evicted > 0 shouldBe true
      actual shouldBe expected
    }
    spillFolders shouldBe Nil
  }

  test("should hold the build in memory with spill_join off") {
    val engine = engineOf(
      OptimizerOptions()
        .withDisabled(Optimization.SpillAggregation)
        .withDisabled(Optimization.SpillJoin)
    ).withSpillThreshold(1)
    val (r, evicted, _) = spills(run(engine, queries.head))
    evicted shouldBe 0L
    r.numRows > 0 shouldBe true
  }

  test("should delete a query's spill files when it fails") {
    // Spills past 1 byte, then fails at the 1 MB limit
    val engine = engineOf().withSpillThreshold(1).withQueryMemoryLimit(1L << 20)
    val e      = intercept[WvletLangException](run(engine, queries(3)))
    e.statusCode shouldBe StatusCode.RESOURCE_EXHAUSTED
    spillFolders shouldBe Nil
  }

  test("should return every reservation of a spilled join once drained") {
    val engine             = engineOf()
    val memory             = MemoryPool().newQuery().withSpill(256L * 1024, spillRoot.getPath)
    val (rows, evicted, _) = spills {
      withRelation(queries(3)) { r =>
        MemoryContext.withCurrent(memory) {
          val op = engine.physicalPlan(GenSQL.expand(r, summon[Context]))
          try op.drain().map(_.size).sum
          finally op.close()
        }
      }
    }
    evicted > 0 shouldBe true
    rows > 0 shouldBe true
    memory.reservedBytes shouldBe 0L
    memory.close()
    spillFolders shouldBe Nil
  }

end JoinSpillTest
