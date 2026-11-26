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
package wvlet.lang.engine.plan

import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/**
  * A right / full join's unmatched build rows are emitted by tasks over ranges of the build rows
  * (`parallel_outer_tail`), and give the same rows as one run on the calling thread
  */
class ParallelOuterTailTest extends UniTest:

  /** Runs each task when it is submitted, but plans as if tasks ran four at a time */
  private object FourAtATime extends TaskRunner:
    def parallelism: Int                               = 4
    def submit[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)

  /** A table whose batches are its parts */
  private class Parts(val name: String, val schema: Schema, batches: Seq[Batch])
      extends TableSource:
    def scan(spec: ScanSpec): Iterator[Batch]                   = batches.iterator
    override def canSplit: Boolean                              = true
    override def scanSplits(spec: ScanSpec): Option[ScanSplits] = Some(
      ScanSplits(batches.toIndexedSeq.map(b => () => Iterator.single(b)), () => ())
    )

  private def longs(xs: Array[Long]): Vec = LongVec(PType.Int64, xs, null)

  private def schema(p: String) = Schema(
    IndexedSeq(Field(s"${p}k", PType.Int64), Field(s"${p}v", PType.Int64))
  )

  private def key: IndexedSeq[VExpr] = IndexedSeq(ColRef(0, PType.Int64, "k"))

  /** A probe scan of two parts, matching every 7th build key and some keys past the build */
  private def probe: ScanOp =
    def batch(keys: Array[Long]) = Batch(Array(longs(keys), longs(keys.map(_ * 10))), keys.length)
    val all                      = (0L until BuildRows.toLong + 100 by 7).toArray
    val t                        = Parts(
      "p",
      schema("p_"),
      Seq(batch(all.take(all.length / 2)), batch(all.drop(all.length / 2)))
    )
    ScanOp(t, t.schema, None, None)

  private val BuildRows = 100000

  /** The build: keys 0 until BuildRows */
  private def build(rows: Int = BuildRows, p: String = "b_"): ScanOp =
    val keys = (0L until rows.toLong).toArray
    val t = MemTable(s"${p}t", schema(p), Seq(Batch(Array(longs(keys), longs(keys.map(-_))), rows)))
    ScanOp(t, t.schema, None, None)

  private def join(left: Operator, right: Operator, kind: JoinKind): HashJoinOp = HashJoinOp(
    left,
    right,
    key,
    key,
    kind,
    None,
    Schema(left.schema.fields ++ right.schema.fields)
  )

  private def rows(op: Operator): Seq[String] =
    try
      op.drain()
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).mkString(",")))
        .sorted
    finally op.close()

  private def parallel(op: Operator, split: Boolean = true): Operator = Parallelizer(
    op,
    FourAtATime,
    parallelOuterTail = split
  )

  private def morsels(op: Operator): Seq[MorselOp] =
    op match
      case m: MorselOp =>
        Seq(m)
      case _ =>
        op.children.flatMap(morsels)

  /** The tasks that the tail of op's morsels ran as, once op has run */
  private def tailTasks(op: Operator): Seq[Int] = morsels(op).map(_.tailTasks).filter(_ > 0)

  test("should emit a right join's unmatched build rows as tasks over ranges of the build") {
    def plan = join(probe, build(), JoinKind.Right)
    val op   = parallel(plan)
    val got  = rows(op)
    tailTasks(op) shouldBe Seq(4)
    got.size shouldBe BuildRows
    got shouldBe rows(plan)
  }

  test("should emit a full join's rows the same way") {
    def plan = join(probe, build(), JoinKind.Full)
    val op   = parallel(plan)
    val got  = rows(op)
    tailTasks(op) shouldBe Seq(4)
    got shouldBe rows(plan)
  }

  test("should aggregate the unmatched rows of every range") {
    def plan = HashAggOp(
      ProjectOp(
        join(probe, build(), JoinKind.Right),
        IndexedSeq(IsNullExpr(ColRef(0, PType.Int64, "p_k"), false)),
        Schema(IndexedSeq(Field("unmatched", PType.Bool)))
      ),
      IndexedSeq(ColRef(0, PType.Bool, "unmatched")),
      IndexedSeq.empty,
      Schema(IndexedSeq(Field("unmatched", PType.Bool)))
    )
    val op = parallel(plan)
    rows(op) shouldBe Seq("false", "true")
    tailTasks(op) shouldBe Seq(4)
  }

  test("should run the tail once without the toggle or for a small build") {
    val off = parallel(join(probe, build(), JoinKind.Right), split = false)
    rows(off).size shouldBe BuildRows
    tailTasks(off) shouldBe Nil
    val small = parallel(join(probe, build(rows = 1000), JoinKind.Right))
    rows(small).size shouldBe 1000
    tailTasks(small) shouldBe Nil
  }

  test("should run nested right joins' tail once") {
    // The inner join's unmatched rows probe the outer join, which emits its own after them
    def plan = join(join(probe, build(), JoinKind.Right), build(p = "c_"), JoinKind.Right)
    val op   = parallel(plan)
    rows(op) shouldBe rows(plan)
    tailTasks(op) shouldBe Nil
  }

  test("should build an outer join on its other input at 2x unless the build aggregates") {
    val on  = OptimizerOptions()
    val off = OptimizerOptions().withDisabled(Optimization.SmallerOuterBuilds)
    JoinOrderOptimizer.swapOuterBuild(on, 3000, 1000) shouldBe true
    JoinOrderOptimizer.swapOuterBuild(off, 3000, 1000) shouldBe false
    // An aggregating build (TPC-DS q10's decorrelated exists) keeps the 4x ratio
    JoinOrderOptimizer.swapOuterBuild(on, 3000, 1000, aggregates = true) shouldBe false
    JoinOrderOptimizer.swapOuterBuild(on, 5000, 1000, aggregates = true) shouldBe true
  }

end ParallelOuterTailTest
