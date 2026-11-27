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
  * A pipeline whose stream is one batch of an aggregation's output and whose join probes a large
  * build runs over row ranges of the batch (`slice_single_batch_probes`), as TPC-DS q65 probes its
  * 2.3M (store, item) revenues with the 102 per-store averages, with the same rows as one run
  */
class SliceSingleBatchProbesTest extends UniTest:

  /** Runs each task when it is submitted, but plans as if tasks ran four at a time */
  private object FourAtATime extends TaskRunner:
    def parallelism: Int                               = 4
    def submit[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)

  private def col(i: Int): VExpr = ColRef(i, PType.Int64, s"c${i}")

  private def longs(xs: Array[Long]): Vec = LongVec(PType.Int64, xs, null)

  /** A table of `rows` rows (k, v) with k = row % 25, one batch */
  private def table(name: String, rows: Int): ScanOp =
    val ids    = (0L until rows.toLong).toArray
    val schema = Schema(IndexedSeq(Field("k", PType.Int64), Field("v", PType.Int64)))
    val t      = MemTable(name, schema, Seq(Batch(Array(longs(ids.map(_ % 25)), longs(ids)), rows)))
    ScanOp(t, schema, None, None)

  /**
    * Each k's sum of v over `probeRows` rows, joined with the `buildRows` rows of its k, keeping
    * the rows whose v is below a tenth of the sum
    */
  private def plan(buildRows: Int): Operator =
    val agg = HashAggOp(
      table("p", 1000),
      IndexedSeq(col(0)),
      IndexedSeq(AggCall("sum", Seq(col(1)), false, None, PType.Int64)),
      Schema(IndexedSeq(Field("k", PType.Int64), Field("s", PType.Int64)))
    )
    val right = table("b", buildRows)
    val join  = HashJoinOp(
      agg,
      right,
      IndexedSeq(col(0)),
      IndexedSeq(col(0)),
      JoinKind.Inner,
      None,
      Schema(agg.schema.fields ++ right.schema.fields)
    )
    FilterOp(
      join,
      Compare(CmpOp.Lt, Arith(ArithOp.Mul, col(3), Const(10L, PType.Int64), PType.Int64), col(1))
    )

  private def rows(op: Operator): Seq[String] =
    try
      op.drain()
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).mkString(",")))
        .sorted
    finally op.close()

  private def morsels(op: Operator): Seq[MorselOp] =
    op match
      case m: MorselOp =>
        Seq(m)
      case _ =>
        op.children.flatMap(morsels)

  /** The rows of the plan parallelized with the toggle on or off, and the row ranges it ran as */
  private def run(buildRows: Int, slice: Boolean): (Seq[String], Int) =
    val op  = Parallelizer(plan(buildRows), FourAtATime, sliceSingleBatches = slice)
    val out = rows(op)
    (out, morsels(op).map(_.slicedParts).sum)

  test("probe a large build over row ranges of an aggregation's output, with the same rows") {
    val n                = MorselOp.SliceBuildRows.toInt
    val (sliced, parts)  = run(n, slice = true)
    val (whole, noParts) = run(n, slice = false)
    sliced shouldBe whole
    sliced shouldBe rows(plan(n))
    sliced.size > 0 shouldBe true
    parts shouldBe 4
    noParts shouldBe 0
  }

  test("probe a small build on the calling thread") {
    val (out, parts) = run(1000, slice = true)
    out shouldBe rows(plan(1000))
    parts shouldBe 0
  }

end SliceSingleBatchProbesTest
