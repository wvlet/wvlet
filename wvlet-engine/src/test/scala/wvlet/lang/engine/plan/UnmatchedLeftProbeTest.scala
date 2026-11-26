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
  * A LEFT hash join under a filter testing a build key for NULL emits only its unmatched probe rows
  * (`unmatched_left_probe`), with the same result as the full LEFT join and filter
  */
class UnmatchedLeftProbeTest extends UniTest:

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

  private def longs(xs: Option[Long]*): Vec = LongVec(
    PType.Int64,
    xs.map(_.getOrElse(0L)).toArray,
    xs.map(_.isEmpty).toArray
  )

  /** A scan of (a, b, v) rows, one batch per part */
  private def table(name: String, parts: Seq[Seq[(Option[Long], Long, Long)]]): ScanOp =
    val schema = Schema(
      IndexedSeq(
        Field(s"${name}_a", PType.Int64),
        Field(s"${name}_b", PType.Int64),
        Field(s"${name}_v", PType.Int64)
      )
    )
    val batches = parts.map { rows =>
      Batch(
        Array(
          longs(rows.map(_._1)*),
          longs(rows.map(r => Some(r._2))*),
          longs(rows.map(r => Some(r._3))*)
        ),
        rows.size
      )
    }
    val t = Parts(name, schema, batches)
    ScanOp(t, t.schema, None, None)

  // Probe rows: matched (one build row, or two sharing the key), unmatched, and a NULL key
  private def probe = table(
    "p",
    Seq(
      Seq((Some(1L), 1L, 10L), (Some(2L), 1L, 20L), (Some(3L), 1L, 30L)),
      Seq((None, 1L, 40L), (Some(4L), 2L, 50L), (Some(1L), 2L, 60L))
    )
  )

  private def build = table(
    "b",
    Seq(Seq((Some(1L), 1L, 100L), (Some(3L), 1L, 300L)), Seq((Some(3L), 1L, 301L), (None, 2L, 0L)))
  )

  private def col(i: Int) = ColRef(i, PType.Int64, s"c${i}")

  /** LEFT JOIN on (a, b), then `cond` over the 6 joined columns */
  private def leftJoinFilter(cond: VExpr): FilterOp =
    val join = HashJoinOp(
      probe,
      build,
      IndexedSeq(col(0), col(1)),
      IndexedSeq(col(0), col(1)),
      JoinKind.Left,
      None,
      probe.schema ++ build.schema
    )
    FilterOp(join, cond, Array(0, 1, 2, 3, 5))

  private def joinOf(op: Operator): HashJoinOp =
    op.children
      .collectFirst { case j: HashJoinOp =>
        j
      }
      .get

  private def rows(op: Operator): Seq[Seq[Any]] =
    try op
        .drain()
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toSeq))
        .sortBy(_.toString)
    finally op.close()

  private def bothWays(cond: VExpr): (Seq[Seq[Any]], Seq[Seq[Any]], Boolean) =
    val plain  = rows(leftJoinFilter(cond))
    val marked = leftJoinFilter(cond)
    Planner.markUnmatchedLeftProbes(marked)
    val on = joinOf(marked).unmatchedOnly
    (plain, rows(marked), on)

  private val keyIsNull = IsNullExpr(col(3), negate = false)

  test("should emit only the unmatched probe rows for IS NULL of a build key") {
    val (plain, marked, on) = bothWays(keyIsNull)
    on shouldBe true
    marked shouldBe plain
    // Unmatched (2, 1), (4, 2), (1, 2), and the NULL key, padded with NULL build columns
    marked.map(_(2)).toSet shouldBe Set(20L, 40L, 50L, 60L)
    marked.foreach(r => r(3) shouldBe null)
  }

  test("should keep the other conjuncts of the filter") {
    val cond                = AndExpr(Compare(CmpOp.Gt, col(2), Const(30L, PType.Int64)), keyIsNull)
    val (plain, marked, on) = bothWays(cond)
    on shouldBe true
    marked shouldBe plain
    marked.map(_(2)).toSet shouldBe Set(40L, 50L, 60L)
  }

  test("should leave a LEFT join alone when the filter tests a non-key build column") {
    val (plain, marked, on) = bothWays(IsNullExpr(col(5), negate = false))
    on shouldBe false
    marked shouldBe plain
  }

  test("should leave a LEFT join alone for IS NOT NULL of a build key") {
    val (plain, marked, on) = bothWays(IsNullExpr(col(3), negate = true))
    on shouldBe false
    marked shouldBe plain
    // The two matches of key (3, 1) and the one of (1, 1)
    marked.size shouldBe 3
  }

  test("should keep emitting only unmatched rows when the probe runs in parallel") {
    val plain  = rows(Parallelizer(leftJoinFilter(keyIsNull), FourAtATime))
    val marked = leftJoinFilter(keyIsNull)
    Planner.markUnmatchedLeftProbes(marked)
    val par = Parallelizer(marked, FourAtATime)
    par.explain() shouldContain "unmatched only"
    rows(par) shouldBe plain
  }

end UnmatchedLeftProbeTest
