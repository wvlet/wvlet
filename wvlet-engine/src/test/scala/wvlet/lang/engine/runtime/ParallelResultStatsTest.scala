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
package wvlet.lang.engine.runtime

import wvlet.lang.engine.exec.TaskRunner
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/** Shared CTE result statistics computed by tasks (`parallel_result_stats`) */
class ParallelResultStatsTest extends UniTest:
  // Runs each task when submitted, but reports 8 threads, so results are split into chunks and
  // partitions on every platform
  private object Eight extends TaskRunner:
    def parallelism: Int                               = 8
    def submit[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)

  private val runners = Seq(Eight, TaskRunner.Sequential, TaskRunner.default)

  private def batchesOf(rows: Int, perBatch: Int)(col: (Int, Int) => Vec): Seq[Batch] =
    (0 until rows by perBatch).map { start =>
      val n = math.min(perBatch, rows - start)
      Batch(Array(col(start, n)), n)
    } :+ Batch(Array(LongVec(PType.Int64, Array.empty[Long], null)), 0)

  // 300K rows of 54,321 distinct longs (0 included) with every 7th row NULL
  private val longBatches =
    batchesOf(300000, 4096) { (start, n) =>
      LongVec(
        PType.Int64,
        Array.tabulate(n)(i => ((start + i) % 54321).toLong * 1000003L),
        Array.tabulate(n)(i => (start + i) % 7 == 3)
      )
    }

  test("count the distinct longs and ints of a result as a group table does") {
    val expected = Stats.resultDistinct(longBatches, 0, PType.Int64, primitive = false)
    val ints     =
      batchesOf(200000, 1000)((start, n) =>
        IntVec(PType.Int32, Array.tabulate(n)(i => (start + i) % 777 - 300), null)
      )
    runners.foreach { r =>
      ParallelResultStats.distinct(longBatches, 0, r) shouldBe Some(expected)
      ParallelResultStats.distinct(ints, 0, r) shouldBe Some(777L)
    }
  }

  test("count the distinct strings of a result, skipping NULLs") {
    val strs =
      batchesOf(250000, 3000) { (start, n) =>
        StrVec(
          Array.tabulate(n)(i =>
            if (start + i) % 11 == 0 then
              null
            else
              s"customer-${(start + i) % 40000}"
          ),
          Array.tabulate(n)(i => (start + i) % 11 == 0)
        )
      }
    val expected = Stats.resultDistinct(strs, 0, PType.Str)
    expected shouldBe 40000L
    runners.foreach { r =>
      ParallelResultStats.distinct(strs, 0, r) shouldBe Some(expected)
    }
    ParallelResultStats.distinct(Seq.empty, 0, Eight) shouldBe Some(0L)
  }

  test("leave columns other than longs, ints and strings to the group table") {
    val ds = Seq(Batch(Array(DoubleVec(Array(1.0, 2.0), null)), 2))
    ParallelResultStats.distinct(ds, 0, Eight) shouldBe None
  }

  test("count the rows a condition keeps as Stats.resultRows does") {
    val x = ColRef(0, PType.Int64, "x")
    for c <- Seq(
        Compare(CmpOp.Gt, x, Const(20000L * 1000003L, PType.Int64)),
        Compare(CmpOp.Ne, x, Const(0L, PType.Int64)),
        Compare(CmpOp.Eq, x, Const(null, PType.Int64))
      )
    do
      val expected = Stats.resultRows(longBatches, c)
      expected.isDefined shouldBe true
      runners.foreach { r =>
        ParallelResultStats.rows(longBatches, c, r) shouldBe expected
      }
  }

end ParallelResultStatsTest
