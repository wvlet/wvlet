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

import wvlet.lang.engine.exec.ComputeOnce
import wvlet.lang.engine.exec.TaskRunner
import wvlet.lang.engine.expr.Kernels
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.vector.Batch
import wvlet.lang.engine.vector.IntVec
import wvlet.lang.engine.vector.LongVec
import wvlet.lang.engine.vector.StrVec

import scala.util.control.NonFatal

/**
  * Statistics of a shared CTE result computed for planning ([[Stats.resultDistinct]],
  * [[Stats.resultRows]]), computed by the tasks of a [[TaskRunner]] (`parallel_result_stats`).
  * Planning runs on one thread while the engine's workers are idle: TPC-DS q11 spent about 40 ms of
  * its 195 ms at SF10 counting the distinct customer ids of its 1M-row `year_total` result in a
  * string group table and the rows passing its readers' filters, one batch at a time.
  *
  * Distinct values are counted in two parallel steps. Each task mixes the non-NULL values of a
  * range of batches into longs and counts them per partition (the top bits of the mixed value).
  * Then each task counts the distinct values of one partition in an open-addressing set sized for
  * it. Integer values are mixed by a bijection, so their counts are exact. A string is mixed from
  * its hash code and length, so strings that share both count once: an estimate for planning, about
  * one value per 10,000 low at 1M distinct strings.
  *
  * A chunk no task has started yet runs on the calling thread ([[ComputeOnce]]), so the caller
  * never waits for queued work, even on a pool thread.
  */
object ParallelResultStats:
  /** The fewest rows of one task, so that small results are counted on the calling thread */
  val MinRowsPerTask: Long = 64 * 1024

  private val Mix = 0x9e3779b97f4a7c15L

  /**
    * The distinct non-NULL values of column k of `batches`, or None when a column isn't a long, int
    * or string vector
    */
  def distinct(batches: Seq[Batch], k: Int, runner: TaskRunner): Option[Long] =
    val live      = batches.filter(_.size > 0).toIndexedSeq
    val supported = live.forall { b =>
      b.column(k) match
        case _: LongVec | _: IntVec | _: StrVec =>
          true
        case _ =>
          false
    }
    if !supported then
      None
    else
      val rows   = live.map(_.size.toLong).sum
      val n      = tasksFor(rows, runner)
      val chunks = chunksOf(live, rows, n)
      // One partition per task, rounded up to a power of two
      val pbits  = 64 - java.lang.Long.numberOfLeadingZeros(n - 1L)
      val mixed  = inParallel(runner, chunks.size)(i => mixKeys(chunks(i), k, pbits))
      val counts = inParallel(runner, 1 << pbits)(p => countPartition(mixed, p, pbits))
      Some(counts.sum)

  /**
    * The rows of `batches` for which `cond` is TRUE, as [[Stats.resultRows]] counts them, or None
    * if the condition fails to evaluate
    */
  def rows(batches: Seq[Batch], cond: VExpr, runner: TaskRunner): Option[Long] =
    val live   = batches.filter(_.size > 0).toIndexedSeq
    val rows   = live.map(_.size.toLong).sum
    val chunks = chunksOf(live, rows, tasksFor(rows, runner))
    try
      Some(
        inParallel(runner, chunks.size) { i =>
          var n = 0L
          chunks(i).foreach { b =>
            val mask = Kernels.trueMask(cond.eval(b))
            var j    = 0
            while j < b.size do
              if mask(j) then
                n += 1
              j += 1
          }
          n
        }.sum
      )
    catch
      case NonFatal(_) =>
        None

  /** The tasks for `rows` rows: one per [[MinRowsPerTask]] rows, at most twice the parallelism */
  private def tasksFor(rows: Long, runner: TaskRunner): Int =
    math.max(1L, math.min(runner.parallelism * 2L, rows / MinRowsPerTask)).toInt

  /** Consecutive batches split into n chunks of similar rows */
  private def chunksOf(live: IndexedSeq[Batch], rows: Long, n: Int): IndexedSeq[IndexedSeq[Batch]] =
    if n == 1 then
      IndexedSeq(live)
    else
      val target = (rows + n - 1) / n
      val out    = IndexedSeq.newBuilder[IndexedSeq[Batch]]
      var cur    = IndexedSeq.newBuilder[Batch]
      var acc    = 0L
      live.foreach { b =>
        cur += b
        acc += b.size
        if acc >= target then
          out += cur.result()
          cur = IndexedSeq.newBuilder[Batch]
          acc = 0L
      }
      if acc > 0 then
        out += cur.result()
      out.result()

  /**
    * Mixed values grouped by partition: those of partition p are `keys(starts(p))` until
    * `keys(starts(p + 1))`
    */
  private class Partitioned(val keys: Array[Long], val starts: Array[Int])

  /** The mixed non-NULL values of column k of the batches, grouped by partition */
  private def mixKeys(batches: IndexedSeq[Batch], k: Int, pbits: Int): Partitioned =
    val out                       = new Array[Long](batches.map(_.size).sum)
    val hist                      = new Array[Int](1 << pbits)
    var m                         = 0
    val shift                     = 64 - pbits
    inline def add(v: Long): Unit =
      val x = v * Mix
      out(m) = x
      m += 1
      if pbits > 0 then
        hist((x >>> shift).toInt) += 1
      else
        hist(0) += 1
    batches.foreach { b =>
      val n = b.size
      b.column(k) match
        case v: LongVec =>
          val values = v.values
          val nulls  = v.nulls
          var i      = 0
          while i < n do
            if nulls == null || !nulls(i) then
              add(values(i))
            i += 1
        case v: IntVec =>
          val values = v.values
          val nulls  = v.nulls
          var i      = 0
          while i < n do
            if nulls == null || !nulls(i) then
              add(values(i).toLong)
            i += 1
        case v: StrVec =>
          val nulls = v.nulls
          var i     = 0
          while i < n do
            if nulls == null || !nulls(i) then
              val s = v.str(i)
              if s != null then
                add((s.hashCode.toLong << 32) | (s.length & 0xffffffffL))
            i += 1
        case other =>
          throw IllegalStateException(s"Not a long, int or string column: ${other}")
    }
    // Group the values by partition (a counting sort), so each partition's task reads only its own
    val starts = new Array[Int](hist.length + 1)
    var p      = 0
    while p < hist.length do
      starts(p + 1) = starts(p) + hist(p)
      p += 1
    if pbits == 0 then
      Partitioned(java.util.Arrays.copyOf(out, m), starts)
    else
      val keys = new Array[Long](m)
      val next = java.util.Arrays.copyOf(starts, hist.length)
      var j    = 0
      while j < m do
        val x = out(j)
        val q = (x >>> shift).toInt
        keys(next(q)) = x
        next(q) += 1
        j += 1
      Partitioned(keys, starts)

  end mixKeys

  /** The distinct mixed values of partition p, counted in a set at most half full */
  private def countPartition(mixed: IndexedSeq[Partitioned], p: Int, pbits: Int): Long =
    val members = mixed.map(m => (m.starts(p + 1) - m.starts(p)).toLong).sum
    val bits  = math.max(4, 64 - java.lang.Long.numberOfLeadingZeros(math.max(1L, members * 2 - 1)))
    val slots = new Array[Long](1 << bits)
    val mask  = slots.length - 1
    var count = 0L
    // 0 marks an empty slot, so the value 0 (in partition 0) is counted on the side
    var hasZero = false
    mixed.foreach { m =>
      val keys = m.keys
      var j    = m.starts(p)
      val end  = m.starts(p + 1)
      while j < end do
        val x = keys(j)
        if x == 0L then
          hasZero = true
        else
          var i = ((x << pbits) >>> (64 - bits)).toInt
          while slots(i) != 0L && slots(i) != x do
            i = (i + 1) & mask
          if slots(i) == 0L then
            slots(i) = x
            count += 1
        j += 1
    }
    count + (
      if hasZero then
        1
      else
        0
    )

  end countPartition

  /**
    * f(0), ..., f(n - 1), computed by the runner's tasks and the calling thread. After a failure,
    * chunks not started yet never start, and the call returns once the started ones finish, so no
    * task outlives it
    */
  private def inParallel[A](runner: TaskRunner, n: Int)(f: Int => A): IndexedSeq[A] =
    val tasks = IndexedSeq.tabulate(n)(i => ComputeOnce(() => f(i)))
    if n <= 1 || runner.parallelism <= 1 then
      tasks.map(_.get())
    else
      val handles = tasks.tail.map(t => runner.submit(() => t.run()))
      try
        tasks.map(_.get())
      finally
        handles.foreach(_.cancel())
        tasks.foreach { t =>
          t.runUnless(() => true)
          t.awaitIfStarted()
        }

end ParallelResultStats
