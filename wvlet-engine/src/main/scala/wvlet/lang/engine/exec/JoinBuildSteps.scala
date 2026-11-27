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

import wvlet.lang.engine.vector.*

/**
  * The steps of a [[JoinBuild]] after its input is drained, split into tasks on a runner
  * (`parallel_join_build`): concatenating the input, finding the rows with no NULL key, and the
  * range of a key. With [[TaskRunner.Sequential]] each step runs on the calling thread
  */
object JoinBuildSteps:
  /** The number of tasks a parallel build step splits into */
  private[exec] def taskCount(runner: TaskRunner): Int = runner.parallelism * 2

  /** Bounds of `tasks` (or fewer) contiguous chunks over positions 0 until n */
  private[exec] def chunks(n: Int, tasks: Int): Array[Int] =
    val k = math.max(1, math.min(tasks, n))
    Array.tabulate(k + 1)(c => (n.toLong * c / k).toInt)

  /** Run f(0) ... f(n - 1), as separate tasks on the runner, and wait for all of them */
  private[exec] def runAll(n: Int, runner: TaskRunner)(f: Int => Unit): Unit =
    if n == 1 || (runner eq TaskRunner.Sequential) then
      var i = 0
      while i < n do
        f(i)
        i += 1
    else
      val handles = Array.tabulate(n)(i => runner.submit(() => f(i)))
      handles.foreach(_.get())

  /** An array of n copies of v, filled in chunks on the runner */
  private[exec] def filledInts(n: Int, v: Int, runner: TaskRunner): Array[Int] =
    if runner eq TaskRunner.Sequential then
      Vec.filledInts(n, v)
    else
      val a  = new Array[Int](n)
      val ch = chunks(n, taskCount(runner))
      runAll(ch.length - 1, runner)(c => java.util.Arrays.fill(a, ch(c), ch(c + 1), v))
      a

  /** The rows with no NULL key, in input order, and their count */
  private[exec] def validRows(keys: Array[Vec], n: Int, runner: TaskRunner): (Array[Int], Int) =
    val nulls = keys.map(_.nulls).filter(_ != null)
    val valid = new Array[Int](n)
    if nulls.isEmpty then
      // Every row is valid
      val ch = chunks(n, taskCount(runner))
      runAll(ch.length - 1, runner) { c =>
        var i = ch(c)
        while i < ch(c + 1) do
          valid(i) = i
          i += 1
      }
      (valid, n)
    else
      // Each chunk compacts its valid rows in place, then chunks move down in order
      val ch     = chunks(n, taskCount(runner))
      val counts = new Array[Int](ch.length - 1)
      runAll(ch.length - 1, runner) { c =>
        var m = ch(c)
        var i = ch(c)
        while i < ch(c + 1) do
          var k = 0
          while k < nulls.length && !nulls(k)(i) do
            k += 1
          if k == nulls.length then
            valid(m) = i
            m += 1
          i += 1
        counts(c) = m - ch(c)
      }
      var nv = 0
      var c  = 0
      while c < counts.length do
        if ch(c) != nv then
          System.arraycopy(valid, ch(c), valid, nv, counts(c))
        nv += counts(c)
        c += 1
      (valid, nv)
    end if

  end validRows

  /**
    * The [min, max] of an Int32 / Int64 vector at rows `rows(0 until n)` (all non-NULL), computed
    * in chunks on the runner; None for other vectors or no rows
    */
  private[exec] def keyRange(
      keys: Vec,
      rows: Array[Int],
      n: Int,
      runner: TaskRunner
  ): Option[(Long, Long)] =
    if n == 0 then
      return None
    val ints: Array[Int] =
      keys match
        case v: IntVec =>
          v.values
        case _ =>
          null
    val longs: Array[Long] =
      keys match
        case v: LongVec =>
          v.values
        case _ =>
          null
    if ints == null && longs == null then
      return None
    val ch  = chunks(n, taskCount(runner))
    val los = Array.fill(ch.length - 1)(Long.MaxValue)
    val his = Array.fill(ch.length - 1)(Long.MinValue)
    runAll(ch.length - 1, runner) { c =>
      var lo = Long.MaxValue
      var hi = Long.MinValue
      var j  = ch(c)
      while j < ch(c + 1) do
        val x =
          if ints != null then
            ints(rows(j)).toLong
          else
            longs(rows(j))
        if x < lo then
          lo = x
        if x > hi then
          hi = x
        j += 1
      los(c) = lo
      his(c) = hi
    }
    Some((los.min, his.max))

  end keyRange

  /**
    * The valid rows `valid(0 until nv)` of a flat table's keys split into `parts` hash partitions
    * (by the top bits of [[LongJoinTable.hash]], as [[GroupTable.partitionRows]] then mapped to
    * build rows), each in input order: chunks of rows hash and count their partitions in tasks,
    * then copy their rows to their offsets in each partition (`parallel_join_keys`)
    */
  private[exec] def flatPartitions(
      key: Array[Long],
      valid: Array[Int],
      nv: Int,
      parts: Int,
      runner: TaskRunner
  ): Array[Array[Int]] =
    val shift  = 32 - Integer.numberOfTrailingZeros(parts)
    val ch     = chunks(nv, taskCount(runner))
    val k      = ch.length - 1
    val part   = new Array[Int](nv)
    val counts = Array.ofDim[Int](k, parts)
    runAll(k, runner) { c =>
      val cnt = counts(c)
      var j   = ch(c)
      while j < ch(c + 1) do
        val p = LongJoinTable.hash(key(valid(j))) >>> shift
        part(j) = p
        cnt(p) += 1
        j += 1
    }
    // counts(c)(p) becomes chunk c's first offset in partition p
    val sizes = new Array[Int](parts)
    var c     = 0
    while c < k do
      var p = 0
      while p < parts do
        val n = counts(c)(p)
        counts(c)(p) = sizes(p)
        sizes(p) += n
        p += 1
      c += 1
    val out = new Array[Array[Int]](parts)
    runAll(parts, runner)(p => out(p) = new Array[Int](sizes(p)))
    runAll(k, runner) { c =>
      val pos = counts(c)
      var j   = ch(c)
      while j < ch(c + 1) do
        val p = part(j)
        out(p)(pos(p)) = valid(j)
        pos(p) += 1
        j += 1
    }
    out

  end flatPartitions

  /**
    * [[Batch.concat]] with the copies of typed columns split by input batch into tasks on the
    * runner; other columns are concatenated on the calling thread
    */
  private[exec] def concat(schema: Schema, batches: IndexedSeq[Batch], runner: TaskRunner): Batch =
    if batches.size == 1 then
      return batches.head.compact()
    val offsets = new Array[Int](batches.size + 1)
    var b       = 0
    while b < batches.size do
      offsets(b + 1) = offsets(b) + batches(b).size
      b += 1
    val total  = offsets(batches.size)
    val groups = chunks(batches.size, taskCount(runner))
    val copies = scala.collection.mutable.ArrayBuffer.empty[(Int, Int) => Unit]
    // Copies the arrays of batches from until until into `out` at their offsets
    def typed[A: scala.reflect.ClassTag](arrays: IndexedSeq[Array[A]]): Array[A] =
      val out                               = new Array[A](total)
      def copy(from: Int, until: Int): Unit =
        var i = from
        while i < until do
          System.arraycopy(arrays(i), 0, out, offsets(i), batches(i).size)
          i += 1
      copies += copy
      out
    def nullsOf(vs: IndexedSeq[Vec]): Array[Boolean] =
      if vs.forall(_.nulls == null) then
        null
      else
        val out                               = new Array[Boolean](total)
        def copy(from: Int, until: Int): Unit =
          var i = from
          while i < until do
            val m = vs(i).nulls
            if m != null then
              System.arraycopy(m, 0, out, offsets(i), batches(i).size)
            i += 1
        copies += copy
        out
    val cols =
      Array.tabulate[Vec](schema.size) { c =>
        val pt = schema(c).ptype
        val vs = batches.map(_.columns(c))
        if vs.forall(v => v.isInstanceOf[LongVec] && v.ptype == pt) then
          LongVec(pt, typed(vs.map(_.asInstanceOf[LongVec].values)), nullsOf(vs))
        else if vs.forall(v => v.isInstanceOf[IntVec] && v.ptype == pt) then
          IntVec(pt, typed(vs.map(_.asInstanceOf[IntVec].values)), nullsOf(vs))
        else if pt == PType.Float64 && vs.forall(_.isInstanceOf[DoubleVec]) then
          DoubleVec(typed(vs.map(_.asInstanceOf[DoubleVec].values)), nullsOf(vs))
        else if pt == PType.Str && vs.forall(_.isInstanceOf[StrVec]) then
          StrVec(typed(vs.map(_.asInstanceOf[StrVec].values)), nullsOf(vs))
        else if pt == PType.Bool && vs.forall(_.isInstanceOf[BoolVec]) then
          BoolVec(typed(vs.map(_.asInstanceOf[BoolVec].values)), nullsOf(vs))
        else
          Batch.concatVecs(pt, vs, total)
      }
    runAll(groups.length - 1, runner) { g =>
      copies.foreach(_(groups(g), groups(g + 1)))
    }
    Batch(cols, total)

  end concat

end JoinBuildSteps
