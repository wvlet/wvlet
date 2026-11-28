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

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/**
  * The spilled state of a hash join's build past the query's spill threshold (`spill_join`, grace
  * hash join): the build rows routed by key hash into [[JoinSpill.Partitions]] partitions, of which
  * those that fit stay in memory (`kept`, the build the join probes as usual) and the rest
  * (`evicted`) are written to one build file each. Probes route the rows of evicted partitions to
  * one probe file per partition ([[JoinSpill.ProbeRouter]]); the join that finishes the build then
  * joins each evicted partition's build and probe files on their own.
  *
  * A partition stays in memory while the query is under its spill threshold and the kept partitions
  * take at most half of it, so the tail that joins the evicted pairs has room; past that, the
  * largest kept partition is written out. Rows with a NULL key all go to partition 0 on both sides,
  * so NULL-equal keys (INTERSECT / EXCEPT) still meet. Built by one thread; probe files take blocks
  * from several
  *
  * @param depth
  *   0 for a join's own build, 1 for the build of an evicted partition split again
  */
final class JoinSpill(
    schema: Schema,
    keys: IndexedSeq[VExpr],
    memory: MemoryContext,
    val depth: Int
):
  import JoinSpill.*

  private val space = memory.spillSpace
  private val types = keys.map(_.ptype)
  val seed: Int     = SeedBase + depth

  /** The build file of each evicted partition */
  val buildFiles: Array[SpillFile] = new Array[SpillFile](Partitions)

  /** The probe file of each evicted partition, once a probe routed rows to it */
  val probeFiles: Array[SpillFile] = new Array[SpillFile](Partitions)

  /** Whether each partition was written to its build file */
  val evicted: Array[Boolean] = new Array[Boolean](Partitions)

  private val hasher      = GroupTable(types)
  private val kept        = Array.fill(Partitions)(mutable.ArrayBuffer.empty[Batch])
  private val keptBytes   = new Array[Long](Partitions)
  private val keptMemory  = Array.fill(Partitions)(memory.child())
  private var keptTotal   = 0L
  private val keptCap     = math.max(1L, memory.spillThresholdBytes / 2)
  private val pending     = Array.fill(Partitions)(mutable.ArrayBuffer.empty[Batch])
  private val pendingRows = new Array[Int](Partitions)

  /** Route the rows of a compacted batch to their partitions */
  def add(c: Batch): Unit =
    val n = c.size
    if n == 0 then
      return
    val parts = partitionsOf(hasher, keys.map(_.eval(c)).toArray, n, seed)
    val rows  = rowsByPartition(parts, n)
    var p     = 0
    while p < Partitions do
      val r = rows(p)
      if r != null then
        val piece =
          if r.length == n then
            c
          else
            c.gather(r, r.length)
        if evicted(p) then
          addPending(p, piece)
        else
          keep(p, piece)
      p += 1

  private def keep(p: Int, piece: Batch): Unit =
    val bytes = JoinBuild.inputBytes(piece)
    var done  = false
    while !done do
      if keptTotal + bytes <= keptCap && keptMemory(p).tryReserve(bytes) then
        kept(p) += piece
        keptBytes(p) += bytes
        keptTotal += bytes
        done = true
      else
        val victim = largestKept(p)
        evict(victim)
        if victim == p then
          addPending(p, piece)
          done = true

  // The kept partition with the most bytes, `p` (the one asking) when it holds the most or none
  // other holds any
  private def largestKept(p: Int): Int =
    var best = p
    var q    = 0
    while q < Partitions do
      if !evicted(q) && keptBytes(q) > keptBytes(best) then
        best = q
      q += 1
    best

  private def evict(p: Int): Unit =
    evicted(p) = true
    kept(p).foreach(addPending(p, _))
    kept(p).clear()
    keptTotal -= keptBytes(p)
    keptBytes(p) = 0
    keptMemory(p).release()
    flush(p)
    partitionsEvicted.incrementAndGet()

  private def addPending(p: Int, piece: Batch): Unit =
    pending(p) += piece
    pendingRows(p) += piece.size
    if pendingRows(p) >= BlockRows then
      flush(p)

  private def flush(p: Int): Unit =
    if pending(p).nonEmpty then
      if buildFiles(p) == null then
        buildFiles(p) = space.newFile(schema)
      buildFiles(p).append(Batch.concat(schema, pending(p).toSeq))
      pending(p).clear()
      pendingRows(p) = 0

  /** Whether any partition was written out */
  def anyEvicted: Boolean = evicted.contains(true)

  /**
    * Write the rest of the evicted partitions and close their build files; the kept partitions'
    * batches, whose reservations stay until [[releaseKept]]
    */
  def finish(): Seq[Batch] =
    var p = 0
    while p < Partitions do
      if evicted(p) then
        flush(p)
        if buildFiles(p) != null then
          buildFiles(p).finish()
      p += 1
    kept.toSeq.flatMap(_.toSeq)

  /** Return the kept partitions' reservations, once the build counts its table instead */
  def releaseKept(): Unit =
    keptMemory.foreach(_.release())
    kept.foreach(_.clear())

  /** Append probe rows of evicted partition `p`, encoded by the calling thread */
  def appendProbe(p: Int, probeSchema: Schema, b: Batch): Unit =
    val c     = b.compact()
    val block = SpillBlocks.encode(c)
    val bytes = MemoryContext.estimatedBytes(c)
    synchronized {
      if probeFiles(p) == null then
        probeFiles(p) = space.newFile(probeSchema)
      probeFiles(p).appendBlock(block, c.size, bytes)
    }

  /** Close the probe files: every probe of the build has routed its rows */
  def finishProbes(): Unit = synchronized {
    probeFiles.foreach { f =>
      if f != null then
        f.finish()
    }
  }

  /** Delete the files of partition `p`, once joined */
  def delete(p: Int): Unit =
    Option(buildFiles(p)).foreach(_.delete())
    Option(probeFiles(p)).foreach(_.delete())
    buildFiles(p) = null
    probeFiles(p) = null

  /** Delete every file left (after a failure or an early close) */
  def deleteAll(): Unit = (0 until Partitions).foreach(delete)

  /** A router of probe rows for one probe copy of the join */
  def probeRouter(probeSchema: Schema, probeKeys: IndexedSeq[VExpr]): ProbeRouter = ProbeRouter(
    this,
    probeSchema,
    probeKeys,
    types
  )

end JoinSpill

object JoinSpill:
  /** The partitions a spilled build splits into */
  inline val Partitions = 64

  private inline val PartitionShift = 26

  /** Rows of one partition gathered into each block of its file */
  inline val BlockRows = 65536

  // Hash seeds of a build split for the first and the second time; nonzero, so that the bits
  // routing a row to a spill partition don't pick the in-memory table's partition too
  private val SeedBase = 0x5bd1

  /** Partitions written out (for tests) */
  val partitionsEvicted: AtomicLong = AtomicLong(0)

  /** Spilled partitions joined (for tests) */
  val partitionsJoined: AtomicLong = AtomicLong(0)

  /** How many times a spilled partition's build may spill again, by another hash */
  inline val MaxDepth = 1

  /** The partition of each row: by a seeded hash of its keys, 0 when a key is NULL */
  def partitionsOf(hasher: GroupTable, keys: Array[Vec], n: Int, seed: Int): Array[Int] =
    val h = hasher.keyHashes(keys, n)
    AggSpillRouter.reseed(h, n, seed)
    val out = new Array[Int](n)
    var i   = 0
    while i < n do
      out(i) = h(i) >>> PartitionShift
      i += 1
    keys.foreach { k =>
      if k.nulls != null then
        var j = 0
        while j < n do
          if k.nulls(j) then
            out(j) = 0
          j += 1
    }
    out

  /** The rows of each partition in row order, null for a partition without rows */
  def rowsByPartition(parts: Array[Int], n: Int): Array[Array[Int]] =
    val counts = new Array[Int](Partitions)
    var i      = 0
    while i < n do
      counts(parts(i)) += 1
      i += 1
    val out = new Array[Array[Int]](Partitions)
    var p   = 0
    while p < Partitions do
      if counts(p) > 0 then
        out(p) = new Array[Int](counts(p))
      p += 1
    val pos = new Array[Int](Partitions)
    i = 0
    while i < n do
      val q = parts(i)
      out(q)(pos(q)) = i
      pos(q) += 1
      i += 1
    out

  /**
    * Routes one probe copy's rows: those of kept partitions return to the join, those of evicted
    * partitions are buffered and appended to their probe files, at most [[ProbeRouter.MaxBuffered]]
    * estimated bytes at once (parts run without a memory context, so this bounds them)
    */
  final class ProbeRouter(
      spill: JoinSpill,
      schema: Schema,
      keys: IndexedSeq[VExpr],
      buildTypes: IndexedSeq[PType]
  ):
    private val hasher   = GroupTable(buildTypes)
    private val buffered = Array.fill(Partitions)(mutable.ArrayBuffer.empty[Batch])
    private var bytes    = 0L

    /** The rows of `b` in kept partitions (null for none); buffers the others */
    def route(b: Batch): Batch =
      val n     = b.size
      val parts = partitionsOf(hasher, keys.map(_.eval(b)).toArray, n, spill.seed)
      val keep  = new Array[Int](n)
      var nk    = 0
      var i     = 0
      while i < n do
        if !spill.evicted(parts(i)) then
          keep(nk) = i
          nk += 1
        i += 1
      if nk == n then
        return b
      val rows = rowsByPartition(parts, n)
      var p    = 0
      while p < Partitions do
        val r = rows(p)
        if r != null && spill.evicted(p) then
          val piece = b.gather(r, r.length)
          buffered(p) += piece
          bytes += MemoryContext.estimatedBytes(piece)
        p += 1
      if bytes >= ProbeRouter.MaxBuffered then
        flush()
      if nk == 0 then
        null
      else
        b.take(java.util.Arrays.copyOf(keep, nk), nk)

    /** Append every buffered row to its probe file */
    def flush(): Unit =
      var p = 0
      while p < Partitions do
        if buffered(p).nonEmpty then
          spill.appendProbe(p, schema, Batch.concat(schema, buffered(p).toSeq))
          buffered(p).clear()
        p += 1
      bytes = 0L

  end ProbeRouter

  object ProbeRouter:
    /** The most probe bytes one copy buffers before writing them */
    inline val MaxBuffered = 4L << 20

end JoinSpill

/**
  * The build keys a spilled build's runtime filters need, collected while the build routes its
  * rows: per filtered key the [min, max] range of the valid rows (no NULL key), and their keys
  * while there are at most [[RowKeyFilter.MaxKeys]] of them, so the filters are those an in-memory
  * build of every row would set
  */
final class FilterKeys private (
    groups: IndexedSeq[(Int, Seq[RuntimeFilter])],
    keys: IndexedSeq[VExpr]
):
  private val bounds = new Array[(Long, Long)](groups.size)
  private val held   = Array.fill(groups.size)(mutable.ArrayBuffer.empty[Vec])
  private val sample = new Array[Vec](groups.size)
  private var rows   = 0L

  /** Add the keys of a compacted build batch */
  def add(c: Batch): Unit =
    val raw         = keys.map(_.eval(c)).toArray
    val (valid, nv) = JoinBuildSteps.validRows(raw, c.size, TaskRunner.Sequential)
    if nv > 0 then
      rows += nv
      var g = 0
      while g < groups.size do
        val k = raw(groups(g)._1)
        JoinBuildSteps
          .keyRange(k, valid, nv, TaskRunner.Sequential)
          .foreach { (lo, hi) =>
            val b = bounds(g)
            bounds(g) =
              if b == null then
                (lo, hi)
              else
                (math.min(b._1, lo), math.max(b._2, hi))
          }
        if rows <= RowKeyFilter.MaxKeys then
          held(g) += k.gather(valid, nv)
        else
          held(g).clear()
        if sample(g) == null then
          sample(g) = k.gather(valid, 1)
        g += 1

  /** Set the filters from every key added */
  def set(steps: TaskRunner): Unit =
    var g = 0
    while g < groups.size do
      val (k, fs) = groups(g)
      val b       = bounds(g)
      if b != null then
        if rows <= RowKeyFilter.MaxKeys then
          val vs  = held(g).toSeq
          val n   = vs.iterator.map(_.size).sum
          val all = Batch.concatVecs(keys(k).ptype, vs, n)
          RuntimeFilter.setAll(fs, all, Array.range(0, n), n, steps)
        else
          // Past MaxKeys an in-memory build sets a range per-row filter too
          lazy val perRow = RowKeyFilter.of(sample(g), Array.empty[Int], rows.toInt, b._1, b._2)
          fs.foreach(_.set(b, perRow))
      held(g).clear()
      g += 1

end FilterKeys

object FilterKeys:
  /** The collector of the keys `filters` read, null without filters */
  def of(filters: Seq[RuntimeFilter], keys: IndexedSeq[VExpr]): FilterKeys =
    if filters.isEmpty then
      null
    else
      FilterKeys(filters.groupBy(_.key).toIndexedSeq.sortBy(_._1), keys)
