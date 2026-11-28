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

import scala.collection.mutable

/**
  * The hash table of a join's build (right) input: its rows, and for each distinct key the chain of
  * rows holding it. Built once on first use, then only read, so probes may share it across threads
  */
final class JoinBuild(
    right: Operator,
    rightKeys: IndexedSeq[VExpr],
    trackMatches: Boolean = false,
    runner: TaskRunner = TaskRunner.Sequential,
    filters: Seq[RuntimeFilter] = Nil,
    packKeys: Boolean = true,
    flatTable: Boolean = true,
    nullsEqual: Boolean = false,
    outputsBuild: Boolean = true,
    trackExclusions: Boolean = false
):
  @volatile
  private var built = false

  // The query's memory budget, captured where the build is planned: the build runs on whichever
  // thread probes first. A child: the build's reservation returns once no probe reads it
  private val queryMemory = MemoryContext.current
  private val memory      = queryMemory.child()

  /**
    * Whether a build past the query's spill threshold spills its partitions that don't fit
    * (`spill_join`); set before the build is made
    */
  var spill: Boolean = false

  /**
    * 0 for a join's own build, 1 for the build of a spilled partition (which may split once more)
    */
  private[exec] var spillDepth: Int = 0

  /** The spilled partitions, or null when the build fits in memory */
  @volatile
  private var spilled: JoinSpill = null

  /** The build's spilled state once built, null when it fits in memory */
  def spillState: JoinSpill = spilled

  /** Whether the build spilled some of its partitions (known once built) */
  def isSpilled: Boolean = spilled != null

  /** The query's memory context, under which the joins of spilled partitions build */
  private[exec] def queryContext: MemoryContext = queryMemory

  /** The runner of the build steps */
  private[exec] def buildRunner: TaskRunner = runner

  // Whether copies of the join probe this build (parallel parts): only their pipeline may free it
  @volatile
  private var copied = false

  /** Whether copies of the join probe this build */
  def hasCopies: Boolean = copied

  private[exec] def markCopied(): Unit = copied = true

  /**
    * Drop the table and return its reservation, once every probe of it has finished (the last part
    * and tail of its pipeline, or a join probing it alone). Only with memory accounting on, so
    * turning it off keeps the tables to the query's end as before
    */
  def free(): Unit =
    if memory.isTracking then
      synchronized {
        build = null
        hasher = null
        tables = null
        heads = null
        chain = null
        packer = null
        directHeads = null
        directStore = null
        flat = null
        matched = null
        excluded = null
      }
      memory.release()

  // Runtime filters drop NULL probe keys, which match NULL build keys here
  require(!nullsEqual || filters.isEmpty, "A join with NULL-equal keys takes no runtime filters")

  // With `nullsEqual`, whether a build key has a NULL: NULL probe keys then look up the hash
  // tables as values, which the packed, direct, and flat paths cannot hold
  private var nullKeys = false

  var build: Batch = null

  // Rows are partitioned by the hash of their key, one hash table per partition; a single
  // partition for small builds. `heads(p)(g)` is the first row of group g of partition p, and
  // `chain(row)` the next row with the same key
  private var parts: Int                = 1
  private var hasher: GroupTable        = null
  private var tables: Array[GroupTable] = null
  private var heads: Array[Array[Int]]  = null
  private var chain: Array[Int]         = null
  // Multi-column integer keys packed into one Int64 key from the build key ranges, so the tables
  // compare one Long per candidate; null when the keys are not packed
  private var packer: KeyPacker = null
  // A single integer key over a dense range: `directHeads(key - directMin)` is the first row with
  // the key (DuckDB's perfect hash join, with chains for duplicate keys), so a probe neither hashes
  // nor compares keys. `directStore` reads probe keys in the build key's representation. Null when
  // the build uses hash tables
  private var directHeads: Array[Int]          = null
  private var directMin: Long                  = 0L
  private var directStore: GroupTable.KeyStore = null
  // A single integer key outside a dense range: one flat table over (key, first row) pairs, null
  // when the build uses a direct index or GroupTables. `directStore` normalizes its probe keys too
  private var flat: LongJoinTable = null

  /** Whether no two build rows share a key, so every chain ends at its first row */
  var uniqueKeys: Boolean = false

  /** The row with the same key after `row`, or -1 */
  inline def nextMatch(row: Int): Int = chain(row)

  /**
    * Whether a dense single integer key may use a direct index (`join_direct_index`); set before
    * the build is made
    */
  var directIndex: Boolean = true

  /**
    * Whether a large build concatenates its input, finds its valid rows and key ranges, and fills
    * its direct index on the runner instead of the calling thread (`parallel_join_build`); set
    * before the build is made
    */
  var parallelBuild: Boolean = true

  /**
    * Whether a build of at most [[JoinBuild.MaxCodedRows]] rows encodes its dictionary-encoded
    * string columns over their distinct values ([[JoinBuildCodes.encodeStrings]]), so that probe
    * outputs carry codes shared by every batch (`join_build_string_codes`); set before the build is
    * made
    */
  var stringCodes: Boolean = true

  /** Whether string columns give up encoding once they look nearly unique (`string_code_probe`) */
  var codeProbe: Boolean = true

  /** Whether a large flat table keeps a Bloom filter of its keys (`probe_miss_filter`) */
  var missFilter: Boolean = false

  /** Whether narrow flat entries fit the row number in fewer bits (`narrow_join_entries`) */
  var narrowEntries: Boolean = true

  /** Whether the key steps of a large build run on the runner too (`parallel_join_keys`) */
  var parallelKeys: Boolean = true

  /** Whether the build probes through a direct index instead of hash tables (for tests) */
  def usesDirectIndex: Boolean = directHeads != null

  /** Whether the build probes through a [[LongJoinTable]] (for tests) */
  def usesFlatTable: Boolean = flat != null

  /**
    * Build rows some probe matched, shared by the probe copies of a right / full join. Copies only
    * ever set entries to true, and the unmatched rows are read after every copy has finished
    */
  var matched: Array[Boolean] = null

  /** Build rows a probe row excluded (`exclude` of a RIGHT SEMI join), shared as `matched` is */
  var excluded: Array[Boolean] = null

  /** Whether the table is built, so that probes may read it */
  def isBuilt: Boolean = built

  def ensureBuilt(): Unit =
    if !built then
      synchronized {
        if !built then
          buildTable()
          built = true
      }

  /**
    * The build input with `spill`: read with [[MemoryContext.tryReserve]] of each batch and what
    * building on it adds ([[JoinBuild.inputBytes]]), routed to [[JoinSpill]] partitions once the
    * query would pass its spill threshold. Returns the rows to build the table over (the kept
    * partitions when spilled) and the context counting them until the table is built
    */
  private def readSpillable(): (Seq[Batch], MemoryContext) =
    val input = memory.child()
    val b     = mutable.ArrayBuffer.empty[Batch]
    var x     = right.next()
    while x != null do
      val c = x.compact()
      if spilled == null && !input.tryReserve(JoinBuild.inputBytes(c)) then
        // The buffered rows move to the partitions, which count them again
        val s = JoinSpill(right.schema, rightKeys, memory, spillDepth)
        input.release()
        filterKeys = FilterKeys.of(filters, rightKeys)
        b.foreach { y =>
          if filterKeys != null then
            filterKeys.add(y)
          s.add(y)
        }
        b.clear()
        spilled = s
      if spilled != null then
        if filterKeys != null then
          filterKeys.add(c)
        spilled.add(c)
      else
        b += c
      x = right.next()
    if spilled != null then
      (spilled.finish(), null)
    else
      (b.toSeq, input)

  // The build keys of a spilled build that its runtime filters need, collected while routing
  private var filterKeys: FilterKeys = null

  private def buildTable(): Unit =
    val spillable        = spill && memory.canSpill
    val (batches, input) =
      if spillable then
        readSpillable()
      else
        (memory.drain(right), null)
    val rows = batches.iterator.map(_.size.toLong).sum
    // The row chains and hash tables or direct index, before they are allocated (counted ahead
    // with the input when spillable)
    if !spillable then
      memory.reserve(rows * JoinBuild.IndexBytesPerRow)
    // The runner of the build steps after the input (`parallel_join_build`): small builds stay on
    // the calling thread
    val steps =
      if parallelBuild && runner.parallelism > 1 && rows >= JoinBuild.MinParallelRows then
        runner
      else
        TaskRunner.Sequential
    build =
      if batches.isEmpty then
        Batch.empty(right.schema)
      else if steps eq TaskRunner.Sequential then
        Batch.concat(right.schema, batches)
      else
        JoinBuildSteps.concat(right.schema, batches.toIndexedSeq, steps)
    // Joins that output only probe columns never pass the codes on
    if stringCodes && outputsBuild then
      build = JoinBuildCodes.encodeStrings(build, batches, steps, codeProbe)
    val rawKeys  = rightKeys.map(_.eval(build)).toArray
    val rawTypes = rightKeys.map(_.ptype)
    // Rows with NULL keys never match, unless NULLs are equal (`IS NOT DISTINCT FROM`)
    nullKeys = nullsEqual && rawKeys.exists(k => k.nulls != null && k.nulls.contains(true))
    // With NULL keys equal, every row is valid: no key columns to check for NULLs
    val (valid, nv) = JoinBuildSteps.validRows(
      if nullKeys then
        Array.empty
      else
        rawKeys
      ,
      build.size,
      steps
    )
    if spilled == null then
      filters
        .groupBy(_.key)
        .foreach((k, fs) => RuntimeFilter.setAll(fs, rawKeys(k), valid, nv, steps))
    else if filterKeys != null then
      // From every build key, not only the kept partitions'
      filterKeys.set(steps)
      filterKeys = null
    val keySteps =
      if parallelKeys then
        steps
      else
        TaskRunner.Sequential
    if packKeys && !nullKeys then
      packer = KeyPacker.of(rawTypes, rawKeys, valid, nv, keySteps)
    // The key columns the tables hold
    val (keyVecs, types) =
      if packer == null then
        (rawKeys, rawTypes)
      else
        (Array[Vec](packer.pack(rawKeys, build.size, keySteps)), IndexedSeq(PType.Int64))
    chain = JoinBuildSteps.filledInts(build.size, -1, steps)
    if directIndex && types.size == 1 && !nullKeys then
      buildDirect(keyVecs(0), types(0), valid, nv, steps)
    if directHeads == null && flatTable && types.size == 1 && !nullKeys then
      buildFlat(keyVecs(0), types(0), valid, nv, keySteps)
    if directHeads == null && flat == null then
      buildHashed(keyVecs, types, valid, nv)
    if trackMatches then
      matched = new Array[Boolean](build.size)
    if trackExclusions then
      excluded = new Array[Boolean](build.size)
    if spillable then
      // The table and its rows, counted at their sizes, replace the input's ahead reservation
      val held = MemoryContext.estimatedBytes(build) + structureBytes
      if input != null then
        input.release()
      if spilled != null then
        spilled.releaseKept()
      memory.reserve(held)

  end buildTable

  /** The bytes of the build's chains, tables or direct index, and match marks */
  private def structureBytes: Long =
    val n     = build.size.toLong
    var bytes = 4L * n
    if directHeads != null then
      bytes += 4L * directHeads.length
    else if flat != null then
      bytes += flat.sizeBytes
    else
      bytes += JoinBuild.HashedBytesPerRow * n
    if matched != null then
      bytes += n
    if excluded != null then
      bytes += n
    bytes

  /**
    * The direct index of a single Int32 / Date / Int64 key whose range over the valid rows spans at
    * most [[JoinBuild.DirectRangeFactor]] slots per row, or at most [[JoinBuild.DirectMinSlots]];
    * leaves `directHeads` null otherwise
    */
  private def buildDirect(key: Vec, t: PType, valid: Array[Int], nv: Int, steps: TaskRunner): Unit =
    if nv == 0 || !(t == PType.Int32 || t == PType.Date || t == PType.Int64) then
      return
    val store = GroupTable.KeyStore(t)
    val norm  = store.normalize(key)
    // The key of row r as a Long; valid rows have no NULL key
    val ints: Array[Int] =
      norm match
        case v: IntVec =>
          v.values
        case _ =>
          null
    val longs: Array[Long] =
      norm match
        case v: LongVec =>
          v.values
        case _ =>
          null
    if ints == null && longs == null then
      return
    def keyAt(r: Int): Long =
      if ints != null then
        ints(r).toLong
      else
        longs(r)
    val (lo, hi) = JoinBuildSteps.keyRange(norm, valid, nv, steps).get
    val range    = hi - lo
    // A negative range overflowed a Long
    if range < 0 ||
      range >=
        math.min(
          JoinBuild.DirectMaxSlots,
          math.max(JoinBuild.DirectMinSlots, nv.toLong * JoinBuild.DirectRangeFactor)
        )
    then
      return
    val heads = JoinBuildSteps.filledInts(range.toInt + 1, -1, steps)
    // Insert rows(from until until) in reverse, so chains enumerate rows in input order; whether
    // two of them share a key
    def insert(rows: Array[Int], from: Int, until: Int): Boolean =
      var dup = false
      var j   = until - 1
      while j >= from do
        val row = rows(j)
        val k   = (keyAt(row) - lo).toInt
        val h   = heads(k)
        if h >= 0 then
          dup = true
        chain(row) = h
        heads(k) = row
        j -= 1
      dup
    val dup =
      if steps eq TaskRunner.Sequential then
        insert(valid, 0, nv)
      else
        // Split the key range into slices, one task each, so that no two tasks write the same
        // `heads` slot. Row chunks scatter their rows by slice into `order`, chunk after chunk, so
        // each slice lists its rows in input order and its chains come out as in a serial build
        val tasks  = JoinBuildSteps.taskCount(steps)
        val shift  = math.max(0, 64 - java.lang.Long.numberOfLeadingZeros(range / tasks))
        val slices = (range >>> shift).toInt + 1
        val chunks = JoinBuildSteps.chunks(nv, tasks)
        val counts = Array.ofDim[Int](chunks.length - 1, slices)
        JoinBuildSteps.runAll(chunks.length - 1, steps) { c =>
          val cnt = counts(c)
          var j   = chunks(c)
          while j < chunks(c + 1) do
            cnt(((keyAt(valid(j)) - lo) >>> shift).toInt) += 1
            j += 1
        }
        // Where each slice starts in `order`, and where each chunk writes within it
        val sliceStart = new Array[Int](slices + 1)
        val pos        = Array.ofDim[Int](chunks.length - 1, slices)
        var off        = 0
        var s          = 0
        while s < slices do
          sliceStart(s) = off
          var c = 0
          while c < counts.length do
            pos(c)(s) = off
            off += counts(c)(s)
            c += 1
          s += 1
        sliceStart(slices) = off
        val order = new Array[Int](nv)
        JoinBuildSteps.runAll(chunks.length - 1, steps) { c =>
          val p = pos(c)
          var j = chunks(c)
          while j < chunks(c + 1) do
            val row = valid(j)
            val sl  = ((keyAt(row) - lo) >>> shift).toInt
            order(p(sl)) = row
            p(sl) += 1
            j += 1
        }
        val dups = new Array[Boolean](slices)
        JoinBuildSteps.runAll(slices, steps) { s =>
          dups(s) = insert(order, sliceStart(s), sliceStart(s + 1))
        }
        dups.contains(true)
    directMin = lo
    directStore = store
    uniqueKeys = !dup
    directHeads = heads

  end buildDirect

  /** The [[LongJoinTable]] of a single Int32 / Date / Int64 key; leaves `flat` null otherwise */
  private def buildFlat(key: Vec, t: PType, valid: Array[Int], nv: Int, steps: TaskRunner): Unit =
    if !(t == PType.Int32 || t == PType.Date || t == PType.Int64) then
      return
    val store = GroupTable.KeyStore(t)
    // Every key as a Long; the values of rows with a NULL key are never read
    val longs: Array[Long] =
      store.normalize(key) match
        case v: LongVec =>
          v.values
        case v: IntVec =>
          val a = new Array[Long](v.size)
          var i = 0
          while i < a.length do
            a(i) = v.values(i).toLong
            i += 1
          a
        case _ =>
          return
    if runner.parallelism > 1 && nv >= JoinBuild.MinParallelRows then
      parts = GroupTable.partitionCount(runner.parallelism)
    val rowsOf =
      if parts == 1 then
        Array(java.util.Arrays.copyOf(valid, nv))
      else if !(steps eq TaskRunner.Sequential) then
        JoinBuildSteps.flatPartitions(longs, valid, nv, parts, steps)
      else
        val h = new Array[Int](nv)
        var j = 0
        while j < nv do
          h(j) = LongJoinTable.hash(longs(valid(j)))
          j += 1
        val byPos = GroupTable.partitionRows(h, nv, parts)
        // Positions among the valid rows -> build rows
        byPos.foreach { r =>
          var k = 0
          while k < r.length do
            r(k) = valid(r(k))
            k += 1
        }
        byPos
    val (table, unique) = LongJoinTable.build(
      longs,
      rowsOf,
      Integer.numberOfTrailingZeros(parts),
      chain,
      runner,
      missFilter,
      fitRowBits = narrowEntries,
      parallelSteps = !(steps eq TaskRunner.Sequential)
    )
    // A null table: a skewed build, which GroupTables take
    if table != null then
      directStore = store
      uniqueKeys = unique
      flat = table

  end buildFlat

  /** Hash tables over the valid rows, one per hash partition */
  private def buildHashed(
      keyVecs: Array[Vec],
      types: IndexedSeq[PType],
      valid: Array[Int],
      nv: Int
  ): Unit =
    if runner.parallelism > 1 && nv >= JoinBuild.MinParallelRows then
      parts = GroupTable.partitionCount(runner.parallelism)
    // Rows of each partition, in input order, and (with several partitions) their key hashes, so
    // that partition tables do not hash the keys again
    var hashesOf: Array[Array[Int]] = null
    val rowsOf: Array[Array[Int]]   =
      if parts == 1 then
        Array(java.util.Arrays.copyOf(valid, nv))
      else
        hasher = GroupTable(types)
        val h     = hasher.keyHashes(keyVecs.map(_.gather(valid, nv)), nv)
        val byPos = GroupTable.partitionRows(h, nv, parts)
        hashesOf = new Array[Array[Int]](parts)
        // Positions among the valid rows -> build rows (a loop: Array.map would box every Int)
        var p = 0
        while p < parts do
          val r  = byPos(p)
          val ph = new Array[Int](r.length)
          var j  = 0
          while j < r.length do
            ph(j) = h(r(j))
            r(j) = valid(r(j))
            j += 1
          hashesOf(p) = ph
          p += 1
        byPos
    tables = new Array[GroupTable](parts)
    heads = new Array[Array[Int]](parts)
    val uniqueParts             = new Array[Boolean](parts)
    def buildPart(p: Int): Unit =
      val rows = rowsOf(p)
      val n    = rows.length
      // A partition has at most one group per row
      val table   = GroupTable(types, math.min(n, GroupTable.MaxPresizeGroups))
      val partKey = keyVecs.map(_.gather(rows, n))
      // Hashes passed in, so the table, which lives through the probes, keeps no hash buffer
      val hashes =
        if hashesOf == null then
          table.keyHashes(partKey, n)
        else
          hashesOf(p)
      val ids  = table.groupIds(partKey, n, hashes)
      val head = Vec.filledInts(table.numGroups, -1)
      // Insert in reverse so chains enumerate rows in input order. Partitions hold disjoint rows,
      // so they write disjoint chain entries
      var j = n - 1
      while j >= 0 do
        val row = rows(j)
        val g   = ids(j)
        chain(row) = head(g)
        head(g) = row
        j -= 1
      tables(p) = table
      heads(p) = head
      uniqueParts(p) = table.numGroups == n
    if parts == 1 then
      buildPart(0)
    else
      (0 until parts).map(p => runner.submit(() => buildPart(p))).foreach(_.get())
    uniqueKeys = !uniqueParts.contains(false)

  end buildHashed

  /**
    * The first build row with the key of each probe row, or -1 when none has it (or the probe key
    * has a NULL, unless `nullsEqual`). Safe to call from several threads once built
    */
  def firstMatches(probeKeys: Array[Vec], n: Int): Array[Int] =
    val out = new Array[Int](n)
    // Probe rows outside the build key ranges pack as NULL, which never match
    val keys =
      if packer == null then
        probeKeys
      else
        Array[Vec](packer.pack(probeKeys, n))
    if directHeads != null then
      directMatches(keys(0), n, out)
    else if flat != null then
      directStore.normalize(keys(0)) match
        case v: LongVec =>
          flat.firstRows(v.values, v.nulls, n, out)
        case v: IntVec =>
          flat.firstRows(v.values, v.nulls, n, out)
        case other =>
          throw IllegalStateException(
            s"Unexpected join key vector: ${other.getClass.getSimpleName}"
          )
    else if parts == 1 then
      val ids  = tables(0).lookup(keys, n, nullsMatch = nullKeys)
      val head = heads(0)
      var i    = 0
      while i < n do
        val g = ids(i)
        out(i) =
          if g >= 0 then
            head(g)
          else
            -1
        i += 1
    else
      // Hash once: the partition tables hash alike, so they take each partition's hashes
      val h      = hasher.keyHashes(keys, n)
      val rowsOf = GroupTable.partitionRows(h, n, parts)
      var p      = 0
      while p < parts do
        val rows = rowsOf(p)
        val m    = rows.length
        if m > 0 then
          // Gathered keys keep the table's key comparisons sequential
          val ph = new Array[Int](m)
          var j  = 0
          while j < m do
            ph(j) = h(rows(j))
            j += 1
          val ids  = tables(p).lookup(keys.map(_.gather(rows, m)), m, ph, nullKeys)
          val head = heads(p)
          j = 0
          while j < m do
            val g = ids(j)
            out(rows(j)) =
              if g >= 0 then
                head(g)
              else
                -1
            j += 1
        p += 1
    end if
    out

  end firstMatches

  /** [[firstMatches]] through the direct index */
  private def directMatches(key: Vec, n: Int, out: Array[Int]): Unit =
    val norm  = directStore.normalize(key)
    val nulls = norm.nulls
    val heads = directHeads
    val lo    = directMin
    val size  = heads.length.toLong
    var i     = 0
    norm match
      case v: IntVec =>
        val x = v.values
        while i < n do
          val k = x(i).toLong - lo
          out(i) =
            if k >= 0 && k < size && (nulls == null || !nulls(i)) then
              heads(k.toInt)
            else
              -1
          i += 1
      case v: LongVec =>
        val x = v.values
        while i < n do
          val k = x(i) - lo
          // k < 0 also catches a subtraction that overflowed past Long.MaxValue
          out(i) =
            if k >= 0 && k < size && (nulls == null || !nulls(i)) then
              heads(k.toInt)
            else
              -1
          i += 1
      case other =>
        throw IllegalStateException(s"Unexpected join key vector: ${other.getClass.getSimpleName}")

  end directMatches

end JoinBuild

object JoinBuild:
  /**
    * Bytes per build row a memory reservation counts for the row chains (4) and the hash tables or
    * direct index (~8 at their load factors)
    */
  private[exec] val IndexBytesPerRow = 12L

  /** Bytes per row of hashed tables: slots at half load, group hashes, and the stored keys */
  private[exec] val HashedBytesPerRow = 24L

  /**
    * The bytes a spillable build reserves for a batch of its input ahead (`spill_join`): the batch,
    * its copy in the concatenated build, and per row its valid-row list, packed key, chain, and
    * table entries, so the build spills before building it would take the heap past the threshold
    */
  private[exec] def inputBytes(b: Batch): Long =
    2 * MemoryContext.estimatedBytes(b) + b.size.toLong * (8L + 4L + 4L + HashedBytesPerRow)

  /** Builds with fewer rows use one hash table, built on the calling thread */
  def MinParallelRows: Int = sys
    .props
    .get("wvlet.engine.partitionedJoinMinRows")
    .flatMap(_.toIntOption)
    .getOrElse(65536)

  /**
    * Beyond [[DirectMinSlots]], a direct index takes at most this many slots per build row: 4 bytes
    * each, so at most 16 bytes per row, under what a hash table at half load takes
    */
  inline val DirectRangeFactor = 4

  /**
    * A direct index may always take this many slots (1 MB), however few build rows it has: it stays
    * in cache, and a probe makes one load instead of hashing and comparing. Small builds over wide
    * key ranges (a dimension filtered to a few hundred rows) take it, so their probes, mostly
    * misses, are cheap
    */
  inline val DirectMinSlots = 1L << 18

  /** The most slots a direct index takes */
  inline val DirectMaxSlots = 1L << 27

  /** The most rows of a build whose string columns are encoded (`join_build_string_codes`) */
  inline val MaxCodedRows = 1 << 20

  /**
    * The most rows of a build whose string columns without Parquet dictionary codes are encoded
    * too, hashing each row (small dimensions such as TPC-DS `call_center`)
    */
  inline val MaxPlainCodedRows = 1 << 16

end JoinBuild

/** How a right / full join tracks and emits build rows no probe row matched */
enum OuterMode:
  /** Tracks its own matches and emits the unmatched rows at the end of its input */
  case Own

  /** One part of a split probe: marks the shared build's matches, emits no unmatched rows */
  case Part

  /**
    * The last run after every part: marks the shared matches, then emits the unmatched rows (those
    * in [[HashJoinOp.withTailRange]] only, `parallel_outer_tail`)
    */
  case Tail

/**
  * Equi hash join building on the right input. Output: left columns then right columns (Semi /
  * Anti: left columns only). `residual` is an extra condition over the joined row.
  *
  * `shared` is a build already made for this join elsewhere: copies of the join that probe parts of
  * the left input in parallel share one build. `exclude`: see [[BuildExclusions]]
  */
class HashJoinOp(
    val left: Operator,
    val right: Operator,
    val leftKeys: IndexedSeq[VExpr],
    val rightKeys: IndexedSeq[VExpr],
    val kind: JoinKind,
    val residual: Option[VExpr],
    val schema: Schema,
    shared: JoinBuild = null,
    outerMode: OuterMode = OuterMode.Own,
    buildRunner: TaskRunner = TaskRunner.Sequential,
    val runtimeFilters: Seq[RuntimeFilter] = Nil,
    val nullsEqual: Boolean = false,
    val exclude: Option[VExpr] = None
) extends Operator:
  override def children: Seq[Operator] = Seq(left, right)
  override def name: String            = s"HashJoin[${kind}]"
  // `hashed` marks `join_direct_index` disabled, as `eager` marks `late_gather`. Whether a build
  // actually uses a direct index depends on its key range, known only once it is built
  override protected def details =
    s"(${leftKeys.map(_.sig).mkString(",")} = ${rightKeys.map(_.sig).mkString(",")}${residual
        .map(r => s", residual=${r.sig}")
        .getOrElse("")}${BuildExclusions.tag(exclude)}${Operator.eagerTag(lateGather)}${
        if directIndex then
          ""
        else
          ", hashed"
      }${
        if parallelBuild then
          ""
        else
          ", serial build"
      }${
        if nullsEqual then
          ", nulls equal"
        else
          ""
      }${ProbeRows.tags(this)})"

  /** The build this join probes */
  val joinBuild: JoinBuild =
    if shared != null then
      shared.markCopied()
      shared
    else
      JoinBuild(
        right,
        rightKeys,
        trackMatches = kind.tracksBuildMatches,
        runner = buildRunner,
        filters = runtimeFilters,
        nullsEqual = nullsEqual,
        outputsBuild = !kind.outputsProbeOnly,
        trackExclusions = exclude.nonEmpty
      )

  /** The same join over another left input, sharing this join's build */
  def withLeft(newLeft: Operator, mode: OuterMode = OuterMode.Own): HashJoinOp = HashJoinOp(
    newLeft,
    right,
    leftKeys,
    rightKeys,
    kind,
    residual,
    schema,
    joinBuild,
    mode,
    nullsEqual = nullsEqual,
    exclude = exclude
  ).withLateGather(lateGather).withUnmatchedOnly(unmatchedOnly)

  override def close(): Unit =
    if partJoin != null then
      partJoin.close()
      partJoin = null
    // A shared build's right input belongs to the join that made the build
    if shared == null then
      super.close()
    else
      left.close()

  private var built                         = false
  private var build: Batch                  = null
  private var buildMatched: Array[Boolean]  = null
  private var buildExcluded: Array[Boolean] = null
  private var emittedUnmatched              = false
  private val leftWidth                     = left.schema.size

  /** Whether the output columns are deferred instead of gathered (`late_gather`) */
  var lateGather: Boolean = true

  def withLateGather(enabled: Boolean): this.type =
    lateGather = enabled
    this

  /** Whether this LEFT join emits only its unmatched rows (`unmatched_left_probe`) */
  var unmatchedOnly: Boolean = false

  def withUnmatchedOnly(enabled: Boolean): this.type =
    unmatchedOnly = enabled && kind == JoinKind.Left && residual.isEmpty
    this

  def withMissFilter(enabled: Boolean): this.type =
    joinBuild.missFilter = enabled
    this

  /** The filters below the probe side that read this build's keys (`left_match_prefilter`) */
  var prefilters: Seq[BuildKeyMatch.Source] = Nil

  // The build positions [from, until) whose unmatched rows a tail run emits
  private var tailRange = (0, Int.MaxValue)

  /** Emit only the unmatched build rows at positions [from, until) (`parallel_outer_tail`) */
  def withTailRange(from: Int, until: Int): this.type =
    tailRange = (from, until)
    this

  /** Whether the build may use a direct index for a dense integer key (`join_direct_index`) */
  def directIndex: Boolean = joinBuild.directIndex

  def withDirectIndex(enabled: Boolean): this.type =
    joinBuild.directIndex = enabled
    this

  /** Whether a large build runs its steps on the build runner (`parallel_join_build`) */
  def parallelBuild: Boolean = joinBuild.parallelBuild

  def withParallelBuild(enabled: Boolean): this.type =
    joinBuild.parallelBuild = enabled
    this

  /** Whether the build encodes its string columns (`join_build_string_codes`) */
  def stringCodes: Boolean = joinBuild.stringCodes

  def withStringCodes(enabled: Boolean): this.type =
    joinBuild.stringCodes = enabled
    this

  def withCodeProbe(enabled: Boolean): this.type =
    joinBuild.codeProbe = enabled
    this

  private def rowsOf(b: Batch, idx: Array[Int], n: Int): Batch =
    if lateGather then
      b.take(idx, n)
    else
      b.gather(idx, n)

  /** Probe rows `lIdx` next to build rows `rIdx` (-1 for NULLs) */
  private def joined(probe: Batch, lIdx: Array[Int], rIdx: Array[Int]): Batch =
    rowsOf(probe, lIdx, lIdx.length) ++ rowsOf(build, rIdx, rIdx.length)

  private def buildTable(): Unit =
    joinBuild.ensureBuilt()
    build = joinBuild.build
    if kind.tracksBuildMatches then
      buildMatched =
        outerMode match
          case OuterMode.Own if shared == null =>
            // This join's own build: its matched array is this join's alone
            joinBuild.matched
          case OuterMode.Own =>
            new Array[Boolean](build.size)
          case _ =>
            joinBuild.matched
    if exclude.nonEmpty then
      buildExcluded = BuildExclusions.marks(joinBuild, outerMode, shared != null, build.size)
    built = true

  /**
    * Whether a build past the spill threshold spills its partitions that don't fit (`spill_join`)
    */
  def spills: Boolean = joinBuild.spill

  def withSpill(enabled: Boolean): this.type =
    // Probe rows of spilled partitions are written to files too
    joinBuild.spill =
      enabled && SpillBlocks.spillable(left.schema) && SpillBlocks.spillable(right.schema)
    this

  // Whether this join joins its build's spilled partitions once its probe input ends: the join
  // that owns its build, or the copy in a pipeline's spill tail ([[withSpillFinish]]); the copies
  // probing parts only route their rows
  private var finishesSpill = shared == null

  /** Make this copy the one that joins the spilled partitions, after every other probe of them */
  def withSpillFinish(): this.type =
    finishesSpill = true
    this

  // The router of this copy's probe rows, once the build spilled
  private var router: JoinSpill.ProbeRouter = null
  // 0: probing, 1: emitting the unmatched rows of the in-memory build, 2: joining the spilled
  // partitions, 3: done
  private var phase = 0
  // The next spilled partition to join, and the join of the current one
  private var nextPartition        = 0
  private var partJoin: HashJoinOp = null

  def next(): Batch =
    if !built then
      buildTable()
    var out: Batch = null
    while out == null do
      phase match
        case 0 =>
          val probe = left.next()
          if probe == null then
            if router != null then
              router.flush()
            phase = 1
          else
            out = probeOrRoute(probe)
        case 1 =>
          out = emitUnmatchedBuild()
          if out == null then
            phase = 2
            if finishesSpill && joinBuild.isSpilled then
              // Every probe routed its rows: the in-memory build is done with
              joinBuild.spillState.finishProbes()
              build = null
              buildMatched = null
              buildExcluded = null
              joinBuild.free()
            else
              phase = 3
        case 2 =>
          out = nextSpilled()
          if out == null then
            phase = 3
        case _ =>
          freeOwnBuild()
          return null
    end while
    out

  end next

  private def probeOrRoute(probe: Batch): Batch =
    val s = joinBuild.spillState
    if s == null then
      probeBatch(probe)
    else
      if router == null then
        router = s.probeRouter(left.schema, leftKeys)
      val kept = router.route(probe)
      if kept == null then
        null
      else
        probeBatch(kept)

  /**
    * The next rows of the spilled partitions' joins: each partition's build file joined with its
    * probe file on its own, with the same join kind, then its files deleted. Null once every
    * partition is joined
    */
  private def nextSpilled(): Batch =
    val s          = joinBuild.spillState
    var out: Batch = null
    while out == null do
      if partJoin == null then
        while nextPartition < JoinSpill.Partitions && !needsJoin(s, nextPartition) do
          s.delete(nextPartition)
          nextPartition += 1
        if nextPartition >= JoinSpill.Partitions then
          return null
        partJoin = partitionJoin(s, nextPartition)
      out = partJoin.next()
      if out == null then
        partJoin.close()
        partJoin = null
        s.delete(nextPartition)
        nextPartition += 1
    out

  // Whether spilled partition p can emit rows: it has build rows, and probe rows unless the join
  // emits unmatched build rows
  private def needsJoin(s: JoinSpill, p: Int): Boolean =
    s.buildFiles(p) != null &&
      (s.probeFiles(p) != null || (kind.tracksBuildMatches && kind != JoinKind.RightSemi))

  private def partitionJoin(s: JoinSpill, p: Int): HashJoinOp =
    val bf    = s.buildFiles(p)
    val pf    = s.probeFiles(p)
    val probe = IteratorOp(
      left.schema,
      () =>
        if pf == null then
          Iterator.empty
        else
          pf.read()
      ,
      "SpilledProbe"
    )
    val buildInput = IteratorOp(right.schema, () => bf.read(), "SpilledBuild")
    MemoryContext.withCurrent(joinBuild.queryContext) {
      val j = HashJoinOp(
        probe,
        buildInput,
        leftKeys,
        rightKeys,
        kind,
        residual,
        schema,
        buildRunner = joinBuild.buildRunner,
        nullsEqual = nullsEqual,
        exclude = exclude
      ).withFlagsOf(this)
      // Its output outlives the partition's build, which its next partition frees
      j.withLateGather(false)
      // A partition that doesn't fit splits once more, by another hash
      j.joinBuild.spillDepth = joinBuild.spillDepth + 1
      j.withSpill(joinBuild.spillDepth < JoinSpill.MaxDepth)
      JoinSpill.partitionsJoined.incrementAndGet()
      j
    }

  end partitionJoin

  /** A join probing its build alone frees it once its probe input and unmatched rows are done */
  private def freeOwnBuild(): Unit =
    if shared == null && !joinBuild.hasCopies && build != null then
      build = null
      buildMatched = null
      buildExcluded = null
      joinBuild.free()

  private def emitUnmatchedBuild(): Batch =
    if buildMatched == null || emittedUnmatched || outerMode == OuterMode.Part then
      return null
    emittedUnmatched = true
    // RIGHT SEMI emits the matched build rows; the other kinds the unmatched ones
    val wanted = kind == JoinKind.RightSemi
    val idx = BuildExclusions.positions(buildMatched, buildExcluded, build.size, tailRange, wanted)
    if idx.isEmpty then
      return null
    val n = idx.length
    if kind.outputsBuildOnly then
      rowsOf(build, idx, n)
    else
      val leftPad = left.schema.types.map(t => Vec.nullVec(t, n)).toArray
      Batch(leftPad, n) ++ rowsOf(build, idx, n)

  private def probeBatch(probe: Batch): Batch =
    val keyVecs = leftKeys.map(_.eval(probe)).toArray
    val first   = joinBuild.firstMatches(keyVecs, probe.size)
    if residual.isEmpty && kind.outputsProbeOnly then
      // A semi / anti join without a residual only asks whether a match exists: the first one
      // answers it, without walking the chain of build rows sharing the key
      return ProbeRows.exists(probe, first, kind == JoinKind.Semi, lateGather)
    if unmatchedOnly then
      return ProbeRows.unmatched(probe, build, first, lateGather)
    val n = probe.size
    // Matching pairs, sized for one match per probe row and doubled on overflow
    var lIdx = new Array[Int](n)
    var rIdx = new Array[Int](n)
    var np   = 0
    var i    = 0
    if joinBuild.uniqueKeys then
      // At most one match per probe row: the pairs fit, and no chain is read
      while i < n do
        val r = first(i)
        if r >= 0 then
          lIdx(np) = i
          rIdx(np) = r
          np += 1
        i += 1
    while i < n do
      var r = first(i)
      while r >= 0 do
        if np == lIdx.length then
          val cap = math.max(16, np * 2)
          lIdx = java.util.Arrays.copyOf(lIdx, cap)
          rIdx = java.util.Arrays.copyOf(rIdx, cap)
        lIdx(np) = i
        rIdx(np) = r
        np += 1
        r = joinBuild.nextMatch(r)
      i += 1
    if np < lIdx.length then
      lIdx = java.util.Arrays.copyOf(lIdx, np)
      rIdx = java.util.Arrays.copyOf(rIdx, np)
    // The pairs' columns, gathered once for the exclusion and the residual
    var pairs: Batch = null
    if exclude.nonEmpty && lIdx.nonEmpty then
      pairs = joined(probe, lIdx, rIdx)
      BuildExclusions.mark(exclude.get.eval(pairs), rIdx, buildExcluded)
    // The pairs the residual condition kept, reusing the columns it gathered (late_gather only)
    var kept: Batch = null
    // Apply the residual condition on candidate pairs
    residual.foreach { cond =>
      if lIdx.nonEmpty then
        // Only the columns the condition reads are gathered
        val candidates =
          if pairs != null then
            pairs
          else
            joined(probe, lIdx, rIdx)
        val mask = Kernels.trueMask(cond.eval(candidates))
        val keep = HashJoinOp.trueRows(mask, lIdx.length)
        lIdx = HashJoinOp.pick(lIdx, keep)
        rIdx = HashJoinOp.pick(rIdx, keep)
        if lateGather then
          kept = candidates.take(keep, keep.length)
    }
    def matchedPairs: Batch =
      if kept != null then
        kept
      else
        joined(probe, lIdx, rIdx)
    kind match
      case JoinKind.Inner | JoinKind.Cross =>
        if lIdx.isEmpty then
          null
        else
          matchedPairs
      case JoinKind.Left | JoinKind.Full | JoinKind.Right =>
        if buildMatched != null then
          rIdx.foreach(r => buildMatched(r) = true)
        if kind == JoinKind.Right then
          if lIdx.isEmpty then
            null
          else
            matchedPairs
        else
          val matched = new Array[Boolean](probe.size)
          var j       = 0
          while j < lIdx.length do
            matched(lIdx(j)) = true
            j += 1
          // Matched pairs, then each unmatched probe row against NULLs
          var total = lIdx.length
          i = 0
          while i < probe.size do
            if !matched(i) then
              total += 1
            i += 1
          if total == 0 then
            null
          else if total == lIdx.length then
            joined(probe, lIdx, rIdx)
          else
            val allL = java.util.Arrays.copyOf(lIdx, total)
            val allR = java.util.Arrays.copyOf(rIdx, total)
            var k    = lIdx.length
            i = 0
            while i < probe.size do
              if !matched(i) then
                allL(k) = i
                allR(k) = -1
                k += 1
              i += 1
            joined(probe, allL, allR)
        end if
      case JoinKind.Semi =>
        val matched = new Array[Boolean](probe.size)
        lIdx.foreach(l => matched(l) = true)
        val r =
          if lateGather then
            probe.takeWhere(matched)
          else
            probe.filter(matched)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.Anti =>
        val keep = new Array[Boolean](probe.size)
        java.util.Arrays.fill(keep, true)
        lIdx.foreach(l => keep(l) = false)
        val r =
          if lateGather then
            probe.takeWhere(keep)
          else
            probe.filter(keep)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.RightSemi | JoinKind.RightAnti =>
        // Only mark the matched build rows; they are emitted at the end of the input
        rIdx.foreach(r => buildMatched(r) = true)
        null
    end match

  end probeBatch

end HashJoinOp

object HashJoinOp:
  /** Positions 0 until n where mask is true */
  private[exec] def trueRows(mask: Array[Boolean], n: Int): Array[Int] = positionsOf(mask, n, true)

  /** Positions 0 until n where mask is `value` */
  private[exec] def positionsOf(mask: Array[Boolean], n: Int, value: Boolean): Array[Int] =
    var count = 0
    var i     = 0
    while i < n do
      if mask(i) == value then
        count += 1
      i += 1
    val out = new Array[Int](count)
    var k   = 0
    i = 0
    while i < n do
      if mask(i) == value then
        out(k) = i
        k += 1
      i += 1
    out

  /** xs at each position in idx (Array.map would box every Int) */
  private[exec] def pick(xs: Array[Int], idx: Array[Int]): Array[Int] =
    val out = new Array[Int](idx.length)
    var i   = 0
    while i < idx.length do
      out(i) = xs(idx(i))
      i += 1
    out

end HashJoinOp
