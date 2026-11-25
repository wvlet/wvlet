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

/** A composite grouping key. NULL key values are equal to each other (GROUP BY semantics) */
final class RowKey(val values: Array[Any]):
  override def hashCode(): Int =
    var h = 17
    var i = 0
    while i < values.length do
      val v = values(i)
      h =
        h * 31 + (
          if v == null then
            0
          else
            v.hashCode()
        )
      i += 1
    h

  override def equals(o: Any): Boolean =
    o match
      case k: RowKey =>
        java
          .util
          .Arrays
          .equals(values.asInstanceOf[Array[AnyRef]], k.values.asInstanceOf[Array[AnyRef]])
      case _ =>
        false

object GroupKey:
  /** The NULL key of single-column groups */
  object NullKey

  def single(v: Vec, i: Int): AnyRef =
    val x = v.get(i)
    if x == null then
      NullKey
    else
      x.asInstanceOf[AnyRef]

  def of(keys: Array[Vec], i: Int): AnyRef =
    if keys.length == 1 then
      single(keys(0), i)
    else
      val a = new Array[Any](keys.length)
      var k = 0
      while k < keys.length do
        a(k) = keys(k).get(i)
        k += 1
      RowKey(a)

  /** True when any key column of row i is NULL (such rows never match in joins) */
  def hasNull(keys: Array[Vec], i: Int): Boolean =
    var k = 0
    while k < keys.length do
      if keys(k).isNull(i) then
        return true
      k += 1
    false

end GroupKey

/**
  * Assigns dense group ids to distinct keys and keeps one copy of each key's values.
  *
  * An open-addressing hash table over typed key columns: hashes are computed a column at a time,
  * and keys are compared in place against the stored key columns, so no per-row key objects are
  * allocated. NULL key values are equal to each other (GROUP BY semantics); [[lookup]] treats rows
  * with a NULL key as unmatched (join semantics).
  *
  * With `longDecimals`, decimal key columns are stored and compared as unscaled Long values
  * ([[GroupTable.DecimalStore]]) instead of boxed BigDecimals.
  */
final class GroupTable(
    keyTypes: IndexedSeq[PType],
    expectedGroups: Int = 0,
    longDecimals: Boolean = false
) extends GroupIndex:
  import GroupTable.*

  private val stores: Array[KeyStore] = keyTypes.map(KeyStore(_, longDecimals)).toArray
  private val width                   = stores.length
  private var count                   = 0
  // Each slot packs (hash << 32 | group id), or is Empty: a probe compares the hash without
  // loading anything of the group, as DuckDB's salted hash table entries
  private var slots = emptySlots(slotCapacity(expectedGroups))
  private var mask  = slots.length - 1
  // The hash of each group in id order, read sequentially when rehashing
  private var groupHash = new Array[Int](math.max(64, expectedGroups))
  // Per-batch row hashes of groupIds, reused across batches (inserting is single-threaded)
  private var hashBuf: Array[Int] = null

  if expectedGroups > 0 then
    stores.foreach(_.reserve(expectedGroups))

  def numGroups: Int = count

  private def normalize(keys: Array[Vec]): Array[Vec] =
    val out = new Array[Vec](width)
    var k   = 0
    while k < width do
      out(k) = stores(k).normalize(keys(k))
      k += 1
    out

  private def hashes(keys: Array[Vec], n: Int): Array[Int] = hashesInto(keys, n, new Array[Int](n))

  /** Row hashes into h, whose first n entries must be zero */
  private def hashesInto(keys: Array[Vec], n: Int, h: Array[Int]): Array[Int] =
    var k = 0
    while k < width do
      stores(k).hashInto(keys(k), n, h)
      k += 1
    var i = 0
    while i < n do
      h(i) = fmix(h(i))
      i += 1
    h

  private def sameKey(g: Int, keys: Array[Vec], i: Int): Boolean =
    var k = 0
    while k < width do
      if !stores(k).equalsAt(g, keys(k), i) then
        return false
      k += 1
    true

  /** The group of row i, or -1 when its key is not in the table */
  private def find(keys: Array[Vec], i: Int, h: Int): Int =
    var idx = h & mask
    while true do
      val e = slots(idx)
      if e == Empty then
        return -1
      if (e >>> 32).toInt == h then
        val g = e.toInt
        if sameKey(g, keys, i) then
          return g
      idx = (idx + 1) & mask
    -1

  /** [[find]] for one Int64 key column without NULLs: no virtual key comparison */
  private def findLong(store: LongStore, x: Array[Long], i: Int, h: Int): Int =
    var idx = h & mask
    while true do
      val e = slots(idx)
      if e == Empty then
        return -1
      if (e >>> 32).toInt == h then
        val g = e.toInt
        if store.valueAt(g) == x(i) && !store.isNullAt(g) then
          return g
      idx = (idx + 1) & mask
    -1

  /** [[find]] for one Int32 key column: no virtual key comparison */
  private def findInt(store: IntStore, x: Array[Int], i: Int, h: Int): Int =
    var idx = h & mask
    while true do
      val e = slots(idx)
      if e == Empty then
        return -1
      if (e >>> 32).toInt == h then
        val g = e.toInt
        if store.valueAt(g) == x(i) && !store.isNullAt(g) then
          return g
      idx = (idx + 1) & mask
    -1

  private def insert(keys: Array[Vec], i: Int, h: Int): Int =
    val g = count
    if g == groupHash.length then
      groupHash = java.util.Arrays.copyOf(groupHash, g * 2)
    groupHash(g) = h
    var k = 0
    while k < width do
      stores(k).add(keys(k), i)
      k += 1
    count += 1
    if count * 2 > slots.length then
      rehash(slots.length * 2)
    else
      var idx = h & mask
      while slots(idx) != Empty do
        idx = (idx + 1) & mask
      slots(idx) = entry(h, g)
    g

  /** Re-insert every group (the newest included) into a table of `cap` slots */
  private def rehash(cap: Int): Unit =
    slots = emptySlots(cap)
    mask = cap - 1
    var g = 0
    while g < count do
      val h   = groupHash(g)
      var idx = h & mask
      while slots(idx) != Empty do
        idx = (idx + 1) & mask
      slots(idx) = entry(h, g)
      g += 1

  /**
    * A single Int64 key column, whose rows can take the [[findLong]] path. Without `anyNulls`, only
    * when it has no NULLs: [[findLong]] never matches NULL keys, which inserting must group
    */
  private def longKey(norm: Array[Vec], anyNulls: Boolean = false): Array[Long] =
    if width == 1 && (anyNulls || norm(0).nulls == null) then
      norm(0) match
        case v: LongVec if stores(0).isInstanceOf[LongStore] =>
          v.values
        case _ =>
          null
    else
      null

  /** [[longKey]] for a single Int32 key column, for the [[findInt]] path */
  private def intKey(norm: Array[Vec], anyNulls: Boolean = false): Array[Int] =
    if width == 1 && (anyNulls || norm(0).nulls == null) then
      norm(0) match
        case v: IntVec if stores(0).isInstanceOf[IntStore] =>
          v.values
        case _ =>
          null
    else
      null

  /**
    * The hash of each row's key, as this table computes it: equal keys (NULLs included) hash alike,
    * so the hashes can partition rows by key
    */
  def keyHashes(keys: Array[Vec], n: Int): Array[Int] = hashes(normalize(keys), n)

  def groupIds(keys: Array[Vec], n: Int): Array[Int] =
    val norm = normalize(keys)
    val ids  = dictionaryIds(norm, n, null)
    if ids != null then
      return ids
    if hashBuf == null || hashBuf.length < n then
      hashBuf = new Array[Int](n)
    else
      java.util.Arrays.fill(hashBuf, 0, n, 0)
    assignIds(norm, n, hashesInto(norm, n, hashBuf))

  /**
    * [[groupIds]] with each row's hash already computed by [[keyHashes]] (of a table with the same
    * key types), so the keys are not hashed again
    */
  def groupIds(keys: Array[Vec], n: Int, keyHashes: Array[Int]): Array[Int] =
    val norm = normalize(keys)
    val ids  = dictionaryIds(norm, n, keyHashes)
    if ids != null then
      ids
    else
      assignIds(norm, n, keyHashes)

  // Per batch, the group of each combination of dictionary codes seen so far (-1 when not yet),
  // and each row's combination (-1 when a key has no code), reused across batches
  private var codeGroups: Array[Int] = null
  private var rowCodes: Array[Int]   = null

  /**
    * Group ids through the dictionary codes of the keys ([[StrVec.codes]]): the first row of each
    * code combination finds or inserts its group as [[assignIds]] does, and the other rows with the
    * same combination take that group. Rows with a key without a code (-1) find their group one by
    * one. `h` holds the row hashes, or is null to hash only the rows that look up a group.
    *
    * Null (the caller takes [[assignIds]]) unless every key is a string carrying codes and the
    * combinations number at most half the rows, as DuckDB's dictionary aggregation without a
    * dictionary id: a batch's codes don't identify entries of other batches, so each batch looks
    * its combinations up again
    */
  private def dictionaryIds(norm: Array[Vec], n: Int, h: Array[Int]): Array[Int] =
    if width == 0 then
      return null
    var combos = 1L
    var k      = 0
    while k < width do
      norm(k) match
        case v: StrVec if v.codes != null && stores(k).isInstanceOf[StrStore] =>
          combos *= v.dictSize
        case _ =>
          return null
      if combos * 2 > n then
        return null
      k += 1
    val m = combos.toInt
    if codeGroups == null || codeGroups.length < m then
      codeGroups = new Array[Int](math.max(m, 64))
    java.util.Arrays.fill(codeGroups, 0, m, -1)
    if rowCodes == null || rowCodes.length < n then
      rowCodes = new Array[Int](n)
    val c = rowCodes
    // Mixed-radix combination of the codes, column by column
    java.util.Arrays.fill(c, 0, n, 0)
    k = 0
    while k < width do
      val v     = norm(k).asInstanceOf[StrVec]
      val codes = v.codes
      val size  = v.dictSize
      var i     = 0
      while i < n do
        val x = codes(i)
        if c(i) >= 0 then
          c(i) =
            if x >= 0 then
              c(i) * size + x
            else
              -1
        i += 1
      k += 1
    val ids = new Array[Int](n)
    var i   = 0
    while i < n do
      val x = c(i)
      ids(i) =
        if x < 0 then
          findOrInsert(norm, i, h)
        else
          val g = codeGroups(x)
          if g >= 0 then
            g
          else
            val ng = findOrInsert(norm, i, h)
            codeGroups(x) = ng
            ng
      i += 1
    ids

  end dictionaryIds

  /** The group of row i, inserted when new: hashed here when `h` is null */
  private def findOrInsert(norm: Array[Vec], i: Int, h: Array[Int]): Int =
    val hash =
      if h != null then
        h(i)
      else
        strRowHash(norm, i)
    val g = find(norm, i, hash)
    if g >= 0 then
      g
    else
      insert(norm, i, hash)

  /** The hash of row i of string keys, as [[hashesInto]] computes it with [[StrStore.hashInto]] */
  private def strRowHash(norm: Array[Vec], i: Int): Int =
    var x = 0
    var k = 0
    while k < width do
      val v = norm(k).asInstanceOf[StrVec]
      val s = v.values(i)
      if s != null && !v.isNull(i) then
        x = x * 31 + s.hashCode
      k += 1
    fmix(x)

  private def assignIds(norm: Array[Vec], n: Int, h: Array[Int]): Array[Int] =
    val ids = new Array[Int](n)
    val x   = longKey(norm)
    val xi  = intKey(norm)
    var i   = 0
    if x != null then
      val ls = stores(0).asInstanceOf[LongStore]
      while i < n do
        val g = findLong(ls, x, i, h(i))
        ids(i) =
          if g >= 0 then
            g
          else
            insert(norm, i, h(i))
        i += 1
    else if xi != null then
      val is = stores(0).asInstanceOf[IntStore]
      while i < n do
        val g = findInt(is, xi, i, h(i))
        ids(i) =
          if g >= 0 then
            g
          else
            insert(norm, i, h(i))
        i += 1
    else
      while i < n do
        val g = find(norm, i, h(i))
        ids(i) =
          if g >= 0 then
            g
          else
            insert(norm, i, h(i))
        i += 1
    ids

  end assignIds

  /**
    * Group ids of existing keys without inserting new ones: -1 for keys not in the table and,
    * unless `nullsMatch`, for rows with a NULL key column. With `nullsMatch`, a NULL key finds the
    * group holding NULL there, as in GROUP BY (`IS NOT DISTINCT FROM`)
    */
  def lookup(keys: Array[Vec], n: Int, nullsMatch: Boolean = false): Array[Int] =
    val norm = normalize(keys)
    lookupNormalized(norm, n, hashes(norm, n), nullsMatch)

  /**
    * [[lookup]] with each row's hash already computed by [[keyHashes]] (of a table with the same
    * key types), so the keys are not hashed again
    */
  def lookup(keys: Array[Vec], n: Int, keyHashes: Array[Int], nullsMatch: Boolean): Array[Int] =
    lookupNormalized(normalize(keys), n, keyHashes, nullsMatch)

  private def lookupNormalized(
      norm: Array[Vec],
      n: Int,
      h: Array[Int],
      nullsMatch: Boolean
  ): Array[Int] =
    val ids = new Array[Int](n)
    // Rows with a NULL key are reset to -1 below, so the fast paths may take NULL rows too. When
    // NULLs match, the fast paths (which ignore NULL flags) take only keys without NULLs
    val x  = longKey(norm, anyNulls = !nullsMatch)
    val xi = intKey(norm, anyNulls = !nullsMatch)
    var i  = 0
    if x != null then
      val ls = stores(0).asInstanceOf[LongStore]
      while i < n do
        ids(i) = findLong(ls, x, i, h(i))
        i += 1
    else if xi != null then
      val is = stores(0).asInstanceOf[IntStore]
      while i < n do
        ids(i) = findInt(is, xi, i, h(i))
        i += 1
    else
      while i < n do
        ids(i) = find(norm, i, h(i))
        i += 1
    var k = 0
    while !nullsMatch && k < width do
      val nulls = norm(k).nulls
      if nulls != null then
        i = 0
        while i < n do
          if nulls(i) then
            ids(i) = -1
          i += 1
      k += 1
    ids

  end lookupNormalized

  /** Key values of every group, in group id order */
  def keyVectors(): Array[Vec] = stores.map(_.result(count))

end GroupTable

/** Dense group ids of the distinct keys of streamed batches, in first-seen order */
trait GroupIndex:
  def numGroups: Int

  /**
    * The group id of each of the first n rows of `keys`, adding new keys as groups with the next
    * ids. NULL key values are equal to each other
    */
  def groupIds(keys: Array[Vec], n: Int): Array[Int]

  /** Key values of every group, in group id order */
  def keyVectors(): Array[Vec]

object GroupTable:
  /** An unused slot: no entry has a negative group id */
  private inline val Empty = -1L

  private inline def entry(h: Int, g: Int): Long = (h.toLong << 32) | (g.toLong & 0xffffffffL)

  /**
    * Callers pre-size tables to at most this many groups: group estimates and row-count bounds can
    * be far above the actual groups, and an overestimate costs this much zero-filled memory at most
    */
  inline val MaxPresizeGroups = 1 << 18

  /** Slots for n groups at most half full: a power of two of at least 1024 */
  private def slotCapacity(n: Int): Int =
    val want = math.max(1024L, n.toLong * 2)
    if want >= (1 << 30) then
      1 << 30
    else
      Integer.highestOneBit((want - 1).toInt) << 1

  /** Array.fill would box every element */
  private def emptySlots(n: Int): Array[Long] =
    val a = new Array[Long](n)
    java.util.Arrays.fill(a, Empty)
    a

  /**
    * Positions 0 until n split into `parts` (a power of two) partitions by key hash, each in input
    * order. The partition comes from the top bits of the hash, which a partition's own GroupTable
    * never uses as slot bits (tables index by the low bits), so every partition's table keeps all
    * of its slots usable
    */
  def partitionRows(h: Array[Int], n: Int, parts: Int): Array[Array[Int]] =
    if parts == 1 then
      val all = new Array[Int](n)
      var k   = 0
      while k < n do
        all(k) = k
        k += 1
      return Array(all)
    val shift  = 32 - Integer.numberOfTrailingZeros(parts)
    val counts = new Array[Int](parts)
    var i      = 0
    while i < n do
      counts(h(i) >>> shift) += 1
      i += 1
    // A loop: Array.tabulate would box every partition number
    val out = new Array[Array[Int]](parts)
    var p   = 0
    while p < parts do
      out(p) = new Array[Int](counts(p))
      p += 1
    val pos = new Array[Int](parts)
    i = 0
    while i < n do
      val p = h(i) >>> shift
      out(p)(pos(p)) = i
      pos(p) += 1
      i += 1
    out

  /** A power of two of at least twice the parallelism, so partitions balance across threads */
  def partitionCount(parallelism: Int): Int =
    var parts = 1
    while parts < parallelism * 2 do
      parts *= 2
    parts

  /** Murmur3 finalizer */
  inline def fmix(x: Int): Int =
    var h = x
    h ^= h >>> 16
    h *= 0x85ebca6b
    h ^= h >>> 13
    h *= 0xc2b2ae35
    h ^= h >>> 16
    h

  inline def mixInto(h: Array[Int], i: Int, x: Int): Unit = h(i) = h(i) * 31 + x

  inline def hashLong(x: Long): Int = (x ^ (x >>> 32)).toInt

  /** The stored key values of one key column */
  abstract class KeyStore:
    protected var nulls: Array[Boolean] = null
    protected def capacity: Int
    protected def grow(cap: Int): Unit

    /** The vector in the representation this store reads */
    def normalize(v: Vec): Vec

    def hashInto(v: Vec, n: Int, h: Array[Int]): Unit

    /** Whether row i of v equals the key of group g */
    def equalsAt(g: Int, v: Vec, i: Int): Boolean

    protected def addValue(g: Int, v: Vec, i: Int): Unit
    def result(n: Int): Vec

    private var size = 0

    /** Make room for n keys up front */
    final def reserve(n: Int): Unit =
      if n > capacity then
        grow(n)

    final def add(v: Vec, i: Int): Unit =
      if size == capacity then
        val cap = math.max(16, capacity * 2)
        grow(cap)
        if nulls != null then
          nulls = java.util.Arrays.copyOf(nulls, cap)
      if v.isNull(i) then
        if nulls == null then
          nulls = new Array[Boolean](capacity)
        nulls(size) = true
      else
        addValue(size, v, i)
      size += 1

    protected final inline def nullsEqual(g: Int, v: Vec, i: Int)(
        inline same: => Boolean
    ): Boolean =
      val gn = nulls != null && nulls(g)
      val vn = v.isNull(i)
      if gn || vn then
        gn && vn
      else
        same

    protected final def resultNulls(n: Int): Array[Boolean] =
      if nulls == null then
        null
      else
        java.util.Arrays.copyOf(nulls, n)

  end KeyStore

  object KeyStore:
    def apply(t: PType, longDecimals: Boolean = false): KeyStore =
      t match
        case d: PType.Decimal if longDecimals =>
          DecimalStore(d)
        case PType.Int32 | PType.Date =>
          IntStore(t)
        case PType.Int64 | PType.Timestamp | PType.TimestampTz =>
          LongStore(t)
        case PType.Float64 =>
          DoubleStore()
        case PType.Str =>
          StrStore()
        case PType.Bool =>
          BoolStore()
        case _ =>
          ObjStore(t)

  /** Rebuild a vector in the canonical representation of a type */
  private def canonical(v: Vec, t: PType): Vec =
    val b = VecBuilder(t, v.size)
    var i = 0
    while i < v.size do
      b.add(v.get(i))
      i += 1
    b.result()

  final class IntStore(t: PType) extends KeyStore:
    private var values                 = new Array[Int](16)
    protected def capacity: Int        = values.length
    protected def grow(cap: Int): Unit = values = java.util.Arrays.copyOf(values, cap)
    def normalize(v: Vec): Vec         =
      v match
        case x: IntVec =>
          x
        case other =>
          canonical(other, t)

    def hashInto(v: Vec, n: Int, h: Array[Int]): Unit =
      val x  = v.asInstanceOf[IntVec].values
      val ns = v.nulls
      var i  = 0
      while i < n do
        // NULL slots may hold any value; all NULLs must hash alike
        if ns == null || !ns(i) then
          mixInto(h, i, x(i))
        i += 1

    def equalsAt(g: Int, v: Vec, i: Int): Boolean =
      nullsEqual(g, v, i)(values(g) == v.asInstanceOf[IntVec].values(i))

    inline def valueAt(g: Int): Int      = values(g)
    inline def isNullAt(g: Int): Boolean = nulls != null && nulls(g)

    protected def addValue(g: Int, v: Vec, i: Int): Unit =
      values(g) = v.asInstanceOf[IntVec].values(i)

    def result(n: Int): Vec = IntVec(t, java.util.Arrays.copyOf(values, n), resultNulls(n))

  end IntStore

  final class LongStore(t: PType) extends KeyStore:
    private var values                 = new Array[Long](16)
    protected def capacity: Int        = values.length
    protected def grow(cap: Int): Unit = values = java.util.Arrays.copyOf(values, cap)
    def normalize(v: Vec): Vec         =
      v match
        case x: LongVec if x.ptype == t =>
          x
        case other =>
          canonical(other, t)

    def hashInto(v: Vec, n: Int, h: Array[Int]): Unit =
      val x  = v.asInstanceOf[LongVec].values
      val ns = v.nulls
      var i  = 0
      while i < n do
        if ns == null || !ns(i) then
          mixInto(h, i, hashLong(x(i)))
        i += 1

    def equalsAt(g: Int, v: Vec, i: Int): Boolean =
      nullsEqual(g, v, i)(values(g) == v.asInstanceOf[LongVec].values(i))

    inline def valueAt(g: Int): Long     = values(g)
    inline def isNullAt(g: Int): Boolean = nulls != null && nulls(g)

    protected def addValue(g: Int, v: Vec, i: Int): Unit =
      values(g) = v.asInstanceOf[LongVec].values(i)

    def result(n: Int): Vec = LongVec(t, java.util.Arrays.copyOf(values, n), resultNulls(n))

  end LongStore

  /** Doubles are equal when their bits are (as java.lang.Double.equals) */
  final class DoubleStore extends KeyStore:
    private var values                 = new Array[Double](16)
    protected def capacity: Int        = values.length
    protected def grow(cap: Int): Unit = values = java.util.Arrays.copyOf(values, cap)
    def normalize(v: Vec): Vec         =
      v match
        case x: DoubleVec =>
          x
        case other =>
          canonical(other, PType.Float64)

    def hashInto(v: Vec, n: Int, h: Array[Int]): Unit =
      val x  = v.asInstanceOf[DoubleVec].values
      val ns = v.nulls
      var i  = 0
      while i < n do
        if ns == null || !ns(i) then
          mixInto(h, i, hashLong(java.lang.Double.doubleToLongBits(x(i))))
        i += 1

    def equalsAt(g: Int, v: Vec, i: Int): Boolean =
      nullsEqual(g, v, i)(
        java.lang.Double.doubleToLongBits(values(g)) ==
          java.lang.Double.doubleToLongBits(v.asInstanceOf[DoubleVec].values(i))
      )

    protected def addValue(g: Int, v: Vec, i: Int): Unit =
      values(g) = v.asInstanceOf[DoubleVec].values(i)

    def result(n: Int): Vec = DoubleVec(java.util.Arrays.copyOf(values, n), resultNulls(n))

  end DoubleStore

  final class StrStore extends KeyStore:
    private var values                 = new Array[String](16)
    protected def capacity: Int        = values.length
    protected def grow(cap: Int): Unit = values = java.util.Arrays.copyOf(values, cap)
    def normalize(v: Vec): Vec         =
      v match
        case x: StrVec =>
          x
        case other =>
          canonical(other, PType.Str)

    def hashInto(v: Vec, n: Int, h: Array[Int]): Unit =
      val x  = v.asInstanceOf[StrVec].values
      val ns = v.nulls
      var i  = 0
      while i < n do
        val s = x(i)
        if s != null && (ns == null || !ns(i)) then
          mixInto(h, i, s.hashCode)
        i += 1

    def equalsAt(g: Int, v: Vec, i: Int): Boolean =
      nullsEqual(g, v, i)(values(g) == v.asInstanceOf[StrVec].values(i))

    protected def addValue(g: Int, v: Vec, i: Int): Unit =
      values(g) = v.asInstanceOf[StrVec].values(i)

    def result(n: Int): Vec = StrVec(java.util.Arrays.copyOf(values, n), resultNulls(n))

  final class BoolStore extends KeyStore:
    private var values                 = new Array[Boolean](16)
    protected def capacity: Int        = values.length
    protected def grow(cap: Int): Unit = values = java.util.Arrays.copyOf(values, cap)
    def normalize(v: Vec): Vec         =
      v match
        case x: BoolVec =>
          x
        case other =>
          canonical(other, PType.Bool)

    def hashInto(v: Vec, n: Int, h: Array[Int]): Unit =
      val x  = v.asInstanceOf[BoolVec].values
      val ns = v.nulls
      var i  = 0
      while i < n do
        if ns == null || !ns(i) then
          mixInto(
            h,
            i,
            if x(i) then
              1231
            else
              1237
          )
        i += 1

    def equalsAt(g: Int, v: Vec, i: Int): Boolean =
      nullsEqual(g, v, i)(values(g) == v.asInstanceOf[BoolVec].values(i))

    protected def addValue(g: Int, v: Vec, i: Int): Unit =
      values(g) = v.asInstanceOf[BoolVec].values(i)

    def result(n: Int): Vec = BoolVec(java.util.Arrays.copyOf(values, n), resultNulls(n))

  end BoolStore

  /**
    * Decimals of one type as their unscaled values at its scale: a Long while the value fits, and a
    * BigDecimal otherwise. A value has one representation, so values are equal exactly when their
    * representations are
    */
  final class DecimalStore(t: PType.Decimal) extends KeyStore:
    private var values = new Array[Long](16)
    // The values that don't fit in a Long, or null while none has been added
    private var big: Array[AnyRef]     = null
    protected def capacity: Int        = values.length
    protected def grow(cap: Int): Unit =
      values = java.util.Arrays.copyOf(values, cap)
      if big != null then
        big = java.util.Arrays.copyOf(big, cap)

    def normalize(v: Vec): Vec =
      v match
        case x: LongVec if x.ptype == t =>
          x
        case x: ObjVec if x.ptype == t =>
          x
        case other =>
          canonical(other, t)

    /** Row i of a non-LongVec vector, rescaled to this type's scale */
    private def rescaled(v: Vec, i: Int): java.math.BigDecimal = ValueOps.rescale(
      ValueOps.toBigDecimal(v.get(i)),
      t.scale
    )

    private inline def fitsLong(d: java.math.BigDecimal): Boolean =
      d.unscaledValue().bitLength() < 64

    def hashInto(v: Vec, n: Int, h: Array[Int]): Unit =
      val ns = v.nulls
      var i  = 0
      v match
        case lv: LongVec =>
          val x = lv.values
          while i < n do
            if ns == null || !ns(i) then
              mixInto(h, i, hashLong(x(i)))
            i += 1
        case _ =>
          while i < n do
            if !v.isNull(i) then
              val d = rescaled(v, i)
              mixInto(
                h,
                i,
                if fitsLong(d) then
                  hashLong(d.unscaledValue().longValue())
                else
                  d.hashCode
              )
            i += 1

    def equalsAt(g: Int, v: Vec, i: Int): Boolean =
      nullsEqual(g, v, i) {
        val narrow = big == null || big(g) == null
        v match
          case lv: LongVec =>
            narrow && values(g) == lv.values(i)
          case _ =>
            val d = rescaled(v, i)
            if fitsLong(d) then
              narrow && values(g) == d.unscaledValue().longValue()
            else
              !narrow && d == big(g)
      }

    protected def addValue(g: Int, v: Vec, i: Int): Unit =
      v match
        case lv: LongVec =>
          values(g) = lv.values(i)
        case _ =>
          val d = rescaled(v, i)
          if fitsLong(d) then
            values(g) = d.unscaledValue().longValue()
          else
            if big == null then
              big = new Array[AnyRef](values.length)
            big(g) = d

    def result(n: Int): Vec =
      if big == null then
        LongVec(t, java.util.Arrays.copyOf(values, n), resultNulls(n))
      else
        val b = VecBuilder(t, n)
        var i = 0
        while i < n do
          if nulls != null && nulls(i) then
            b.addNull()
          else if big(i) != null then
            b.add(big(i))
          else
            b.add(java.math.BigDecimal.valueOf(values(i), t.scale))
          i += 1
        b.result()

  end DecimalStore

  /** Any other type, through boxed engine values and their equals / hashCode */
  final class ObjStore(t: PType) extends KeyStore:
    private var values                 = new Array[AnyRef](16)
    protected def capacity: Int        = values.length
    protected def grow(cap: Int): Unit = values = java.util.Arrays.copyOf(values, cap)
    def normalize(v: Vec): Vec         = v
    def hashInto(v: Vec, n: Int, h: Array[Int]): Unit =
      var i = 0
      while i < n do
        val x = v.get(i)
        if x != null then
          mixInto(h, i, x.hashCode)
        i += 1

    def equalsAt(g: Int, v: Vec, i: Int): Boolean =
      nullsEqual(g, v, i)(java.util.Objects.equals(values(g), v.get(i)))

    protected def addValue(g: Int, v: Vec, i: Int): Unit = values(g) = v.get(i).asInstanceOf[AnyRef]
    def result(n: Int): Vec                              =
      val b = VecBuilder(t, n)
      var i = 0
      while i < n do
        if nulls != null && nulls(i) then
          b.addNull()
        else
          b.add(values(i))
        i += 1
      b.result()

end GroupTable

/** An open-addressing Long -> Int hash map */
final class LongIntMap:
  private var keys  = new Array[Long](1024)
  private var vals  = new Array[Int](1024)
  private var used  = new Array[Boolean](1024)
  private var count = 0
  private var mask  = 1023

  def size: Int = count

  private inline def mix(x: Long): Int =
    var h = x * 0x9e3779b97f4a7c15L
    h ^= (h >>> 32)
    h.toInt

  def getOrElse(k: Long, default: Int): Int =
    var i = mix(k) & mask
    while used(i) do
      if keys(i) == k then
        return vals(i)
      i = (i + 1) & mask
    default

  def put(k: Long, v: Int): Unit =
    if (count + 1) * 2 > keys.length then
      rehash()
    var i = mix(k) & mask
    while used(i) do
      if keys(i) == k then
        vals(i) = v
        return
      i = (i + 1) & mask
    used(i) = true
    keys(i) = k
    vals(i) = v
    count += 1

  private def rehash(): Unit =
    val oldKeys = keys
    val oldVals = vals
    val oldUsed = used
    val cap     = keys.length * 2
    keys = new Array[Long](cap)
    vals = new Array[Int](cap)
    used = new Array[Boolean](cap)
    mask = cap - 1
    count = 0
    var i = 0
    while i < oldKeys.length do
      if oldUsed(i) then
        put(oldKeys(i), oldVals(i))
      i += 1

end LongIntMap
