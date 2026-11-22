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
  */
final class GroupTable(keyTypes: IndexedSeq[PType]):
  import GroupTable.*

  private val stores: Array[KeyStore] = keyTypes.map(KeyStore.apply).toArray
  private val width                   = stores.length
  private var count                   = 0
  private var groupHash               = new Array[Int](64)
  private var slots                   = Array.fill(1024)(-1)
  private var mask                    = 1023

  def numGroups: Int = count

  private def normalize(keys: Array[Vec]): Array[Vec] =
    val out = new Array[Vec](width)
    var k   = 0
    while k < width do
      out(k) = stores(k).normalize(keys(k))
      k += 1
    out

  private def hashes(keys: Array[Vec], n: Int): Array[Int] =
    val h = new Array[Int](n)
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
      val g = slots(idx)
      if g < 0 then
        return -1
      if groupHash(g) == h && sameKey(g, keys, i) then
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
      while slots(idx) >= 0 do
        idx = (idx + 1) & mask
      slots(idx) = g
    g

  private def rehash(cap: Int): Unit =
    slots = Array.fill(cap)(-1)
    mask = cap - 1
    var g = 0
    while g < count do
      var idx = groupHash(g) & mask
      while slots(idx) >= 0 do
        idx = (idx + 1) & mask
      slots(idx) = g
      g += 1

  def groupIds(keys: Array[Vec], n: Int): Array[Int] =
    val norm = normalize(keys)
    val h    = hashes(norm, n)
    val ids  = new Array[Int](n)
    var i    = 0
    while i < n do
      val g = find(norm, i, h(i))
      ids(i) =
        if g >= 0 then
          g
        else
          insert(norm, i, h(i))
      i += 1
    ids

  /**
    * Group ids of existing keys without inserting new ones: -1 for keys not in the table and for
    * rows with a NULL key column
    */
  def lookup(keys: Array[Vec], n: Int): Array[Int] =
    val norm = normalize(keys)
    val h    = hashes(norm, n)
    val ids  = new Array[Int](n)
    var i    = 0
    while i < n do
      ids(i) = find(norm, i, h(i))
      i += 1
    var k = 0
    while k < width do
      val nulls = norm(k).nulls
      if nulls != null then
        i = 0
        while i < n do
          if nulls(i) then
            ids(i) = -1
          i += 1
      k += 1
    ids

  /** Key values of every group, in group id order */
  def keyVectors(): Array[Vec] = stores.map(_.result(count))

end GroupTable

object GroupTable:
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
    def apply(t: PType): KeyStore =
      t match
        case PType.Int32 | PType.Date =>
          IntStore(t)
        case PType.Int64 | PType.Timestamp =>
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

    protected def addValue(g: Int, v: Vec, i: Int): Unit =
      values(g) = v.asInstanceOf[IntVec].values(i)

    def result(n: Int): Vec = IntVec(t, java.util.Arrays.copyOf(values, n), resultNulls(n))

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

    protected def addValue(g: Int, v: Vec, i: Int): Unit =
      values(g) = v.asInstanceOf[LongVec].values(i)

    def result(n: Int): Vec = LongVec(t, java.util.Arrays.copyOf(values, n), resultNulls(n))

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
