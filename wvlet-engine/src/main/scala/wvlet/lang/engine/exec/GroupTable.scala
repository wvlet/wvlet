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
  * Assigns dense group ids to distinct keys and keeps one copy of each key's values
  */
final class GroupTable(keyTypes: IndexedSeq[PType]):
  private val map         = java.util.HashMap[AnyRef, Integer]()
  private val keyBuilders = keyTypes.map(t => VecBuilder(t, 64)).toArray
  private var count       = 0
  // Single integer keys take a specialized path without boxing composite keys
  private val singleLong: Boolean =
    keyTypes.size == 1 &&
      (keyTypes.head == PType.Int64 || keyTypes.head == PType.Int32 || keyTypes.head == PType.Date)

  private val longMap   = LongIntMap()
  private var nullGroup = -1

  def numGroups: Int = count

  def groupIds(keys: Array[Vec], n: Int): Array[Int] =
    val ids = new Array[Int](n)
    if singleLong then
      val v                  = keys(0)
      val longs: Array[Long] =
        v match
          case l: LongVec =>
            l.values
          case iv: IntVec =>
            null
          case _ =>
            null
      var i = 0
      while i < n do
        if v.isNull(i) then
          if nullGroup < 0 then
            nullGroup = count
            keyBuilders(0).addNull()
            count += 1
          ids(i) = nullGroup
        else
          val x: Long =
            if longs != null then
              longs(i)
            else
              v.asInstanceOf[IntVec].values(i).toLong
          val g = longMap.getOrElse(x, -1)
          if g >= 0 then
            ids(i) = g
          else
            longMap.put(x, count)
            keyBuilders(0).add(v.get(i))
            ids(i) = count
            count += 1
        i += 1
    else
      var i = 0
      while i < n do
        val key = GroupKey.of(keys, i)
        val g   = map.get(key)
        if g != null then
          ids(i) = g.intValue()
        else
          map.put(key, count)
          var k = 0
          while k < keys.length do
            keyBuilders(k).add(keys(k).get(i))
            k += 1
          ids(i) = count
          count += 1
        i += 1
    end if
    ids

  end groupIds

  /**
    * Group ids of existing keys without inserting new ones: -1 for keys not in the table and for
    * rows with a NULL key column
    */
  def lookup(keys: Array[Vec], n: Int): Array[Int] =
    val ids = new Array[Int](n)
    var i   = 0
    while i < n do
      if GroupKey.hasNull(keys, i) then
        ids(i) = -1
      else if singleLong then
        val v       = keys(0)
        val x: Long =
          v match
            case l: LongVec =>
              l.values(i)
            case iv: IntVec =>
              iv.values(i).toLong
            case other =>
              ValueOps.toLong(other.get(i))
        ids(i) = longMap.getOrElse(x, -1)
      else
        val g = map.get(GroupKey.of(keys, i))
        ids(i) =
          if g == null then
            -1
          else
            g.intValue()
      i += 1
    ids

  /** Key values of every group, in group id order */
  def keyVectors(): Array[Vec] = keyBuilders.map(_.result())

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
