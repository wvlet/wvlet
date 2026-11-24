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
package wvlet.lang.engine.vector

import java.math.BigDecimal as JBigDecimal

/**
  * An immutable column of values of one physical type. `nulls` is null when no value is null;
  * otherwise `nulls(i)` is true for NULL rows (the value slot of a NULL row is unspecified).
  *
  * `get(i)` returns the engine value of row i (boxed): Boolean, Int (int32, date), Long (int64,
  * timestamp), Double, java.math.BigDecimal (decimal), String, or any object for AnyT, and null for
  * NULL.
  */
sealed abstract class Vec:
  def ptype: PType
  def size: Int
  def nulls: Array[Boolean]

  final def isNull(i: Int): Boolean = nulls != null && nulls(i)
  final def hasNulls: Boolean       = nulls != null

  def get(i: Int): Any

  /** Rows at the given positions, in order */
  def gather(idx: Array[Int], n: Int): Vec

  /** Rows whose mask is true */
  def filter(mask: Array[Boolean], count: Int): Vec =
    val idx = new Array[Int](count)
    var j   = 0
    var i   = 0
    while i < size do
      if mask(i) then
        idx(j) = i
        j += 1
      i += 1
    gather(idx, count)

  def slice(from: Int, until: Int): Vec =
    val n   = math.max(0, until - from)
    val idx = new Array[Int](n)
    var i   = 0
    while i < n do
      idx(i) = from + i
      i += 1
    gather(idx, n)

  protected def gatherNulls(idx: Array[Int], n: Int): Array[Boolean] =
    if nulls == null then
      null
    else
      val out = new Array[Boolean](n)
      var any = false
      var i   = 0
      while i < n do
        val k = idx(i)
        if k < 0 || nulls(k) then
          out(i) = true
          any = true
        i += 1
      if any then
        out
      else
        null

end Vec

object Vec:
  // Element-wise maps over primitive arrays. Array.map and Array.fill go through generic array
  // updates that box every element; these inline into plain loops

  inline def mapIntToLong(a: Array[Int])(inline f: Int => Long): Array[Long] =
    val out = new Array[Long](a.length)
    var i   = 0
    while i < a.length do
      out(i) = f(a(i))
      i += 1
    out

  inline def mapInt(a: Array[Int])(inline f: Int => Int): Array[Int] =
    val out = new Array[Int](a.length)
    var i   = 0
    while i < a.length do
      out(i) = f(a(i))
      i += 1
    out

  inline def mapLong(a: Array[Long])(inline f: Long => Long): Array[Long] =
    val out = new Array[Long](a.length)
    var i   = 0
    while i < a.length do
      out(i) = f(a(i))
      i += 1
    out

  inline def mapFloatToDouble(a: Array[Float]): Array[Double] =
    val out = new Array[Double](a.length)
    var i   = 0
    while i < a.length do
      out(i) = a(i).toDouble
      i += 1
    out

  inline def mapDouble(a: Array[Double])(inline f: Double => Double): Array[Double] =
    val out = new Array[Double](a.length)
    var i   = 0
    while i < a.length do
      out(i) = f(a(i))
      i += 1
    out

  def filledInts(n: Int, v: Int): Array[Int] =
    val a = new Array[Int](n)
    java.util.Arrays.fill(a, v)
    a

  def allTrue(n: Int): Array[Boolean] =
    val a = new Array[Boolean](n)
    java.util.Arrays.fill(a, true)
    a

  /**
    * Nulls of gathered rows: positions may be -1 to produce NULL (outer join padding). Bind the
    * result to a local before passing it to a constructor: a loop inlined into constructor
    * arguments runs with a non-empty operand stack, which HotSpot cannot enter by on-stack
    * replacement, so after a deoptimization a long gather stays in the interpreter
    */
  private[vector] inline def nullsWithPadding(
      nulls: Array[Boolean],
      idx: Array[Int],
      n: Int
  ): Array[Boolean] =
    var out: Array[Boolean] = null
    var i                   = 0
    while i < n do
      val k = idx(i)
      if k < 0 || (nulls != null && nulls(k)) then
        if out == null then
          out = new Array[Boolean](n)
        out(i) = true
      i += 1
    out

  def nullVec(ptype: PType, n: Int): Vec =
    val nulls = allTrue(n)
    ptype match
      case PType.Bool =>
        BoolVec(new Array[Boolean](n), nulls)
      case PType.Int32 | PType.Date =>
        IntVec(ptype, new Array[Int](n), nulls)
      case PType.Int64 | PType.Timestamp | PType.TimestampTz =>
        LongVec(ptype, new Array[Long](n), nulls)
      case d: PType.Decimal =>
        LongVec(d, new Array[Long](n), nulls)
      case PType.Float64 =>
        DoubleVec(new Array[Double](n), nulls)
      case PType.Str =>
        StrVec(new Array[String](n), nulls)
      case _ =>
        ObjVec(ptype, new Array[AnyRef](n), nulls)

  /** A vector repeating one engine value */
  def constant(ptype: PType, value: Any, n: Int): Vec =
    if value == null then
      nullVec(ptype, n)
    else
      ptype match
        // The value is converted once and filled without boxing
        case PType.Bool =>
          val a = new Array[Boolean](n)
          java.util.Arrays.fill(a, ValueOps.toBoolean(value))
          BoolVec(a, null)
        case PType.Int32 | PType.Date =>
          IntVec(ptype, filledInts(n, ValueOps.toInt(value)), null)
        case PType.Int64 | PType.Timestamp | PType.TimestampTz =>
          val a = new Array[Long](n)
          java.util.Arrays.fill(a, ValueOps.toLong(value))
          LongVec(ptype, a, null)
        case PType.Float64 =>
          val a = new Array[Double](n)
          java.util.Arrays.fill(a, ValueOps.toDouble(value))
          DoubleVec(a, null)
        case PType.Str =>
          val a = new Array[String](n)
          java.util.Arrays.fill(a.asInstanceOf[Array[AnyRef]], ValueOps.toStr(value))
          StrVec(a, null)
        case d: PType.Decimal =>
          val r = ValueOps.rescale(ValueOps.toBigDecimal(value), d.scale)
          val u = r.unscaledValue()
          // Same storage as DecimalBuilder: Long-backed when the unscaled value fits
          if u.bitLength() < 64 then
            val a = new Array[Long](n)
            java.util.Arrays.fill(a, u.longValue())
            LongVec(d, a, null)
          else
            val a = new Array[AnyRef](n)
            java.util.Arrays.fill(a, r)
            ObjVec(d, a, null)
        case _ =>
          val b = VecBuilder(ptype, n)
          var i = 0
          while i < n do
            b.add(value)
            i += 1
          b.result()

  def fromValues(ptype: PType, values: Seq[Any]): Vec =
    val b = VecBuilder(ptype, values.size)
    values.foreach(b.add)
    b.result()

end Vec

final class BoolVec(val values: Array[Boolean], val nulls: Array[Boolean]) extends Vec:
  def ptype: PType     = PType.Bool
  def size: Int        = values.length
  def get(i: Int): Any =
    if isNull(i) then
      null
    else
      values(i)

  def gather(idx: Array[Int], n: Int): Vec =
    val out = new Array[Boolean](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = values(k)
      i += 1
    val outNulls = Vec.nullsWithPadding(nulls, idx, n)
    BoolVec(out, outNulls)

final class IntVec(val ptype: PType, val values: Array[Int], val nulls: Array[Boolean]) extends Vec:
  def size: Int        = values.length
  def get(i: Int): Any =
    if isNull(i) then
      null
    else
      values(i)

  def gather(idx: Array[Int], n: Int): Vec =
    val out = new Array[Int](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = values(k)
      i += 1
    val outNulls = Vec.nullsWithPadding(nulls, idx, n)
    IntVec(ptype, out, outNulls)

/** int64, timestamp (micros), and Long-backed decimal (unscaled) values */
final class LongVec(val ptype: PType, val values: Array[Long], val nulls: Array[Boolean])
    extends Vec:
  def size: Int        = values.length
  def get(i: Int): Any =
    if isNull(i) then
      null
    else
      ptype match
        case PType.Decimal(_, s) =>
          JBigDecimal.valueOf(values(i), s)
        case _ =>
          values(i)

  def gather(idx: Array[Int], n: Int): Vec =
    val out = new Array[Long](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = values(k)
      i += 1
    val outNulls = Vec.nullsWithPadding(nulls, idx, n)
    LongVec(ptype, out, outNulls)

final class DoubleVec(val values: Array[Double], val nulls: Array[Boolean]) extends Vec:
  def ptype: PType     = PType.Float64
  def size: Int        = values.length
  def get(i: Int): Any =
    if isNull(i) then
      null
    else
      values(i)

  def gather(idx: Array[Int], n: Int): Vec =
    val out = new Array[Double](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = values(k)
      i += 1
    val outNulls = Vec.nullsWithPadding(nulls, idx, n)
    DoubleVec(out, outNulls)

final class StrVec(val values: Array[String], val nulls: Array[Boolean]) extends Vec:
  def ptype: PType     = PType.Str
  def size: Int        = values.length
  def get(i: Int): Any =
    if isNull(i) then
      null
    else
      values(i)

  def gather(idx: Array[Int], n: Int): Vec =
    val out = new Array[String](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = values(k)
      i += 1
    val outNulls = Vec.nullsWithPadding(nulls, idx, n)
    StrVec(out, outNulls)

/** Boxed values: wide decimals (java.math.BigDecimal), and nested or untyped values */
final class ObjVec(val ptype: PType, val values: Array[AnyRef], val nulls: Array[Boolean])
    extends Vec:
  def size: Int        = values.length
  def get(i: Int): Any =
    if isNull(i) then
      null
    else
      values(i)

  def gather(idx: Array[Int], n: Int): Vec =
    val out = new Array[AnyRef](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = values(k)
      i += 1
    val outNulls = Vec.nullsWithPadding(nulls, idx, n)
    ObjVec(ptype, out, outNulls)

/**
  * Appends engine values of one type. Decimal builders start Long-backed and switch to BigDecimal
  * storage on the first value that does not fit.
  */
abstract class VecBuilder:
  def ptype: PType
  def add(v: Any): Unit
  def addNull(): Unit
  def size: Int
  def result(): Vec

object VecBuilder:
  def apply(ptype: PType, capacity: Int = 16): VecBuilder =
    ptype match
      case PType.Bool =>
        BoolBuilder(capacity)
      case PType.Int32 | PType.Date =>
        IntBuilder(ptype, capacity)
      case PType.Int64 | PType.Timestamp | PType.TimestampTz =>
        LongBuilder(ptype, capacity)
      case d: PType.Decimal =>
        DecimalBuilder(d, capacity)
      case PType.Float64 =>
        DoubleBuilder(capacity)
      case PType.Str =>
        StrBuilder(capacity)
      case _ =>
        ObjBuilder(ptype, capacity)

  abstract class Base(capacity0: Int) extends VecBuilder:
    protected var n                     = 0
    protected var nulls: Array[Boolean] = null
    protected def capacity: Int
    protected def grow(newCap: Int): Unit
    def size: Int = n

    protected final def ensure(): Unit =
      if n >= capacity then
        val newCap = math.max(16, capacity * 2)
        grow(newCap)
        if nulls != null then
          nulls = java.util.Arrays.copyOf(nulls, newCap)

    def addNull(): Unit =
      ensure()
      if nulls == null then
        nulls = new Array[Boolean](capacity)
      nulls(n) = true
      n += 1

    protected final def resultNulls: Array[Boolean] =
      if nulls == null then
        null
      else
        java.util.Arrays.copyOf(nulls, n)

  final class BoolBuilder(cap: Int) extends Base(cap):
    private var arr                       = new Array[Boolean](math.max(cap, 1))
    def ptype: PType                      = PType.Bool
    protected def capacity: Int           = arr.length
    protected def grow(newCap: Int): Unit = arr = java.util.Arrays.copyOf(arr, newCap)
    def add(v: Any): Unit                 =
      if v == null then
        addNull()
      else
        ensure()
        arr(n) = ValueOps.toBoolean(v)
        n += 1

    def result(): Vec = BoolVec(java.util.Arrays.copyOf(arr, n), resultNulls)

  final class IntBuilder(val ptype: PType, cap: Int) extends Base(cap):
    private var arr                       = new Array[Int](math.max(cap, 1))
    protected def capacity: Int           = arr.length
    protected def grow(newCap: Int): Unit = arr = java.util.Arrays.copyOf(arr, newCap)
    def addInt(v: Int): Unit              =
      ensure()
      arr(n) = v
      n += 1

    def add(v: Any): Unit =
      if v == null then
        addNull()
      else
        addInt(ValueOps.toInt(v))

    def result(): Vec = IntVec(ptype, java.util.Arrays.copyOf(arr, n), resultNulls)

  final class LongBuilder(val ptype: PType, cap: Int) extends Base(cap):
    private var arr                       = new Array[Long](math.max(cap, 1))
    protected def capacity: Int           = arr.length
    protected def grow(newCap: Int): Unit = arr = java.util.Arrays.copyOf(arr, newCap)
    def addLong(v: Long): Unit            =
      ensure()
      arr(n) = v
      n += 1

    def add(v: Any): Unit =
      if v == null then
        addNull()
      else
        addLong(ValueOps.toLong(v))

    def result(): Vec = LongVec(ptype, java.util.Arrays.copyOf(arr, n), resultNulls)

  final class DoubleBuilder(cap: Int) extends Base(cap):
    private var arr                       = new Array[Double](math.max(cap, 1))
    def ptype: PType                      = PType.Float64
    protected def capacity: Int           = arr.length
    protected def grow(newCap: Int): Unit = arr = java.util.Arrays.copyOf(arr, newCap)
    def addDouble(v: Double): Unit        =
      ensure()
      arr(n) = v
      n += 1

    def add(v: Any): Unit =
      if v == null then
        addNull()
      else
        addDouble(ValueOps.toDouble(v))

    def result(): Vec = DoubleVec(java.util.Arrays.copyOf(arr, n), resultNulls)

  final class StrBuilder(cap: Int) extends Base(cap):
    private var arr                       = new Array[String](math.max(cap, 1))
    def ptype: PType                      = PType.Str
    protected def capacity: Int           = arr.length
    protected def grow(newCap: Int): Unit = arr = java.util.Arrays.copyOf(arr, newCap)
    def addString(v: String): Unit        =
      if v == null then
        addNull()
      else
        ensure()
        arr(n) = v
        n += 1

    def add(v: Any): Unit =
      if v == null then
        addNull()
      else
        addString(ValueOps.toStr(v))

    def result(): Vec = StrVec(java.util.Arrays.copyOf(arr, n), resultNulls)

  final class ObjBuilder(val ptype: PType, cap: Int) extends Base(cap):
    private var arr                       = new Array[AnyRef](math.max(cap, 1))
    protected def capacity: Int           = arr.length
    protected def grow(newCap: Int): Unit = arr = java.util.Arrays.copyOf(arr, newCap)
    def add(v: Any): Unit                 =
      if v == null then
        addNull()
      else
        ensure()
        arr(n) = v.asInstanceOf[AnyRef]
        n += 1

    def result(): Vec = ObjVec(ptype, java.util.Arrays.copyOf(arr, n), resultNulls)

  /** Decimal values are rescaled to the builder's scale */
  final class DecimalBuilder(val ptype: PType.Decimal, cap: Int) extends Base(cap):
    private var longs: Array[Long] = new Array[Long](math.max(cap, 1))
    private var big: Array[AnyRef] = null
    protected def capacity: Int    =
      if big != null then
        big.length
      else
        longs.length

    protected def grow(newCap: Int): Unit =
      if big != null then
        big = java.util.Arrays.copyOf(big, newCap)
      else
        longs = java.util.Arrays.copyOf(longs, newCap)

    private def switchToBig(): Unit =
      val b = new Array[AnyRef](longs.length)
      var i = 0
      while i < n do
        b(i) = JBigDecimal.valueOf(longs(i), ptype.scale)
        i += 1
      big = b
      longs = null

    def addUnscaled(v: Long): Unit =
      ensure()
      if big != null then
        big(n) = JBigDecimal.valueOf(v, ptype.scale)
      else
        longs(n) = v
      n += 1

    def addBig(v: JBigDecimal): Unit =
      val d = ValueOps.rescale(v, ptype.scale)
      ensure()
      if big == null then
        if d.unscaledValue().bitLength() < 64 then
          longs(n) = d.unscaledValue().longValue()
          n += 1
          return
        switchToBig()
      big(n) = d
      n += 1

    def add(v: Any): Unit =
      if v == null then
        addNull()
      else
        addBig(ValueOps.toBigDecimal(v))

    def result(): Vec =
      if big != null then
        ObjVec(ptype, java.util.Arrays.copyOf(big, n), resultNulls)
      else
        LongVec(ptype, java.util.Arrays.copyOf(longs, n), resultNulls)

  end DecimalBuilder

end VecBuilder
