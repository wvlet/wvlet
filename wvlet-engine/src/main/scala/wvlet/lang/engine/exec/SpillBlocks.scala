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

import java.math.BigDecimal as JBigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer

/**
  * The block format of spill files: one batch as bytes, with every column as the engine holds it.
  * Per column a tag, a null flag (and one byte per row when it has nulls), then its values:
  * primitives as arrays, strings as UTF-16 chars with a length per row (-1 for null; UTF-16 keeps
  * any String exactly), strings held as batch entries as the entries and the codes, and BigDecimal
  * decimals as scale and unscaled bytes. The column types come from the schema on read. Lists,
  * maps, structs, and intervals (`AnyT`) are not spillable ([[spillable]]).
  */
object SpillBlocks:
  private final val TBool    = 0
  private final val TInt     = 1
  private final val TLong    = 2
  private final val TDouble  = 3
  private final val TStr     = 4
  private final val TEntries = 5
  private final val TBigDec  = 6
  private final val TNull    = 7

  /** Whether batches of `schema` can be written as blocks */
  def spillable(schema: Schema): Boolean = schema.types.forall(_ != PType.AnyT)

  /** The bytes of `b`'s rows (its deferred columns gathered) */
  def encode(b: Batch): Array[Byte] =
    val cols  = b.columns.map(dedup)
    var bytes = 8L
    var i     = 0
    while i < cols.length do
      bytes += columnBytes(cols(i))
      i += 1
    require(bytes <= Int.MaxValue, s"A spill block of ${bytes} bytes is too large")
    val buf = ByteBuffer.allocate(bytes.toInt)
    buf.putInt(b.size)
    buf.putInt(cols.length)
    cols.foreach(putColumn(buf, _))
    buf.array()

  /** The batch of `schema` written as `bytes` by [[encode]] */
  def decode(schema: Schema, bytes: Array[Byte]): Batch =
    val buf = ByteBuffer.wrap(bytes)
    val n   = buf.getInt()
    val nc  = buf.getInt()
    require(nc == schema.size, s"A spill block of ${nc} columns read as ${schema.size}")
    val cols = Array.tabulate[Vec](nc)(c => getColumn(buf, schema(c).ptype, n))
    Batch(cols, n)

  /**
    * A plain string vector whose values repeat (at most half distinct) as entries and codes, so the
    * block holds each string once and reads back coded, for grouping by codes
    */
  private def dedup(v: Vec): Vec =
    v match
      case s: StrVec if s.holdsEntries =>
        // Only the entries the rows use: a gathered piece keeps its batch's whole dictionary
        val e     = entriesOf(s)
        val n     = s.size
        val codes = s.codes
        val remap = Array.fill(e.length)(-1)
        val used  = scala.collection.mutable.ArrayBuffer.empty[String]
        val out   = new Array[Int](n)
        var i     = 0
        while i < n do
          val c = codes(i)
          if c < 0 then
            out(i) = -1
          else
            if remap(c) < 0 then
              remap(c) = used.size
              used += e(c)
            out(i) = remap(c)
          i += 1
        if used.size == e.length then
          v
        else
          StrVec.ofEntries(used.toArray, out, s.nulls, n)
      case s: StrVec if s.size >= 64 =>
        val n      = s.size
        val values = s.values
        val ids    = java.util.HashMap[String, Integer]()
        val codes  = new Array[Int](n)
        val limit  = n / 2
        var i      = 0
        while i < n do
          // Mostly distinct strings (comments, names): stop after a sample
          if i == 1024 && ids.size > 512 then
            return v
          val x = values(i)
          if x == null then
            codes(i) = -1
          else
            var c = ids.get(x)
            if c == null then
              if ids.size >= limit then
                return v
              c = ids.size
              ids.put(x, c)
            codes(i) = c
          i += 1
        val entries = new Array[String](ids.size)
        ids.forEach((x, c) => entries(c) = x)
        StrVec.ofEntries(entries, codes, s.nulls, n)
      case _ =>
        v

  private def columnBytes(v: Vec): Long =
    val n     = v.size.toLong
    val nulls =
      if v.nulls != null then
        n
      else
        0L
    2L + nulls + (
      v match
        case _: BoolVec =>
          n
        case _: IntVec =>
          4L * n
        case _: LongVec =>
          8L * n
        case _: DoubleVec =>
          8L * n
        case s: StrVec if s.holdsEntries =>
          val e = entriesOf(s)
          4L + strBytes(e, e.length) + 4L * n
        case s: StrVec =>
          strBytes(s.values, s.size)
        case o: ObjVec if o.ptype == PType.Null =>
          0L
        case o: ObjVec =>
          var total = 0L
          var i     = 0
          while i < o.size do
            total += 8L
            o.values(i) match
              case d: JBigDecimal =>
                total += d.unscaledValue().bitLength() / 8 + 1
              case null =>
              case x    =>
                unsupported(o.ptype, x)
            i += 1
          total
    )

  end columnBytes

  private def unsupported(t: PType, x: Any): Nothing =
    throw IllegalArgumentException(s"Cannot spill a ${t} value of ${x.getClass.getName}")

  private def strBytes(values: Array[String], n: Int): Long =
    var total = 4L * n
    var i     = 0
    while i < n do
      val s = values(i)
      if s != null then
        total += 2L * s.length
      i += 1
    total

  // The entries of a vector held as entries, read through rows' codes
  private def entriesOf(s: StrVec): Array[String] =
    val e = s.entryStrings
    if e != null then
      e
    else
      Array.empty

  private def putColumn(buf: ByteBuffer, v: Vec): Unit =
    val tag =
      v match
        case _: BoolVec =>
          TBool
        case _: IntVec =>
          TInt
        case _: LongVec =>
          TLong
        case _: DoubleVec =>
          TDouble
        case s: StrVec if s.holdsEntries =>
          TEntries
        case _: StrVec =>
          TStr
        case o: ObjVec if o.ptype == PType.Null =>
          TNull
        case _: ObjVec =>
          TBigDec
    buf.put(tag.toByte)
    val nulls = v.nulls
    if nulls == null then
      buf.put(0.toByte)
    else
      buf.put(1.toByte)
      var i = 0
      while i < v.size do
        buf.put(
          if nulls(i) then
            1.toByte
          else
            0.toByte
        )
        i += 1
    v match
      case b: BoolVec =>
        var i = 0
        while i < b.size do
          buf.put(
            if b.values(i) then
              1.toByte
            else
              0.toByte
          )
          i += 1
      case x: IntVec =>
        buf.asIntBuffer().put(x.values, 0, x.size)
        skip(buf, 4L * x.size)
      case x: LongVec =>
        buf.asLongBuffer().put(x.values, 0, x.size)
        skip(buf, 8L * x.size)
      case x: DoubleVec =>
        buf.asDoubleBuffer().put(x.values, 0, x.size)
        skip(buf, 8L * x.size)
      case s: StrVec if s.holdsEntries =>
        val e = entriesOf(s)
        buf.putInt(e.length)
        putStrings(buf, e, e.length)
        buf.asIntBuffer().put(s.codes, 0, s.size)
        skip(buf, 4L * s.size)
      case s: StrVec =>
        putStrings(buf, s.values, s.size)
      case o: ObjVec if o.ptype == PType.Null =>
      case o: ObjVec                          =>
        var i = 0
        while i < o.size do
          o.values(i) match
            case d: JBigDecimal =>
              val u = d.unscaledValue().toByteArray
              buf.putInt(d.scale())
              buf.putInt(u.length)
              buf.put(u)
            case null =>
              buf.putInt(0)
              buf.putInt(-1)
            case x =>
              unsupported(o.ptype, x)
          i += 1
    end match

  end putColumn

  private def skip(buf: ByteBuffer, bytes: Long): Unit = buf.position(buf.position() + bytes.toInt)

  private def putStrings(buf: ByteBuffer, values: Array[String], n: Int): Unit =
    val lens = buf.asIntBuffer()
    var i    = 0
    while i < n do
      val s = values(i)
      lens.put(
        if s == null then
          -1
        else
          s.length
      )
      i += 1
    skip(buf, 4L * n)
    val chars = buf.asCharBuffer()
    var total = 0L
    i = 0
    while i < n do
      val s = values(i)
      if s != null && s.length > 0 then
        chars.put(s)
        total += s.length
      i += 1
    skip(buf, 2L * total)

  private def getStrings(buf: ByteBuffer, n: Int): Array[String] =
    val lens = new Array[Int](n)
    buf.asIntBuffer().get(lens)
    skip(buf, 4L * n)
    var total = 0L
    var i     = 0
    while i < n do
      if lens(i) > 0 then
        total += lens(i)
      i += 1
    val chars = new Array[Char](total.toInt)
    buf.asCharBuffer().get(chars)
    skip(buf, 2L * total)
    val out = new Array[String](n)
    var off = 0
    i = 0
    while i < n do
      val len = lens(i)
      if len >= 0 then
        out(i) = String(chars, off, len)
        off += len
      i += 1
    out

  private def getColumn(buf: ByteBuffer, t: PType, n: Int): Vec =
    val tag   = buf.get().toInt
    val nulls =
      if buf.get() == 0 then
        null
      else
        val a = new Array[Boolean](n)
        var i = 0
        while i < n do
          a(i) = buf.get() != 0
          i += 1
        a
    tag match
      case TBool =>
        val a = new Array[Boolean](n)
        var i = 0
        while i < n do
          a(i) = buf.get() != 0
          i += 1
        BoolVec(a, nulls)
      case TInt =>
        val a = new Array[Int](n)
        buf.asIntBuffer().get(a)
        skip(buf, 4L * n)
        IntVec(t, a, nulls)
      case TLong =>
        val a = new Array[Long](n)
        buf.asLongBuffer().get(a)
        skip(buf, 8L * n)
        LongVec(t, a, nulls)
      case TDouble =>
        val a = new Array[Double](n)
        buf.asDoubleBuffer().get(a)
        skip(buf, 8L * n)
        DoubleVec(a, nulls)
      case TStr =>
        StrVec(getStrings(buf, n), nulls)
      case TEntries =>
        val e     = getStrings(buf, buf.getInt())
        val codes = new Array[Int](n)
        buf.asIntBuffer().get(codes)
        skip(buf, 4L * n)
        StrVec.ofEntries(e, codes, nulls, n)
      case TNull =>
        Vec.nullVec(t, n)
      case TBigDec =>
        val a = new Array[AnyRef](n)
        var i = 0
        while i < n do
          val scale = buf.getInt()
          val len   = buf.getInt()
          if len >= 0 then
            val u = new Array[Byte](len)
            buf.get(u)
            a(i) = JBigDecimal(BigInteger(u), scale)
          i += 1
        ObjVec(t, a, nulls)
      case other =>
        throw IllegalStateException(s"Unknown spill column tag ${other}")
    end match

  end getColumn

end SpillBlocks
