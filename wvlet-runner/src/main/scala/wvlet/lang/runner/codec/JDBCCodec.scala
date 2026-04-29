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
package wvlet.lang.runner.codec

import wvlet.uni.log.LogSupport
import wvlet.uni.msgpack.spi.MessagePack
import wvlet.uni.msgpack.spi.Packer
import wvlet.uni.weaver.codec.JSONWeaver

import java.sql.ResultSet
import java.sql.Types

/**
  * JDBC ResultSet → MsgPack/JSON codec, ported from wvlet.airframe.codec.JDBCCodec so wvlet can
  * adjust per-database type quirks (DuckDB, Trino, Snowflake, ...) independently of uni.
  */
object JDBCCodec extends LogSupport:
  def apply(rs: ResultSet): ResultSetCodec = ResultSetCodec(rs)

  class ResultSetCodec(rs: ResultSet):
    private lazy val md                                        = rs.getMetaData
    private lazy val columnCount                               = md.getColumnCount
    private lazy val columnCodecs: IndexedSeq[JDBCColumnCodec] = (1 to columnCount)
      .map(i => toJDBCColumnCodec(md.getColumnType(i), md.getColumnTypeName(i)))
      .toIndexedSeq

    private lazy val columnNames = (1 to columnCount).map(i => md.getColumnName(i))

    /**
      * Encode all ResultSet rows as a JSON array of JSON objects (column name → column value).
      */
    def toJson: String = s"[${toJsonSeq.mkString(",")}]"

    /**
      * Encode each ResultSet row as a JSON object string.
      */
    def toJsonSeq: Iterator[String] = mapMsgPackMapRows(JSONWeaver.unweave(_))

    /**
      * Iterator over rows packed as MsgPack maps.
      */
    def mapMsgPackMapRows[U](f: Array[Byte] => U): Iterator[U] =
      new RStoMsgPackIterator[U](f, packRowAsMap(_))

    /**
      * Iterator over rows packed as MsgPack arrays.
      */
    def mapMsgPackArrayRows[U](f: Array[Byte] => U): Iterator[U] =
      new RStoMsgPackIterator[U](f, packRowAsArray(_))

    /**
      * Pack one ResultSet row as a MsgPack map (keys = column names).
      */
    def packRowAsMap(p: Packer): Unit =
      p.packMapHeader(columnCount)
      var col = 1
      while col <= columnCount do
        p.packString(columnNames(col - 1))
        columnCodecs(col - 1).pack(p, rs, col)
        col += 1

    /**
      * Pack one ResultSet row as a MsgPack array.
      */
    def packRowAsArray(p: Packer): Unit =
      p.packArrayHeader(columnCount)
      var col = 1
      while col <= columnCount do
        columnCodecs(col - 1).pack(p, rs, col)
        col += 1

    private class RStoMsgPackIterator[A](f: Array[Byte] => A, packer: Packer => Unit)
        extends Iterator[A]:
      private var hasNextElem: Option[Boolean] = None
      override def hasNext: Boolean            =
        hasNextElem match
          case Some(x) =>
            x
          case None =>
            val x = rs.next()
            hasNextElem = Some(x)
            x

      override def next(): A =
        val p = MessagePack.newPacker()
        packer(p)
        hasNextElem = None
        f(p.toByteArray)

    end RStoMsgPackIterator

  end ResultSetCodec

  def toJDBCColumnCodec(sqlType: Int, typeName: String): JDBCColumnCodec =
    typeName match
      // workaround for sqlite-jdbc and the like, which don't carry rich JDBC types
      case "DATE" =>
        JDBCDateCodec
      case "TIME" | "TIME WITH TIME ZONE" =>
        JDBCTimeCodec
      case "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" =>
        JDBCTimestampCodec
      case x if x.startsWith("DECIMAL") =>
        JDBCDecimalCodec
      case "BIT" =>
        JDBCBooleanCodec
      case _ =>
        sqlType match
          case Types.BIT | Types.BOOLEAN =>
            JDBCBooleanCodec
          case Types.TINYINT | Types.SMALLINT =>
            JDBCShortCodec
          case Types.INTEGER =>
            JDBCIntCodec
          case Types.BIGINT =>
            JDBCLongCodec
          case Types.REAL =>
            JDBCFloatCodec
          case Types.FLOAT | Types.DOUBLE =>
            JDBCDoubleCodec
          case Types.NUMERIC | Types.DECIMAL =>
            JDBCStringCodec
          case Types.CHAR | Types.VARCHAR | Types.LONGNVARCHAR | Types.SQLXML | Types.NCHAR | Types
                .NVARCHAR | Types.CLOB | Types.ROWID =>
            JDBCStringCodec
          case Types.BLOB | Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY =>
            JDBCBinaryCodec
          case Types.DATE =>
            JDBCDateCodec
          case Types.TIME | Types.TIME_WITH_TIMEZONE =>
            JDBCTimeCodec
          case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
            JDBCTimestampCodec
          case Types.ARRAY =>
            JDBCArrayCodec
          case Types.JAVA_OBJECT =>
            JDBCJavaObjectCodec
          case Types.NULL =>
            JDBCNullCodec
          case Types.OTHER =>
            JDBCStringCodec
          case other =>
            warn(s"Unsupported JDBC type: ${other}. Assume string type")
            JDBCStringCodec

  /**
    * Pack a single column value from a ResultSet row.
    */
  trait JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit

  object JDBCBooleanCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getBoolean(colIndex)
      if rs.wasNull() then
        p.packNil
      else
        p.packBoolean(v)

  object JDBCShortCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getShort(colIndex)
      if rs.wasNull() then
        p.packNil
      else
        p.packShort(v)

  object JDBCIntCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getInt(colIndex)
      if rs.wasNull() then
        p.packNil
      else
        p.packInt(v)

  object JDBCLongCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getLong(colIndex)
      if rs.wasNull() then
        p.packNil
      else
        p.packLong(v)

  object JDBCFloatCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getFloat(colIndex)
      if rs.wasNull() then
        p.packNil
      else
        p.packFloat(v)

  object JDBCDoubleCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getDouble(colIndex)
      if rs.wasNull() then
        p.packNil
      else
        p.packDouble(v)

  /** Decimal/Numeric → string preserves precision across drivers. */
  object JDBCDecimalCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getBigDecimal(colIndex)
      if rs.wasNull() || v == null then
        p.packNil
      else
        p.packString(v.toString)

  object JDBCStringCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getString(colIndex)
      if rs.wasNull() || v == null then
        p.packNil
      else
        p.packString(v)

  object JDBCBinaryCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getBytes(colIndex)
      if rs.wasNull() || v == null then
        p.packNil
      else
        p.packBinaryHeader(v.length)
        p.writePayload(v)

  object JDBCDateCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getDate(colIndex)
      if rs.wasNull() || v == null then
        p.packNil
      else
        p.packString(v.toString)

  object JDBCTimeCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getTime(colIndex)
      if rs.wasNull() || v == null then
        p.packNil
      else
        p.packString(v.toString)

  object JDBCTimestampCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getTimestamp(colIndex)
      if rs.wasNull() || v == null then
        p.packNil
      else
        p.packString(v.toString)

  object JDBCArrayCodec extends JDBCColumnCodec with LogSupport:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val v = rs.getArray(colIndex)
      if rs.wasNull() || v == null then
        p.packNil
      else
        packJavaSqlArray(p, v)

    private def packJavaSqlArray(p: Packer, v: java.sql.Array): Unit =
      val arr: AnyRef = v.getArray
      arr match
        case a: Array[String] =>
          p.packArrayHeader(a.length)
          a.foreach(s =>
            if s == null then
              p.packNil
            else
              p.packString(s)
          )
        case a: Array[Int] =>
          p.packArrayHeader(a.length)
          a.foreach(p.packInt(_))
        case a: Array[Short] =>
          p.packArrayHeader(a.length)
          a.foreach(p.packShort(_))
        case a: Array[Long] =>
          p.packArrayHeader(a.length)
          a.foreach(p.packLong(_))
        case a: Array[Char] =>
          p.packArrayHeader(a.length)
          a.foreach(c => p.packString(c.toString))
        case a: Array[Byte] =>
          p.packBinaryHeader(a.length)
          p.writePayload(a)
        case a: Array[Float] =>
          p.packArrayHeader(a.length)
          a.foreach(p.packFloat(_))
        case a: Array[Double] =>
          p.packArrayHeader(a.length)
          a.foreach(p.packDouble(_))
        case a: Array[Boolean] =>
          p.packArrayHeader(a.length)
          a.foreach(p.packBoolean(_))
        case a: Array[?] =>
          p.packArrayHeader(a.length)
          a.foreach(packAnyRef(p, _))
        case other =>
          throw UnsupportedOperationException(
            s"Reading array type of ${arr.getClass} is not supported: ${arr}"
          )
      end match

    end packJavaSqlArray

    private def packAnyRef(p: Packer, v: Any): Unit =
      v match
        case null =>
          p.packNil
        case s: String =>
          p.packString(s)
        case b: Boolean =>
          p.packBoolean(b)
        case b: java.lang.Boolean =>
          p.packBoolean(b.booleanValue)
        case n: Byte =>
          p.packByte(n)
        case n: Short =>
          p.packShort(n)
        case n: Int =>
          p.packInt(n)
        case n: Long =>
          p.packLong(n)
        case n: Float =>
          p.packFloat(n)
        case n: Double =>
          p.packDouble(n)
        case n: java.lang.Number =>
          p.packString(n.toString)
        case bs: Array[Byte] =>
          p.packBinaryHeader(bs.length)
          p.writePayload(bs)
        case other =>
          // Last-resort: stringify. Matches airframe's JDBCJavaObjectCodec behaviour.
          p.packString(other.toString)

  end JDBCArrayCodec

  object JDBCJavaObjectCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit =
      val obj = rs.getObject(colIndex)
      if rs.wasNull() || obj == null then
        p.packNil
      else
        p.packString(obj.toString)

  object JDBCNullCodec extends JDBCColumnCodec:
    def pack(p: Packer, rs: ResultSet, colIndex: Int): Unit = p.packNil

end JDBCCodec
