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
package wvlet.lang.engine.catalog

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.vector.*
import wvlet.uni.parquet.*
import wvlet.uni.parquet.format.LogicalType
import wvlet.uni.parquet.format.PhysicalType
import wvlet.uni.parquet.format.TimeUnit

import scala.collection.mutable

/**
  * Scan request pushed down to a table source. `filter` is implied by the query's own filter: a
  * source may use it to skip data that can't match (e.g. Parquet row groups by statistics), but
  * rows it returns are still filtered by the engine
  */
case class ScanSpec(columns: Option[Seq[String]] = None, filter: Option[ParquetFilter] = None)

/** A readable table: its columns and a batch iterator over the requested columns */
trait TableSource:
  def name: String
  def schema: Schema

  /** Batches with exactly the requested columns (all columns when not given), in that order */
  def scan(spec: ScanSpec): Iterator[Batch]

  /** Whether `scan` uses `spec.filter` to skip data; otherwise the filter is ignored */
  def supportsFilter: Boolean = false

  /** Whether `spec.filter` may refer to this column */
  def canFilterOn(column: String): Boolean = supportsFilter

  /** Row count estimate for join ordering */
  def estimatedRows: Option[Long] = None

/** An in-memory table */
class MemTable(val name: String, val schema: Schema, batches: Seq[Batch]) extends TableSource:
  override def estimatedRows: Option[Long]  = Some(batches.map(_.size.toLong).sum)
  def scan(spec: ScanSpec): Iterator[Batch] =
    spec.columns match
      case None =>
        batches.iterator
      case Some(cols) =>
        val idx = cols.map(c => schema.names.indexOf(c)).toArray
        batches.iterator.map(_.select(idx))

/** A Parquet file, a directory of Parquet files (Hive partitions), or an Iceberg table */
class ParquetSource(val name: String, path: String) extends TableSource:
  private val isSingleFile: Boolean =
    path.endsWith(".parquet") && !path.exists(c => c == '*' || c == '{')

  private lazy val parquetSchema: ParquetSchema =
    if isSingleFile then
      val f = Parquet.open(path)
      try f.schema
      finally f.close()
    else
      val ds = Parquet.dataset(path)
      try ds.schema
      finally ds.close()

  lazy val schema: Schema = Schema(
    parquetSchema.fields.map(f => Field(f.name, ParquetSource.ptypeOf(f), List(name)))
  )

  override def supportsFilter: Boolean = true

  /**
    * UINT64 columns are read as signed Int64 values while Parquet statistics order them unsigned,
    * so a filter on them could skip row groups holding matching rows
    */
  override def canFilterOn(column: String): Boolean = parquetSchema
    .field(column)
    .forall {
      case p: ParquetField.Primitive =>
        !(
          p.physicalType == PhysicalType.INT64 &&
            p.logicalType.contains(LogicalType.Integer(64, false))
        )
      case _ =>
        true
    }

  override lazy val estimatedRows: Option[Long] =
    try
      if isSingleFile then
        val f = Parquet.open(path)
        try Some(f.numRows)
        finally f.close()
      else
        None
    catch
      case _: Exception =>
        None

  def scan(spec: ScanSpec): Iterator[Batch] =
    val cols    = spec.columns.getOrElse(schema.names)
    val types   = cols.map(c => schema.fields.find(_.name == c).map(_.ptype).getOrElse(PType.AnyT))
    var options = ParquetReadOptions.default.withColumns(cols*).withBinaryAsString(true)
    spec.filter.foreach(f => options = options.withRowGroupFilter(f))
    if isSingleFile then
      val f  = Parquet.open(path)
      val it = f.readBatches(options)
      ParquetSource.closing(it.map(b => ParquetSource.toBatch(b, cols, types)), () => f.close())
    else
      val ds = Parquet.dataset(path)
      val it = ds.readBatches(options)
      ParquetSource.closing(it.map(b => ParquetSource.toBatch(b, cols, types)), () => ds.close())

end ParquetSource

object ParquetSource:

  def closing[A](it: Iterator[A], close: () => Unit): Iterator[A] =
    new Iterator[A]:
      private var closed   = false
      def hasNext: Boolean =
        val h = it.hasNext
        if !h && !closed then
          closed = true
          close()
        h
      def next(): A = it.next()

  def ptypeOf(f: ParquetField): PType =
    f match
      case p: ParquetField.Primitive =>
        (p.physicalType, p.logicalType) match
          case (_, Some(LogicalType.Decimal(scale, precision))) =>
            PType.decimal(precision, scale)
          case (PhysicalType.BOOLEAN, _) =>
            PType.Bool
          case (PhysicalType.INT32, Some(LogicalType.Date)) =>
            PType.Date
          case (PhysicalType.INT32, Some(LogicalType.Integer(32, false))) =>
            PType.Int64
          case (PhysicalType.INT32, Some(LogicalType.Time(_, _))) =>
            PType.AnyT
          case (PhysicalType.INT32, _) =>
            PType.Int32
          case (PhysicalType.INT64, Some(LogicalType.Timestamp(_, _))) =>
            PType.Timestamp
          case (PhysicalType.INT64, Some(LogicalType.Time(_, _))) =>
            PType.AnyT
          case (PhysicalType.INT64, _) =>
            PType.Int64
          case (PhysicalType.INT96, _) =>
            PType.Timestamp
          case (PhysicalType.FLOAT, _) | (PhysicalType.DOUBLE, _) =>
            PType.Float64
          case (
                PhysicalType.BYTE_ARRAY,
                Some(LogicalType.String | LogicalType.Enum | LogicalType.Json)
              ) =>
            PType.Str
          case (PhysicalType.BYTE_ARRAY, None) =>
            PType.Str
          case (PhysicalType.FIXED_LEN_BYTE_ARRAY, Some(LogicalType.Uuid)) =>
            PType.Str
          case _ =>
            PType.AnyT
      case g: ParquetField.Group =>
        PType.AnyT

  /** The reader's null flags, shared without copying (vectors are immutable) */
  private def nullsOf(c: ColumnVector): Array[Boolean] = c.nullFlags

  /** Convert uni column vectors into engine vectors, mostly without copying the value arrays */
  def toBatch(b: ColumnBatch, cols: Seq[String], types: Seq[PType]): Batch =
    val vecs = cols
      .indices
      .map { k =>
        val c = b.column(cols(k))
        toVec(c, types(k))
      }
    Batch(vecs.toArray, b.numRows)

  def toVec(c: ColumnVector, ptype: PType): Vec =
    val nulls = nullsOf(c)
    (c, ptype) match
      case (x: ColumnVector.BooleanColumn, PType.Bool) =>
        BoolVec(x.values, nulls)
      case (x: ColumnVector.IntColumn, PType.Int32 | PType.Date) =>
        IntVec(ptype, x.values, nulls)
      case (x: ColumnVector.IntColumn, PType.Int64) =>
        // unsigned int32
        LongVec(PType.Int64, x.values.map(v => Integer.toUnsignedLong(v)), nulls)
      case (x: ColumnVector.IntColumn, d: PType.Decimal) =>
        LongVec(d, x.values.map(_.toLong), nulls)
      case (x: ColumnVector.LongColumn, PType.Int64) =>
        LongVec(PType.Int64, x.values, nulls)
      case (x: ColumnVector.LongColumn, d: PType.Decimal) =>
        LongVec(d, x.values, nulls)
      case (x: ColumnVector.LongColumn, PType.Timestamp) =>
        val unit =
          x.field.logicalType match
            case Some(LogicalType.Timestamp(_, u)) =>
              u
            case _ =>
              TimeUnit.MICROS
        unit match
          case TimeUnit.MICROS =>
            LongVec(PType.Timestamp, x.values, nulls)
          case TimeUnit.MILLIS =>
            LongVec(PType.Timestamp, x.values.map(_ * 1000L), nulls)
          case TimeUnit.NANOS =>
            LongVec(PType.Timestamp, x.values.map(v => Math.floorDiv(v, 1000L)), nulls)
      case (x: ColumnVector.FloatColumn, PType.Float64) =>
        DoubleVec(x.values.map(_.toDouble), nulls)
      case (x: ColumnVector.DoubleColumn, PType.Float64) =>
        DoubleVec(x.values, nulls)
      case (x: ColumnVector.BinaryColumn, PType.Str) =>
        val n   = x.size
        val out = new Array[String](n)
        var i   = 0
        while i < n do
          if nulls == null || !nulls(i) then
            out(i) = x.getString(i)
          i += 1
        StrVec(out, nulls)
      case (_, PType.Timestamp) =>
        val b = VecBuilder(PType.Timestamp, c.size)
        var i = 0
        while i < c.size do
          c.getValue(i) match
            case null =>
              b.addNull()
            case t: java.time.Instant =>
              b.add(t.getEpochSecond * ValueOps.MicrosPerSecond + t.getNano / 1000)
            case other =>
              b.add(ValueOps.parseTimestamp(other.toString))
          i += 1
        b.result()
      case _ =>
        val b = VecBuilder(ptype, c.size)
        var i = 0
        while i < c.size do
          b.add(normalize(c.getValue(i)))
          i += 1
        b.result()
    end match

  end toVec

  /** Nested Parquet values into engine values (Seq / Map / numbers / strings) */
  private def normalize(v: Any): Any =
    v match
      case null =>
        null
      case b: scala.math.BigDecimal =>
        b.bigDecimal
      case s: Seq[?] =>
        s.map(normalize)
      case m: Map[?, ?] =>
        m.map((k, x) => normalize(k) -> normalize(x))
      case d: java.time.LocalDate =>
        d.toString
      case t: java.time.Instant =>
        ValueOps.formatTimestamp(t.getEpochSecond * ValueOps.MicrosPerSecond + t.getNano / 1000)
      case b: Array[Byte] =>
        String(b, "UTF-8")
      case other =>
        other

end ParquetSource

/** Named tables visible to queries */
class EngineCatalog:
  private val tables = mutable.LinkedHashMap.empty[String, TableSource]

  def register(source: TableSource): Unit = tables.synchronized {
    tables(source.name.toLowerCase) = source
  }

  def registerParquet(name: String, path: String): Unit = register(ParquetSource(name, path))

  def drop(name: String): Unit = tables.synchronized {
    tables.remove(name.toLowerCase)
  }

  def find(name: String): Option[TableSource] = tables.synchronized {
    tables.get(name.toLowerCase)
  }

  def tableNames: Seq[String] = tables.synchronized(tables.values.map(_.name).toSeq)

  def get(name: String): TableSource = find(name).getOrElse {
    throw StatusCode.TABLE_NOT_FOUND.newException(s"Table not found: ${name}")
  }
