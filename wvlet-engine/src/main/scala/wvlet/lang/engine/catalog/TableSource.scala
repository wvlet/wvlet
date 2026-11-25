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
import wvlet.uni.cloud.parquet.*
import wvlet.uni.cloud.parquet.format.LogicalType
import wvlet.uni.cloud.parquet.format.PhysicalType
import wvlet.uni.cloud.parquet.format.TimeUnit

import scala.collection.mutable

/**
  * Scan request pushed down to a table source. `filter` is implied by the query's own filter: a
  * source may use it to skip data that can't match (e.g. Parquet row groups by statistics), but
  * rows it returns are still filtered by the engine. `rowFilter` is exact: the source returns only
  * the rows it matches, and the engine doesn't evaluate it again. Only a source that
  * [[TableSource.supportsRowFilter]] gets one; its columns may be outside `columns`
  */
case class ScanSpec(
    columns: Option[Seq[String]] = None,
    filter: Option[ParquetFilter] = None,
    footerCache: Boolean = false,
    rowFilter: Option[ParquetFilter] = None
)

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

  /**
    * Whether `scan` returns only the rows matching `spec.rowFilter`, decoding the other columns
    * only for those rows. A source without it never gets a row filter
    */
  def supportsRowFilter: Boolean = false

  /** Whether `spec.rowFilter` may compare this column, exactly as the engine compares its values */
  def canRowFilterOn(column: String): Boolean = false

  /**
    * The [min, max] range of an integer (or date, in epoch days) column's non-null values, from
    * metadata only, for estimating how many rows a range condition keeps. None when unknown
    */
  def columnRange(column: String): Option[(Long, Long)] = None

  /** Row count estimate for join ordering */
  def estimatedRows: Option[Long] = None

  /**
    * Estimate of the number of distinct non-null values of a column, for join ordering. None when
    * unknown
    */
  def estimatedDistinct(column: String): Option[Double] = None

  /** EXPLAIN lines of what a scan of this source reads, nested above the scan (at `indent`) */
  def explainDetail(indent: Int): Option[String] = None

  /** Whether [[scanSplits]] may cut a scan into parts */
  def canSplit: Boolean = false

  /**
    * The scan cut into parts that can be read independently and concurrently (e.g. Parquet row
    * groups), whose batches in part order are the batches of `scan(spec)`. None when the source
    * can't split, or the scan has a single part
    */
  def scanSplits(spec: ScanSpec): Option[ScanSplits] = None

  /**
    * The identity of the data this source reads: sources with equal keys read the same rows, so
    * that plans over them can be shared. The source itself by default; sources created per
    * reference of the same file use their path
    */
  def key: Any = this

end TableSource

/**
  * Independently readable parts of a scan; `close` releases what the parts share. `rows` holds the
  * row count of each part when the source knows it (e.g. from Parquet row-group metadata), and is
  * empty otherwise
  */
case class ScanSplits(
    parts: IndexedSeq[() => Iterator[Batch]],
    close: () => Unit,
    rows: IndexedSeq[Long] = IndexedSeq.empty
)

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

/**
  * A Parquet file, a directory of Parquet files (Hive partitions), or an Iceberg table. With
  * `footerCache`, planning reads (schema, row count, statistics) of a single file open it through
  * [[ParquetFooterCache]]; scans do when their [[ScanSpec]] says so
  */
class ParquetSource(val name: String, val path: String, val footerCache: Boolean = false)
    extends TableSource:
  override def key: Any = ("parquet", path)

  /** Whether `path` names one Parquet file, rather than a folder or a glob */
  val isSingleFile: Boolean = path.endsWith(".parquet") && !path.exists(c => c == '*' || c == '{')

  /** Open the single file, through the footer cache when `cached` */
  private def openFile(cached: Boolean): ParquetFooterCache.Opened =
    if cached then
      ParquetFooterCache.open(path)
    else
      ParquetFooterCache.Opened(Parquet.open(path), None, false)

  private lazy val parquetSchema: ParquetSchema =
    if isSingleFile then
      val f = openFile(footerCache).file
      try f.schema
      finally f.close()
    else
      val ds = ParquetSource.openDataset(path)
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

  /** Single files only: a folder or Iceberg table still filters rows after decoding them */
  override def supportsRowFilter: Boolean = isSingleFile

  /**
    * Top-level primitive columns that uni's row filter compares as the engine compares their
    * values: signed integers, dates, booleans, MILLIS or MICROS timestamps (the engine floors NANOS
    * to micros, while uni would compare the nanos), and strings
    */
  override def canRowFilterOn(column: String): Boolean = parquetSchema
    .fields
    .find(_.name == column)
    .exists {
      case p: ParquetField.Primitive if !p.isRepeated =>
        (p.physicalType, p.logicalType) match
          case (PhysicalType.BOOLEAN, None) =>
            true
          case (PhysicalType.INT32 | PhysicalType.INT64, None) =>
            true
          case (PhysicalType.INT32 | PhysicalType.INT64, Some(LogicalType.Integer(_, true))) =>
            true
          case (PhysicalType.INT32, Some(LogicalType.Date)) =>
            true
          case (PhysicalType.INT64, Some(LogicalType.Timestamp(_, unit))) =>
            unit == TimeUnit.MILLIS || unit == TimeUnit.MICROS
          case (PhysicalType.BYTE_ARRAY, None | Some(LogicalType.String | LogicalType.Enum)) =>
            true
          case _ =>
            false
      case _ =>
        false
    }

  /** From the file's column statistics, for signed integer and date columns */
  override def columnRange(column: String): Option[(Long, Long)] =
    if !isSingleFile || !canFilterOn(column) then
      None
    else
      def asLong(v: Any): Option[Long] =
        v match
          case i: Int =>
            Some(i.toLong)
          case l: Long =>
            Some(l)
          case d: java.time.LocalDate =>
            Some(d.toEpochDay)
          case _ =>
            None
      schema.fields.find(_.name == column).map(_.ptype) match
        case Some(PType.Int32 | PType.Int64 | PType.Date) =>
          try
            val opened = openFile(footerCache)
            try
              for
                s   <- opened.columnStatistics.get(column)
                min <- s.min.flatMap(asLong)
                max <- s.max.flatMap(asLong)
              yield (min, max)
            finally opened.file.close()
          catch
            case _: Exception =>
              None
        case _ =>
          None

  override lazy val estimatedRows: Option[Long] =
    try
      if isSingleFile then
        val f = openFile(footerCache).file
        try Some(f.numRows)
        finally f.close()
      else
        None
    catch
      case _: Exception =>
        None

  /**
    * Integer and date columns: the width of the [min, max] range in the file statistics, at most
    * the non-null count. Strings of files up to [[ParquetSource.DistinctScanLimit]] rows: counted
    * by reading the column once, as DuckDB keeps distinct-count statistics for join ordering.
    * Estimates are cached per file version (see [[ParquetSource.MaxDistinctEntries]]); a file
    * [[ParquetFooterCache]] wouldn't cache is estimated once per source (a source lives for one
    * query)
    */
  override def estimatedDistinct(column: String): Option[Double] =
    // Stat before computing, so that the entry describes the file read or an older version
    def version = ParquetFooterCache
      .fileVersion(path)
      .filterNot((_, mtime) => ParquetFooterCache.isRecentWrite(mtime))
    if !isSingleFile then
      None
    else
      version match
        case None =>
          // A file whose rewrites can't be told apart: not shared with other queries
          localDistinct.synchronized(localDistinct.getOrElseUpdate(column, computeDistinct(column)))
        case Some(v) =>
          ParquetSource.cachedDistinct(path, column, v) match
            case Some(d) =>
              d
            case None =>
              // Computed outside the lock, so that a scan doesn't block other planners
              val d = computeDistinct(column)
              ParquetSource.putDistinct(path, column, v, d)
              d

  // Estimates of a file without a trusted version, kept for this source only
  private val localDistinct = scala.collection.mutable.HashMap.empty[String, Option[Double]]

  private def computeDistinct(column: String): Option[Double] =
    if !isSingleFile then
      None
    else
      try
        val opened = openFile(footerCache)
        val f      = opened.file
        try
          val rows    = f.numRows
          val stats   = opened.columnStatistics.get(column)
          val nonNull = stats.flatMap(_.numNulls).map(n => rows - n).getOrElse(rows).toDouble
          def asLong(v: Any): Option[Long] =
            v match
              case i: Int =>
                Some(i.toLong)
              case l: Long =>
                Some(l)
              case d: java.time.LocalDate =>
                Some(d.toEpochDay)
              case _ =>
                None
          schema.fields.find(_.name == column).map(_.ptype) match
            // canFilterOn excludes UINT64, whose statistics are ordered unsigned
            case Some(PType.Int32 | PType.Int64 | PType.Date) if canFilterOn(column) =>
              for
                s   <- stats
                min <- s.min.flatMap(asLong)
                max <- s.max.flatMap(asLong)
              yield math.min(nonNull, max.toDouble - min.toDouble + 1)
            case Some(PType.Str) if rows <= ParquetSource.DistinctScanLimit =>
              val seen = scala.collection.mutable.HashSet.empty[Any]
              scan(ScanSpec(Some(Seq(column)), footerCache = footerCache)).foreach { b =>
                val v = b.columns(0)
                var i = 0
                while i < b.size do
                  val x = v.get(i)
                  if x != null then
                    seen += x
                  i += 1
              }
              Some(math.max(1.0, seen.size.toDouble))
            case _ =>
              None
        finally
          f.close()
        end try
      catch
        case _: Exception =>
          None

  private def readOptions(spec: ScanSpec): (Seq[String], Seq[PType], ParquetReadOptions) =
    ParquetSource.readOptions(schema, spec)

  override def canSplit: Boolean = isSingleFile

  /** One part per row group the filter doesn't skip */
  override def scanSplits(spec: ScanSpec): Option[ScanSplits] =
    if !isSingleFile then
      None
    else
      val (cols, types, options) = readOptions(spec)
      val f                      = openFile(spec.footerCache).file
      val groups                 =
        try
          ParquetSource.selectRowGroups(f, spec, options)
        catch
          case e: Throwable =>
            f.close()
            throw e
      if groups.size <= 1 then
        f.close()
        None
      else
        val parts = groups.map { g => () =>
          Iterator.single(ParquetSource.toBatch(f.readRowGroup(g, options), cols, types))
        }
        val meta = f.rowGroups
        Some(ScanSplits(parts, () => f.close(), groups.map(g => meta(g).numRows)))

  def scan(spec: ScanSpec): Iterator[Batch] =
    val (cols, types, options) = readOptions(spec)
    if isSingleFile then
      val f  = openFile(spec.footerCache).file
      val it =
        try
          if spec.filter.exists(DecimalStats.hasDecimalCompare(_, f.schema)) then
            // Read only the row groups the decimal statistics don't skip either
            ParquetSource.selectRowGroups(f, spec, options).iterator.map(f.readRowGroup(_, options))
          else
            f.readBatches(options)
        catch
          case e: Throwable =>
            f.close()
            throw e
      ParquetSource.closing(it.map(b => ParquetSource.toBatch(b, cols, types)), () => f.close())
    else
      val ds = ParquetSource.openDataset(path)
      val it = ds.readBatches(options)
      ParquetSource.closing(it.map(b => ParquetSource.toBatch(b, cols, types)), () => ds.close())

end ParquetSource

object ParquetSource:

  /** Up to how many rows a string column is read to count its distinct values */
  val DistinctScanLimit: Long = 1000000

  /** Up to how many (file, column) distinct-count estimates are kept */
  val MaxDistinctEntries: Int = 4096

  /** A distinct-count estimate and the file version (size, last-modified) it was computed from */
  private case class DistinctEntry(version: (Long, Long), estimate: Option[Double])

  // Estimates per (file path, column), shared by the sources of the same file. Access-ordered, so
  // that the eldest entry is the least recently used
  private val distinctCache =
    new java.util.LinkedHashMap[(String, String), DistinctEntry](16, 0.75f, true):
      override def removeEldestEntry(
          e: java.util.Map.Entry[(String, String), DistinctEntry]
      ): Boolean = size() > MaxDistinctEntries

  /** The cached estimate of `column` in `path`, when it was computed from file version `version` */
  private def cachedDistinct(
      path: String,
      column: String,
      version: (Long, Long)
  ): Option[Option[Double]] = distinctCache.synchronized {
    Option(distinctCache.get((path, column))).filter(_.version == version).map(_.estimate)
  }

  private def putDistinct(
      path: String,
      column: String,
      version: (Long, Long),
      estimate: Option[Double]
  ): Unit = distinctCache.synchronized {
    distinctCache.put((path, column), DistinctEntry(version, estimate))
  }

  /** The file version the cached estimate of `column` in `path` was computed from (for tests) */
  def cachedDistinctVersion(path: String, column: String): Option[(Long, Long)] = distinctCache
    .synchronized {
      Option(distinctCache.get((path, column))).map(_.version)
    }

  /**
    * The row groups of `f` that neither uni's row-group filter nor the decimal statistics
    * ([[DecimalStats]]) of `spec.filter` skip
    */
  def selectRowGroups(
      f: ParquetFile,
      spec: ScanSpec,
      options: ParquetReadOptions
  ): IndexedSeq[Int] =
    val groups = f.selectRowGroups(options)
    spec.filter match
      case Some(filter) if DecimalStats.hasDecimalCompare(filter, f.schema) =>
        val meta = f.rowGroups
        groups.filter(g => DecimalStats.mayMatch(filter, f.schema, meta(g)))
      case _ =>
        groups

  /** The columns, their engine types, and uni read options of a scan over `schema` */
  def readOptions(schema: Schema, spec: ScanSpec): (Seq[String], Seq[PType], ParquetReadOptions) =
    val cols  = spec.columns.getOrElse(schema.names)
    val types = cols.map(c => schema.fields.find(_.name == c).map(_.ptype).getOrElse(PType.AnyT))
    // Decimals are unscaled Long values in the engine: INT32 decimals are read as such directly
    var options = ParquetReadOptions
      .default
      .withColumns(cols*)
      .withBinaryAsString(true)
      .withInt32DecimalAsLong(true)
    spec.filter.foreach(f => options = options.withRowGroupFilter(f))
    spec.rowFilter.foreach(f => options = options.withFilter(f))
    (cols, types, options)

  /**
    * A Parquet dataset (a folder of Parquet files, a Delta table, or an Iceberg table). What uni
    * refuses to read of an Iceberg table (ORC data files, encryption) is a NOT_IMPLEMENTED error
    * naming the table
    */
  def openDataset(path: String): ParquetDataset =
    try
      Parquet.dataset(path)
    catch
      case e: ParquetException if isIcebergPath(path) =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"The wvlet engine can't read the Iceberg table ${path}: ${e.getMessage}",
            e
          )

  private def isIcebergPath(path: String): Boolean =
    path.endsWith(".metadata.json") || IcebergWriter.isTable(path)

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
          // Adjusted to UTC: an instant (TIMESTAMPTZ), as DuckDB reads it
          case (PhysicalType.INT64, Some(LogicalType.Timestamp(true, _))) =>
            PType.TimestampTz
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
        LongVec(PType.Int64, Vec.mapIntToLong(x.values)(Integer.toUnsignedLong), nulls)
      case (x: ColumnVector.IntColumn, d: PType.Decimal) =>
        LongVec(d, Vec.mapIntToLong(x.values)(_.toLong), nulls)
      case (x: ColumnVector.LongColumn, PType.Int64) =>
        LongVec(PType.Int64, x.values, nulls)
      case (x: ColumnVector.LongColumn, d: PType.Decimal) =>
        LongVec(d, x.values, nulls)
      case (x: ColumnVector.LongColumn, t) if t.isTimestamp =>
        val unit =
          x.field.logicalType match
            case Some(LogicalType.Timestamp(_, u)) =>
              u
            case _ =>
              TimeUnit.MICROS
        unit match
          case TimeUnit.MICROS =>
            LongVec(t, x.values, nulls)
          case TimeUnit.MILLIS =>
            LongVec(t, Vec.mapLong(x.values)(_ * 1000L), nulls)
          case TimeUnit.NANOS =>
            LongVec(t, Vec.mapLong(x.values)(Math.floorDiv(_, 1000L)), nulls)
      case (x: ColumnVector.FloatColumn, PType.Float64) =>
        DoubleVec(Vec.mapFloatToDouble(x.values), nulls)
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
      case (_, t) if t.isTimestamp =>
        val b = VecBuilder(t, c.size)
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

  // Maps a (possibly qualified) table reference to its catalog key, when the owner of this
  // catalog names tables by a scheme of its own (e.g. relative to a default schema)
  private var nameKey: Option[String => String] = None

  /**
    * Name tables by `key`: a reference resolves to exactly the table `key(fullName)`, with no
    * fallback to the bare table name, so `s.t` never silently reads another schema's `t`
    */
  def withNameKey(key: String => String): EngineCatalog =
    nameKey = Some(key)
    this

  /**
    * A copy of this catalog, with the same naming, where `source` replaces the table of its name:
    * for running a query over one part of a table
    */
  def overlay(source: TableSource): EngineCatalog =
    val c = EngineCatalog()
    tables.synchronized(c.tables ++= tables)
    c.nameKey = nameKey
    c.register(source)
    c

  /** The table a reference (`name`, qualified as `fullName`) points to */
  def lookup(name: String, fullName: String): Option[TableSource] =
    nameKey match
      case Some(key) =>
        find(key(fullName))
      case None =>
        // A qualified reference names its table exactly; the bare name is the fallback
        find(fullName).orElse(find(name))

  /**
    * information_schema views of this catalog. Tables live in the `main` schema of the `memory`
    * catalog, the names DuckDB uses for an in-memory database
    */
  def informationSchema(name: String): Option[TableSource] =
    def strTable(n: String, cols: Seq[String], rows: Seq[Seq[String]]): TableSource =
      val schema = Schema(cols.map(c => Field(c, PType.Str)).toIndexedSeq)
      val batch  = Batch(
        cols.indices.map(i => Vec.fromValues(PType.Str, rows.map(_(i)))).toArray,
        rows.size
      )
      MemTable(n, schema, Seq(batch))
    name.toLowerCase.split("\\.").takeRight(2) match
      case Array("information_schema", "schemata") =>
        Some(
          strTable(
            "schemata",
            Seq("catalog_name", "schema_name"),
            Seq(Seq(EngineCatalog.CatalogName, EngineCatalog.SchemaName))
          )
        )
      case Array("information_schema", "tables") =>
        Some(
          strTable(
            "tables",
            Seq("table_catalog", "table_schema", "table_name", "table_type"),
            tableNames.map(t =>
              Seq(EngineCatalog.CatalogName, EngineCatalog.SchemaName, t, "BASE TABLE")
            )
          )
        )
      case _ =>
        None

  def get(name: String): TableSource = find(name).getOrElse {
    throw StatusCode.TABLE_NOT_FOUND.newException(s"Table not found: ${name}")
  }

end EngineCatalog

object EngineCatalog:
  val CatalogName = "memory"
  val SchemaName  = "main"
