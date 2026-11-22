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
import wvlet.uni.io.IO
import wvlet.uni.log.LogSupport
import wvlet.uni.parquet.ColumnBatch
import wvlet.uni.parquet.ColumnVector
import wvlet.uni.parquet.Parquet
import wvlet.uni.parquet.ParquetField
import wvlet.uni.parquet.ParquetSchema
import wvlet.uni.parquet.ParquetType
import wvlet.uni.parquet.ParquetWriteOptions
import wvlet.uni.parquet.format.CompressionCodec
import wvlet.uni.parquet.format.LogicalType
import wvlet.uni.parquet.format.PhysicalType
import wvlet.uni.parquet.format.TimeUnit

import java.math.BigDecimal as JBigDecimal
import java.nio.charset.StandardCharsets

/** The file formats the engine writes */
enum DataFileFormat:
  case Parquet,
    Csv,
    Tsv,
    JsonLines

object DataFileFormat:
  /** The format of a path by its extension, or by an explicit `format` option */
  def of(path: String, format: Option[String] = None): DataFileFormat =
    format.map(_.toLowerCase) match
      case Some("parquet") =>
        Parquet
      case Some("csv") =>
        Csv
      case Some("tsv") =>
        Tsv
      case Some("json" | "jsonl" | "ndjson") =>
        JsonLines
      case Some(other) =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported file format: ${other}")
      case None =>
        val lower = path.toLowerCase
        if lower.endsWith(".parquet") then
          Parquet
        else if lower.endsWith(".csv") then
          Csv
        else if lower.endsWith(".tsv") then
          Tsv
        else if lower.endsWith(".json") || lower.endsWith(".jsonl") || lower.endsWith(".ndjson")
        then
          JsonLines
        else
          throw StatusCode
            .NOT_IMPLEMENTED
            .newException(
              s"Unsupported file to write: ${path} (supported: .parquet, .csv, .tsv, .json, .jsonl)"
            )

end DataFileFormat

/**
  * Writes engine batches to a local data file: Parquet through uni's pure-Scala columnar writer,
  * CSV / TSV with a header row, or newline-delimited JSON. Works on JVM, Scala.js (Node.js), and
  * Native
  */
object DataFileWriter extends LogSupport:

  /**
    * Write `batches` to `path`, replacing the file. `options` are the `save to ... with` options:
    * `format`, `compression` (Parquet), and `header` / `delimiter` (CSV)
    */
  def write(
      path: String,
      schema: Schema,
      batches: Seq[Batch],
      options: Map[String, Any] = Map.empty
  ): Unit =
    if path.contains("://") then
      throw StatusCode
        .NOT_IMPLEMENTED
        .newException(s"Writing to remote paths is not supported by the wvlet engine yet: ${path}")
    val opts   = options.map((k, v) => k.toLowerCase -> v)
    val format = DataFileFormat.of(path, opts.get("format").map(_.toString))
    parentOf(path).foreach(IO.createDirectoryIfNotExists)
    format match
      case DataFileFormat.Parquet =>
        writeParquet(path, schema, batches, opts)
      case text =>
        IO.writeString(path, "")
        textChunks(text, schema, batches, opts, withHeader = true).foreach(IO.appendString(path, _))

  /**
    * Add rows to the end of an existing CSV / TSV / JSON lines file without rewriting its rows, so
    * the existing text stays as is. `schema` names the file's columns (rows map to them by
    * position)
    */
  def appendText(path: String, schema: Schema, batches: Seq[Batch]): Unit =
    val format = DataFileFormat.of(path)
    if format == DataFileFormat.Parquet then
      throw StatusCode.INVALID_ARGUMENT.newException(s"Not a text file: ${path}")
    textChunks(format, schema, batches, Map.empty, withHeader = false).foreach(
      IO.appendString(path, _)
    )

  /**
    * Replace `path` with `batches`, writing a temporary file first so a failed write leaves the
    * original file intact
    */
  def replace(path: String, schema: Schema, batches: Seq[Batch]): Unit =
    val tmp = s"${path}.tmp-${wvlet.uni.util.ULID.newULIDString}"
    try
      write(tmp, schema, batches, Map("format" -> formatName(DataFileFormat.of(path))))
      IO.copy(tmp, path, wvlet.uni.io.CopyOptions().withOverwrite(true))
    finally
      IO.deleteIfExists(tmp)

  private def formatName(f: DataFileFormat): String =
    f match
      case DataFileFormat.Parquet =>
        "parquet"
      case DataFileFormat.Csv =>
        "csv"
      case DataFileFormat.Tsv =>
        "tsv"
      case DataFileFormat.JsonLines =>
        "json"

  // Text output, one chunk per batch, so large results are never held as one string
  private def textChunks(
      format: DataFileFormat,
      schema: Schema,
      batches: Seq[Batch],
      opts: Map[String, Any],
      withHeader: Boolean
  ): Iterator[String] =
    format match
      case DataFileFormat.Csv =>
        delimitedChunks(schema, batches, opts, ",", withHeader)
      case DataFileFormat.Tsv =>
        delimitedChunks(schema, batches, opts, "\t", withHeader)
      case _ =>
        opts
          .keys
          .filterNot(_ == "format")
          .foreach { k =>
            throw StatusCode
              .NOT_IMPLEMENTED
              .newException(s"Unsupported JSON write option for the wvlet engine: ${k}")
          }
        batches.iterator.map(b => jsonLines(schema, b))

  private def parentOf(path: String): Option[String] =
    val i = path.lastIndexWhere(c => c == '/' || c == '\\')
    Option.when(i > 0)(path.substring(0, i))

  // --- Parquet ------------------------------------------------------------------------------

  private val ignoredParquetOptions = Set("format", "row_group_size", "row_group_size_bytes")

  private def writeParquet(
      path: String,
      schema: Schema,
      batches: Seq[Batch],
      opts: Map[String, Any]
  ): Unit =
    opts
      .keys
      .filterNot(k => ignoredParquetOptions.contains(k) || k == "compression")
      .foreach { k =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Unsupported Parquet write option for the wvlet engine: ${k}")
      }
    if opts.contains("row_group_size") || opts.contains("row_group_size_bytes") then
      debug("row_group_size is not applied by the wvlet engine; using the default row group size")
    val writeOptions =
      opts.get("compression").map(_.toString.toLowerCase) match
        case None =>
          ParquetWriteOptions.default
        case Some(c) =>
          ParquetWriteOptions.default.withCompression(compressionOf(c))
    val builder =
      schema
        .fields
        .foldLeft(ParquetSchema.builder("schema")) { (b, f) =>
          b.optional(f.name, parquetTypeOf(f))
        }
    val pschema = builder.build()
    val writer  = Parquet.newBatchWriter(path, pschema, writeOptions)
    try
      val fields = pschema.columns
      batches.foreach { b =>
        if b.size > 0 then
          writer.write(
            ColumnBatch(
              b.size,
              schema
                .fields
                .indices
                .map(i => columnOf(fields(i), schema(i).ptype, b.column(i)))
                .toIndexedSeq
            )
          )
      }
      writer.close()
    catch
      case e: Throwable =>
        writer.abort()
        throw e

  end writeParquet

  private def compressionOf(c: String): CompressionCodec =
    c match
      case "uncompressed" | "none" =>
        CompressionCodec.UNCOMPRESSED
      case "snappy" =>
        CompressionCodec.SNAPPY
      case "gzip" =>
        CompressionCodec.GZIP
      case "zstd" =>
        CompressionCodec.ZSTD
      case "brotli" =>
        CompressionCodec.BROTLI
      case "lz4" =>
        CompressionCodec.LZ4_RAW
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported Parquet compression: ${other}")

  private def parquetTypeOf(f: Field): ParquetType =
    f.ptype match
      case PType.Bool =>
        ParquetType.BOOLEAN
      case PType.Int32 =>
        ParquetType.INT32
      case PType.Int64 =>
        ParquetType.INT64
      case PType.Float64 =>
        ParquetType.DOUBLE
      case PType.Str | PType.Null =>
        ParquetType.STRING
      case PType.Date =>
        ParquetType.DATE
      case PType.Timestamp =>
        // Engine timestamps have no time zone: not adjusted to UTC, as DuckDB writes TIMESTAMP
        ParquetType.Primitive(
          PhysicalType.INT64,
          Some(LogicalType.Timestamp(false, TimeUnit.MICROS)),
          0
        )
      case PType.Decimal(p, s) =>
        ParquetType.decimal(p, s)
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"Column ${f.name} of type ${other} can't be written to Parquet by the wvlet engine yet"
          )

  private def nullsOf(v: Vec, n: Int): Array[Boolean] = Array.tabulate(n)(v.isNull)

  // An engine vector as a uni column vector of the Parquet field
  private def columnOf(field: ParquetField, ptype: PType, v: Vec): ColumnVector =
    val n     = v.size
    val nulls = nullsOf(v, n)
    (ptype, v) match
      case (PType.Bool, b: BoolVec) =>
        ColumnVector.BooleanColumn(field, b.values, nulls)
      case (PType.Int32 | PType.Date, i: IntVec) =>
        ColumnVector.IntColumn(field, i.values, nulls)
      case (PType.Int64 | PType.Timestamp, l: LongVec) =>
        ColumnVector.LongColumn(field, l.values, nulls)
      case (PType.Float64, d: DoubleVec) =>
        ColumnVector.DoubleColumn(field, d.values, nulls)
      case (d: PType.Decimal, _) =>
        decimalColumn(field, d, v, nulls)
      case (PType.Bool, _) =>
        ColumnVector.BooleanColumn(
          field,
          Array.tabulate(n)(i => !nulls(i) && ValueOps.toBoolean(v.get(i))),
          nulls
        )
      case (PType.Int32 | PType.Date, _) =>
        ColumnVector.IntColumn(
          field,
          Array.tabulate(n)(i =>
            if nulls(i) then
              0
            else
              ValueOps.toInt(v.get(i))
          ),
          nulls
        )
      case (PType.Int64 | PType.Timestamp, _) =>
        ColumnVector.LongColumn(
          field,
          Array.tabulate(n)(i =>
            if nulls(i) then
              0L
            else
              ValueOps.toLong(v.get(i))
          ),
          nulls
        )
      case (PType.Float64, _) =>
        ColumnVector.DoubleColumn(
          field,
          Array.tabulate(n)(i =>
            if nulls(i) then
              0.0
            else
              ValueOps.toDouble(v.get(i))
          ),
          nulls
        )
      case _ =>
        binaryColumn(
          field,
          Array.tabulate(n)(i =>
            if nulls(i) then
              null
            else
              ValueOps.toStr(v.get(i)).getBytes(StandardCharsets.UTF_8)
          ),
          nulls,
          asString = true
        )
    end match

  end columnOf

  private def decimalColumn(
      field: ParquetField,
      d: PType.Decimal,
      v: Vec,
      nulls: Array[Boolean]
  ): ColumnVector =
    val n     = v.size
    val bound = java.math.BigInteger.TEN.pow(d.precision)
    // Unscaled values; a value wider than the declared precision is an error, not a truncation
    def unscaled(i: Int): java.math.BigInteger =
      val u =
        v match
          case l: LongVec if l.ptype == d =>
            java.math.BigInteger.valueOf(l.values(i))
          case _ =>
            ValueOps.rescale(ValueOps.toBigDecimal(v.get(i)), d.scale).unscaledValue()
      if u.abs().compareTo(bound) >= 0 then
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(
            s"Value ${JBigDecimal(u, d.scale).toPlainString} doesn't fit ${d.name} of column ${field
                .name}"
          )
      u
    val (physical, typeLength) =
      field match
        case p: ParquetField.Primitive =>
          (p.physicalType, p.typeLength)
        case _ =>
          (PhysicalType.FIXED_LEN_BYTE_ARRAY, 16)
    physical match
      case PhysicalType.INT32 =>
        ColumnVector.IntColumn(
          field,
          Array.tabulate(n)(i =>
            if nulls(i) then
              0
            else
              unscaled(i).intValue()
          ),
          nulls
        )
      case PhysicalType.INT64 =>
        ColumnVector.LongColumn(
          field,
          Array.tabulate(n)(i =>
            if nulls(i) then
              0L
            else
              unscaled(i).longValue()
          ),
          nulls
        )
      case _ =>
        // Big-endian two's complement, sign-extended to the fixed length of the field
        val len = typeLength
        binaryColumn(
          field,
          Array.tabulate(n) { i =>
            if nulls(i) then
              null
            else
              val b   = unscaled(i).toByteArray
              val out =
                Array.fill[Byte](len)(
                  if b(0) < 0 then
                    -1
                  else
                    0
                )
              System.arraycopy(b, 0, out, len - b.length, b.length)
              out
          },
          nulls,
          asString = false
        )
    end match

  end decimalColumn

  private def binaryColumn(
      field: ParquetField,
      values: Array[Array[Byte]],
      nulls: Array[Boolean],
      asString: Boolean
  ): ColumnVector =
    val offsets = new Array[Int](values.length + 1)
    var total   = 0
    var i       = 0
    while i < values.length do
      offsets(i) = total
      if values(i) != null then
        total += values(i).length
      i += 1
    offsets(values.length) = total
    val data = new Array[Byte](total)
    i = 0
    while i < values.length do
      if values(i) != null then
        System.arraycopy(values(i), 0, data, offsets(i), values(i).length)
      i += 1
    ColumnVector.BinaryColumn(field, offsets, data, nulls, asString)

  // --- CSV / TSV ----------------------------------------------------------------------------

  private def delimitedChunks(
      schema: Schema,
      batches: Seq[Batch],
      opts: Map[String, Any],
      defaultDelimiter: String,
      withHeader: Boolean
  ): Iterator[String] =
    opts
      .keys
      .filterNot(Set("format", "header", "delimiter", "delim", "sep").contains)
      .foreach { k =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Unsupported CSV write option for the wvlet engine: ${k}")
      }
    val delimiter = opts
      .get("delimiter")
      .orElse(opts.get("delim"))
      .orElse(opts.get("sep"))
      .map(_.toString)
      .getOrElse(defaultDelimiter)
    val header = withHeader && opts.get("header").forall(h => ValueOps.toBoolean(h))
    // Empty strings are quoted to stay distinct from NULL, which is written as nothing
    def quote(s: String): String =
      if s.isEmpty || s.contains(delimiter) || s.exists(c => c == '"' || c == '\n' || c == '\r')
      then
        "\"" + s.replace("\"", "\"\"") + "\""
      else
        s
    val types      = schema.types
    val headerLine = Option.when(header)(schema.names.map(quote).mkString(delimiter) + "\n")
    headerLine.iterator ++
      batches
        .iterator
        .map { b =>
          val sb = StringBuilder()
          var r  = 0
          while r < b.size do
            var c = 0
            while c < b.numColumns do
              if c > 0 then
                sb.append(delimiter)
              val v = b.column(c).get(r)
              if v != null then
                sb.append(quote(textOf(v, types(c))))
              c += 1
            sb.append('\n')
            r += 1
          sb.result()
        }

  end delimitedChunks

  // A CSV cell: arrays and maps as JSON, other values as their SQL text
  private def textOf(v: Any, ptype: PType): String =
    v match
      case _: Seq[?] | _: Map[?, ?] =>
        val sb = StringBuilder()
        appendJson(sb, v, ptype)
        sb.result()
      case _ =>
        ValueOps.toStr(ValueOps.toExternal(v, ptype))

  // --- JSON lines ---------------------------------------------------------------------------

  private def jsonLines(schema: Schema, b: Batch): String =
    val sb    = StringBuilder()
    val names = schema.names.map(jsonString)
    val types = schema.types
    locally {
      var r = 0
      while r < b.size do
        sb.append('{')
        var c = 0
        while c < b.numColumns do
          if c > 0 then
            sb.append(',')
          sb.append(names(c)).append(':')
          appendJson(sb, b.column(c).get(r), types(c))
          c += 1
        sb.append("}\n")
        r += 1
    }
    sb.result()

  private def appendJson(sb: StringBuilder, v: Any, ptype: PType): Unit =
    v match
      case null =>
        sb.append("null")
      case _ if ptype == PType.Date || ptype == PType.Timestamp =>
        sb.append(jsonString(ValueOps.toStr(ValueOps.toExternal(v, ptype))))
      case b: Boolean =>
        sb.append(b)
      case d: Double =>
        if d.isNaN || d.isInfinite then
          sb.append("null")
        else
          sb.append(ValueOps.formatDouble(d))
      case n: (Int | Long | Short | Byte) =>
        sb.append(n.toString)
      case d: JBigDecimal =>
        sb.append(d.toPlainString)
      case s: Seq[?] =>
        sb.append('[')
        s.zipWithIndex
          .foreach { (x, i) =>
            if i > 0 then
              sb.append(',')
            appendJson(sb, x, PType.AnyT)
          }
        sb.append(']')
      case m: Map[?, ?] =>
        sb.append('{')
        m.zipWithIndex
          .foreach { case ((k, x), i) =>
            if i > 0 then
              sb.append(',')
            sb.append(jsonString(k.toString)).append(':')
            appendJson(sb, x, PType.AnyT)
          }
        sb.append('}')
      case other =>
        sb.append(jsonString(ValueOps.toStr(other)))

  private def jsonString(s: String): String =
    val sb = StringBuilder("\"")
    s.foreach {
      case '"' =>
        sb.append("\\\"")
      case '\\' =>
        sb.append("\\\\")
      case '\n' =>
        sb.append("\\n")
      case '\r' =>
        sb.append("\\r")
      case '\t' =>
        sb.append("\\t")
      case c if c < ' ' =>
        sb.append(f"\\u${c.toInt}%04x")
      case c =>
        sb.append(c)
    }
    sb.append('"').result()

end DataFileWriter
