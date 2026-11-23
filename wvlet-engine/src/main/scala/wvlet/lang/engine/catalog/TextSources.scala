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

import wvlet.lang.compiler.SourceIO
import wvlet.lang.engine.vector.*
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.*

import scala.collection.immutable.ListMap
import scala.collection.mutable

/** Infers column types from sampled values (DuckDB-like: integers, doubles, booleans, strings) */
private[catalog] object TypeInference:
  def typeOf(v: Any): PType =
    v match
      case null =>
        PType.Null
      case _: Boolean =>
        PType.Bool
      case _: Long | _: Int =>
        PType.Int64
      case _: Double =>
        PType.Float64
      case _: String =>
        PType.Str
      case _ =>
        PType.AnyT

  def merge(a: PType, b: PType): PType =
    (a, b) match
      case (x, y) if x == y =>
        x
      case (PType.Null, y) =>
        y
      case (x, PType.Null) =>
        x
      case (PType.Int64, PType.Float64) | (PType.Float64, PType.Int64) =>
        PType.Float64
      case (PType.Str, _) | (_, PType.Str) =>
        PType.Str
      case _ =>
        PType.AnyT

  def build(name: String, rows: Seq[Map[String, Any]], columns: Seq[String]): (Schema, Batch) =
    val types = columns.map { c =>
      val t =
        rows.iterator.map(r => typeOf(r.getOrElse(c, null))).foldLeft(PType.Null: PType)(merge)
      if t == PType.Null then
        PType.Str
      else
        t
    }
    val schema = Schema(
      columns.indices.map(i => Field(columns(i), types(i), List(name))).toIndexedSeq
    )
    val cols = columns
      .indices
      .map { i =>
        val b = VecBuilder(types(i), rows.size)
        rows.foreach { r =>
          val v = r.getOrElse(columns(i), null)
          if v == null then
            b.addNull()
          else if types(i) == PType.Str then
            b.add(
              v match
                case s: String =>
                  s
                case other =>
                  jsonText(other)
            )
          else
            b.add(v)
        }
        b.result()
      }
    (schema, Batch(cols.toArray, rows.size))

  end build

  /** A nested value (list, map) as JSON text */
  def jsonText(v: Any): String = JSON.format(toJSON(v))

  private def toJSON(v: Any): JSONValue =
    v match
      case null =>
        JSONNull()
      case b: Boolean =>
        JSONBoolean(b)
      case l: Long =>
        JSONLong(l)
      case d: Double =>
        JSONDouble(d)
      case s: String =>
        JSONString(s)
      case s: Seq[?] =>
        JSONArray(s.map(toJSON).toIndexedSeq)
      case m: Map[?, ?] =>
        JSONObject(m.toSeq.map((k, x) => k.toString -> toJSON(x)))
      case other =>
        JSONString(other.toString)

end TypeInference

/** Rows given as JSON objects, e.g. the rows a source connector streams */
object JsonRows:

  /**
    * An in-memory table of JSON object `lines`. Columns follow `schema` when given (values are
    * converted to its column types, and extra keys are ignored); otherwise names and types are
    * inferred from the rows as for JSON files
    */
  def toMemTable(name: String, lines: Iterator[String], schema: Option[Schema] = None): MemTable =
    val rows = lines.map(_.trim).filter(_.nonEmpty).map(l => toRow(JSON.parse(l))).toIndexedSeq
    schema match
      case Some(s) =>
        val fields = s.fields.map(f => Field(f.name, f.ptype, List(name)))
        val cols   = fields.map { f =>
          val b = VecBuilder(f.ptype, rows.size)
          rows.foreach { r =>
            r.getOrElse(f.name, null) match
              case null =>
                b.addNull()
              case v: String if f.ptype != PType.Str && f.ptype != PType.AnyT =>
                b.add(ValueOps.cast(v, PType.Str, f.ptype))
              case v if f.ptype == PType.Str && !v.isInstanceOf[String] =>
                b.add(TypeInference.jsonText(v))
              case v =>
                b.add(v)
          }
          b.result()
        }
        MemTable(name, Schema(fields), Seq(Batch(cols.toArray, rows.size)))
      case None =>
        val columns = mutable.LinkedHashSet.empty[String]
        rows.foreach(r => columns ++= r.keys)
        val (s, b) = TypeInference.build(name, rows, columns.toSeq)
        MemTable(name, s, Seq(b))

  end toMemTable

  private[catalog] def toRow(v: JSONValue): Map[String, Any] =
    v match
      case o: JSONObject =>
        ListMap.from(o.v.map((k, x) => k -> toValue(x)))
      case other =>
        ListMap("value" -> toValue(other))

  private def toValue(v: JSONValue): Any =
    v match
      case _: JSONNull =>
        null
      case b: JSONBoolean =>
        b.v
      case l: JSONLong =>
        l.v
      case d: JSONDouble =>
        d.v
      case s: JSONString =>
        s.v
      case a: JSONArray =>
        a.v.map(toValue).toList
      case o: JSONObject =>
        ListMap.from(o.v.map((k, x) => k -> toValue(x)))
      case other =>
        other.toString

end JsonRows

/** JSON array, JSON object, or JSON Lines file (optionally gzipped) */
class JsonSource(val name: String, path: String) extends TableSource:
  override def key: Any = ("json", path)

  private lazy val loaded: (Schema, Batch) =
    val bytes =
      if path.endsWith(".gz") then
        SourceIO.readGzipAsBytes(path)
      else
        SourceIO.readAsBytes(path)
    val text                   = String(bytes, "UTF-8").trim
    val values: Seq[JSONValue] =
      if text.startsWith("[") then
        JSON.parse(text) match
          case a: JSONArray =>
            a.v
          case other =>
            Seq(other)
      else if text.startsWith("{") && !text.contains("\n{") then
        Seq(JSON.parse(text))
      else
        text.split("\n").iterator.map(_.trim).filter(_.nonEmpty).map(JSON.parse).toSeq
    val rows    = values.map(JsonRows.toRow)
    val columns = mutable.LinkedHashSet.empty[String]
    rows.foreach(r => columns ++= r.keys)
    TypeInference.build(name, rows, columns.toSeq)

  def schema: Schema = loaded._1

  override def estimatedRows: Option[Long] = Some(loaded._2.size.toLong)

  def scan(spec: ScanSpec): Iterator[Batch] =
    val (s, b) = loaded
    spec.columns match
      case None =>
        Iterator(b)
      case Some(cols) =>
        Iterator(b.select(cols.map(c => s.names.indexOf(c)).toArray))

end JsonSource

/** CSV / TSV file with a header row; column types are inferred from the values */
class CsvSource(val name: String, path: String) extends TableSource:
  override def key: Any = ("csv", path)

  private val delimiter: Char =
    if path.toLowerCase.contains(".tsv") then
      '\t'
    else
      ','

  private lazy val loaded: (Schema, Batch) =
    val bytes =
      if path.endsWith(".gz") then
        SourceIO.readGzipAsBytes(path)
      else
        SourceIO.readAsBytes(path)
    val records = CsvSource.parse(String(bytes, "UTF-8"), delimiter)
    if records.isEmpty then
      (Schema.empty, Batch.ofRows(0))
    else
      val header = records.head.toIndexedSeq
      val rows   = records
        .tail
        .filter(r => r.nonEmpty && !(r.size == 1 && r.head.isEmpty))
        .map { r =>
          ListMap.from(header.indices.map(i => header(i) -> CsvSource.typed(r.lift(i).orNull)))
        }
      TypeInference.build(name, rows, header)

  def schema: Schema = loaded._1

  override def estimatedRows: Option[Long] = Some(loaded._2.size.toLong)

  def scan(spec: ScanSpec): Iterator[Batch] =
    val (s, b) = loaded
    spec.columns match
      case None =>
        Iterator(b)
      case Some(cols) =>
        Iterator(b.select(cols.map(c => s.names.indexOf(c)).toArray))

end CsvSource

object CsvSource:
  /** Interpret an unquoted CSV cell: empty as NULL, then integer, double, boolean, or string */
  def typed(s: String): Any =
    if s == null || s.isEmpty then
      null
    else if s.matches("-?\\d{1,18}") then
      s.toLong
    else if s.matches("-?(\\d+\\.\\d*|\\.\\d+)([eE][-+]?\\d+)?") then
      s.toDouble
    else if s.equalsIgnoreCase("true") then
      true
    else if s.equalsIgnoreCase("false") then
      false
    else
      s

  /** RFC 4180 records */
  def parse(text: String, delimiter: Char): Seq[Seq[String]] =
    val records = Seq.newBuilder[Seq[String]]
    var fields  = mutable.ArrayBuffer.empty[String]
    val cur     = StringBuilder()
    var inQuote = false
    var i       = 0
    while i < text.length do
      val c = text.charAt(i)
      if inQuote then
        if c == '"' then
          if i + 1 < text.length && text.charAt(i + 1) == '"' then
            cur.append('"')
            i += 1
          else
            inQuote = false
        else
          cur.append(c)
      else if c == '"' then
        inQuote = true
      else if c == delimiter then
        fields += cur.toString
        cur.clear()
      else if c == '\n' || c == '\r' then
        if c == '\r' && i + 1 < text.length && text.charAt(i + 1) == '\n' then
          i += 1
        fields += cur.toString
        cur.clear()
        records += fields.toSeq
        fields = mutable.ArrayBuffer.empty[String]
      else
        cur.append(c)
      i += 1
    if cur.nonEmpty || fields.nonEmpty then
      fields += cur.toString
      records += fields.toSeq
    records.result()

  end parse

end CsvSource
