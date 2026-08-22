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
package wvlet.lang.tablestore.schema

import wvlet.lang.tablestore.TableStoreException
import wvlet.uni.json.JSON

/**
  * The monotonic promotion lattice for schema escalation: `null -> long -> double -> string`, with
  * boolean as a leaf that widens to string (it has no numeric supertype). Escalation is
  * irreversible, so widening follows this lattice only — never narrowing.
  */
enum ColumnType:
  case NullType
  case BooleanType
  case LongType
  case DoubleType
  case StringType

object ColumnType:
  private val order: Map[ColumnType, Int] = Map(
    NullType    -> 0,
    BooleanType -> 1,
    LongType    -> 2,
    DoubleType  -> 3,
    StringType  -> 4
  )

  /** The least upper bound in the promotion lattice. Commutative and associative */
  def lub(a: ColumnType, b: ColumnType): ColumnType =
    if a == b then
      a
    else
      (a, b) match
        case (NullType, other) =>
          other
        case (other, NullType) =>
          other
        // Boolean and numerics are incomparable; both widen to string
        case (BooleanType, _) =>
          StringType
        case (_, BooleanType) =>
          StringType
        case (x, y) =>
          if order(x) < order(y) then
            y
          else
            x

  def fold(types: Seq[ColumnType]): ColumnType = types.foldLeft(NullType: ColumnType)(lub)

  def parse(s: String): ColumnType =
    s match
      case "null" =>
        NullType
      case "boolean" =>
        BooleanType
      case "long" =>
        LongType
      case "double" =>
        DoubleType
      case "string" =>
        StringType
      case other =>
        throw TableStoreException(s"Unknown column type in observed schema: ${other}")

end ColumnType

/**
  * The schema observed inside one data file at registration time. Columns are kept sorted by name
  * so that encoding — and therefore checksums and equality tests — is deterministic.
  */
case class ObservedSchema(columns: Seq[(String, ColumnType)]):
  def columnType(name: String): Option[ColumnType] = columns.collectFirst {
    case (n, t) if n == name =>
      t
  }

  def schemaJson: String = ObservedSchema.toJson(this)

object ObservedSchema:
  val empty: ObservedSchema = ObservedSchema(Nil)

  def apply(pairs: Seq[(String, ColumnType)]): ObservedSchema =
    // Deduplicate by column keeping the lub of duplicates, then sort by name
    val merged = pairs
      .groupBy(_._1)
      .view
      .mapValues(vs => ColumnType.fold(vs.map(_._2)))
      .toSeq
      .sortBy(_._1)
    new ObservedSchema(merged)

  /** Infer an observed schema from one JSONL row. Order-independent by construction */
  def fromRow(row: JSON.JSONObject): ObservedSchema = ObservedSchema(
    row.v.map((name, value) => name -> typeOf(value))
  )

  /** Fold row schemas into the schema of a whole file */
  def fromRows(rows: Seq[JSON.JSONObject]): ObservedSchema = ObservedSchema(
    rows
      .flatMap(_.v)
      .groupBy(_._1)
      .map((name, values) => name -> ColumnType.fold(values.map(v => typeOf(v._2))))
      .toSeq
  )

  def fromJson(json: String): ObservedSchema =
    if json == null || json.isEmpty then
      empty
    else
      JSON.parse(json) match
        case obj: JSON.JSONObject =>
          obj.get("columns") match
            case Some(JSON.JSONArray(items)) =>
              ObservedSchema(
                items.collect { case col: JSON.JSONObject =>
                  val name = col
                    .get("name")
                    .collect { case JSON.JSONString(v) =>
                      v
                    }
                  val typ = col
                    .get("type")
                    .collect { case JSON.JSONString(v) =>
                      v
                    }
                  (name, typ) match
                    case (Some(n), Some(t)) =>
                      n -> ColumnType.parse(t)
                    case _ =>
                      throw TableStoreException(
                        s"Malformed observed schema entry: ${JSON.format(col)}"
                      )
                }
              )
            case _ =>
              empty
        case _ =>
          throw wvlet.lang.tablestore.TableStoreException(s"Malformed observed schema: ${json}")

  def toJson(observed: ObservedSchema): String =
    val cols = observed
      .columns
      .map { (name, typ) =>
        JSON.JSONObject(
          Seq("name" -> JSON.JSONString(name), "type" -> JSON.JSONString(typeName(typ)))
        )
      }
    (JSON.JSONObject(Seq("columns" -> JSON.JSONArray(cols.toIndexedSeq)))).toJSON

  def typeName(t: ColumnType): String =
    t match
      case ColumnType.NullType =>
        "null"
      case ColumnType.BooleanType =>
        "boolean"
      case ColumnType.LongType =>
        "long"
      case ColumnType.DoubleType =>
        "double"
      case ColumnType.StringType =>
        "string"

  private def typeOf(value: JSON.JSONValue): ColumnType =
    value match
      case _: JSON.JSONNull =>
        ColumnType.NullType
      case _: JSON.JSONLong =>
        ColumnType.LongType
      case _: JSON.JSONDouble =>
        ColumnType.DoubleType
      case _: JSON.JSONBoolean =>
        ColumnType.BooleanType
      case _: JSON.JSONString =>
        ColumnType.StringType
      // Structured values are encoded as canonical JSON text until their widening rules are
      // specified by the promotion-lattice spec
      case _: JSON.JSONArray =>
        ColumnType.StringType
      case _: JSON.JSONObject =>
        ColumnType.StringType

end ObservedSchema
