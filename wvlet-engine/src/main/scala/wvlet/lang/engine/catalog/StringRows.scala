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

import wvlet.lang.engine.vector.*
import wvlet.lang.model.DataType

/**
  * Rows of another engine's result as typed columns with text values (`None` for NULL), the shape
  * Trino pages and JDBC `getString` produce. Converts them to engine batches, e.g. to write a local
  * file with [[DataFileWriter]]
  */
object StringRows:

  /**
    * The engine schema for columns of a source result. Types the engine can't hold as a scalar
    * (nested types, intervals, time zones) stay text, keeping the source engine's formatting
    */
  def schemaOf(columns: Seq[(String, DataType)]): Schema = Schema(
    columns
      .map { (name, dataType) =>
        Field(name, ptypeOf(dataType))
      }
      .toIndexedSeq
  )

  private def ptypeOf(dataType: DataType): PType =
    dataType match
      case t: DataType.TimestampType if t.withTimeZone =>
        // Zone names (e.g. `2026-01-01 00:00:00 UTC`) are not parsed by the engine's timestamps
        PType.Str
      case n: DataType.NamedType =>
        ptypeOf(n.dataType)
      case other =>
        PType.fromDataType(other) match
          case PType.AnyT | PType.Null =>
            PType.Str
          case p =>
            p

  /** A batch of `rows` laid out as `schema` (from [[schemaOf]]) */
  def toBatch(schema: Schema, rows: Seq[Seq[Option[String]]]): Batch =
    val cols = schema
      .fields
      .indices
      .map { i =>
        val ptype = schema(i).ptype
        val b     = VecBuilder(ptype, rows.size)
        rows.foreach { row =>
          row(i) match
            case None =>
              b.addNull()
            case Some(v) if ptype == PType.Str =>
              b.add(v)
            case Some(v) =>
              b.add(ValueOps.cast(v, PType.Str, ptype))
        }
        b.result()
      }
    Batch(cols.toArray, rows.size)

end StringRows
