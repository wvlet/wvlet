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
package wvlet.lang.embedded

import wvlet.lang.runner.TableRows
import wvlet.uni.weaver.Weaver

import scala.collection.immutable.ListMap

/**
  * A result column
  *
  * @param typeName
  *   the Wvlet type name, e.g. `long`, `double`, `string`, `decimal(15,2)`, `date`
  */
case class WvletEmbedColumn(name: String, typeName: String)

/**
  * Rows of a query run by [[WvletEmbed]]. Values are `java.lang.Long` for integers,
  * `java.lang.Double` for floating-point numbers, `java.lang.Boolean`, and `String` for strings;
  * decimals, dates, and timestamps are strings, so no precision is lost; arrays are `Seq`s and maps
  * `Map`s; NULL is `null`.
  */
case class WvletEmbedResult(
    columns: IndexedSeq[WvletEmbedColumn],
    rows: IndexedSeq[IndexedSeq[Any]]
):
  /** The number of rows */
  def rowCount: Int = rows.size

  /** The number of columns */
  def columnCount: Int = columns.size

  /** The name of the column at `index` (0-based) */
  def columnName(index: Int): String = columns(index).name

  /** The Wvlet type name of the column at `index` (0-based) */
  def columnType(index: Int): String = columns(index).typeName

  /** The value at (`row`, `column`), both 0-based; `null` for NULL */
  def get(row: Int, column: Int): Any = rows(row)(column)

  /**
    * The result as JSON: `{"columns":[{"name":..,"type":..},...],"rows":[[v1,v2,...],...]}`, with
    * values as described in [[WvletEmbedResult]]. The C and SDK bindings return this document
    */
  def toJson: String = WvletEmbedResult
    .jsonCodec
    .toJson(
      ListMap(
        "columns" -> columns.map(c => ListMap("name" -> c.name, "type" -> c.typeName)),
        "rows"    -> rows
      )
    )

end WvletEmbedResult

object WvletEmbedResult:
  private val jsonCodec = summon[Weaver[ListMap[String, Any]]]

  /** A result with no columns and no rows */
  val empty: WvletEmbedResult = WvletEmbedResult(IndexedSeq.empty, IndexedSeq.empty)

  private[embedded] def fromTableRows(t: TableRows): WvletEmbedResult =
    val columns =
      t.schema.fields.map(f => WvletEmbedColumn(f.name.name, f.dataType.wvExpr)).toIndexedSeq
    // Without repeated column names, each row map holds every column in order
    val rows = t.positionalRows.getOrElse(t.rows.map(_.values.toIndexedSeq)).toIndexedSeq
    WvletEmbedResult(columns, rows)
