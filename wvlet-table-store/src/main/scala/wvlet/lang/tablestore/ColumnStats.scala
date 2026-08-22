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
package wvlet.lang.tablestore

import wvlet.lang.tablestore.catalog.PendingColumnStat
import wvlet.lang.tablestore.schema.{ColumnType, TableSchema}
import wvlet.uni.json.JSON

/**
  * Collects per-column min/max/null statistics from rows held in memory. Both write paths emit
  * stats almost for free: the ingest writer has the rows at rotation and the merger reads
  * everything anyway.
  *
  * Stats are advisory pruning metadata: min/max are canonically encoded per type (round-tripping
  * through [[wvlet.lang.tablestore.read.Pruning.decode]]), and a missing row means "must scan".
  */
object ColumnStats:

  /** Compute stats for every column of `schema` over the given rows */
  def collect(rows: Seq[DataRow], schema: TableSchema): Seq[PendingColumnStat] = schema
    .columns
    .map { col =>
      val values   = rows.flatMap(row => row.get(col.name))
      val nulls    = rows.count(row => row.get(col.name).forall(_.isInstanceOf[JSON.JSONNull]))
      val nonNulls = values.filterNot(_.isInstanceOf[JSON.JSONNull])
      val t        = col.columnType

      val encodedMin: Option[String] =
        if t == ColumnType.NullType || nonNulls.isEmpty then
          None
        else
          Some(encodeValue(t, minValue(t, nonNulls)))
      val encodedMax: Option[String] =
        if t == ColumnType.NullType || nonNulls.isEmpty then
          None
        else
          Some(encodeValue(t, maxValue(t, nonNulls)))

      PendingColumnStat(
        columnName = col.name,
        minValue = encodedMin,
        maxValue = encodedMax,
        nullCount = nulls.toLong,
        distinctEstimate = None
      )
    }

  private def comparable(t: ColumnType, v: JSON.JSONValue): Comparable[?] =
    (t, v) match
      case (ColumnType.LongType, JSON.JSONLong(x)) =>
        java.lang.Long.valueOf(x)
      case (ColumnType.DoubleType, JSON.JSONDouble(x)) =>
        java.lang.Double.valueOf(x)
      case (ColumnType.StringType, JSON.JSONString(x)) =>
        x
      case (_, JSON.JSONString(x)) =>
        x
      case (_, JSON.JSONLong(x)) =>
        java.lang.Long.valueOf(x)
      case (_, JSON.JSONDouble(x)) =>
        java.lang.Double.valueOf(x)
      case _ =>
        throw TableStoreException(s"Cannot order value ${JSON.format(v)} as ${t}")

  private def minValue(t: ColumnType, vs: Seq[JSON.JSONValue]): JSON.JSONValue = vs.minBy(
    comparable(t, _).asInstanceOf[Comparable[AnyRef]]
  )

  private def maxValue(t: ColumnType, vs: Seq[JSON.JSONValue]): JSON.JSONValue = vs.maxBy(
    comparable(t, _).asInstanceOf[Comparable[AnyRef]]
  )

  /** Canonical per-type encoding of stat bounds */
  def encodeValue(t: ColumnType, v: JSON.JSONValue): String =
    v match
      case JSON.JSONLong(x) =>
        x.toString
      case JSON.JSONDouble(x) =>
        x.toString
      case JSON.JSONString(x) =>
        x
      case JSON.JSONBoolean(x) =>
        x.toString
      case other =>
        other.toJSON

end ColumnStats
