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

import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.vector.*
import wvlet.uni.cloud.parquet.ParquetFilter

/**
  * Reads a table source. `schema` is the output schema (source columns, possibly renamed or
  * requalified); `columns` are the source column positions to read (all when None); `filter` lets
  * the source skip data that can't match (see [[ScanSpec]]), as do the `runtimeFilters` that hash
  * joins above set once their builds are done
  */
class ScanOp(
    val source: TableSource,
    val schema: Schema,
    val columns: Option[IndexedSeq[Int]],
    val filter: Option[ParquetFilter],
    val runtimeFilters: Seq[RuntimeFilter] = Nil
) extends Operator:
  override def name: String              = s"Scan[${source.name}]"
  override protected def details: String =
    s"(${columns
        .map(c => s"columns=${c.map(source.schema(_).name).mkString(",")}")
        .getOrElse("")}${filter.map(f => s" filter=${f}").getOrElse("")}${
        if runtimeFilters.isEmpty then
          ""
        else
          s" join_filter=${runtimeFilters.map(_.column).distinct.mkString(",")}"
      })"

  override protected def extraInputs(indent: Int): Seq[String] = source.explainDetail(indent).toSeq

  // A breaker's plan is not a child (the source drains and closes it), but walks must reach it
  override def sharedInputs: Seq[Operator] =
    source match
      case b: BreakerSource =>
        Seq(b.op)
      case _ =>
        Nil

  /** The scan request this operator sends to the source */
  def spec: ScanSpec = ScanSpec(
    columns.map(_.map(source.schema(_).name)),
    (filter ++ runtimeFilters.flatMap(_.filter)).reduceOption(_ && _)
  )

  private lazy val it: Iterator[Batch] = source.scan(spec)

  /** The source column name of output position i */
  def sourceColumnName(i: Int): String = source.schema(columns.map(_(i)).getOrElse(i)).name

  def withSchema(s: Schema): ScanOp = ScanOp(source, s, columns, filter, runtimeFilters)

  /** Read only the given output positions */
  def project(idx: IndexedSeq[Int]): ScanOp =
    val base = columns.getOrElse(source.schema.fields.indices)
    ScanOp(source, Schema(idx.map(schema(_))), Some(idx.map(base)), filter, runtimeFilters)

  def withFilter(f: ParquetFilter): ScanOp = ScanOp(
    source,
    schema,
    columns,
    Some(filter.map(_ && f).getOrElse(f)),
    runtimeFilters
  )

  def next(): Batch =
    var b: Batch = null
    while b == null && it.hasNext do
      val x = it.next()
      if x.size > 0 then
        b = x
    b

end ScanOp
