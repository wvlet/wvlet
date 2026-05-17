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
package wvlet.lang.compiler.connector

import wvlet.lang.model.DataType.NamedType

/**
  * Cross-platform query result shape. Values are kept as `Option[String]` so backends don't have to
  * agree on a typed value representation up front — the JDBC, libduckdb, and koffi-FFI backends
  * each call the engine's string-coercion (`duckdb_value_varchar` / `ResultSet.getString`) and hand
  * us the same surface. `None` represents SQL `NULL`.
  *
  * Good enough for a CLI runner that prints to stdout / json / a box-drawn table. If we later need
  * typed access (date arithmetic, decimal math) we'd switch to a `Seq[Any]` plus the existing wvlet
  * `DataType` machinery — for now strings keep all backends consistent.
  */
case class QueryResultRow(values: Seq[Option[String]])

case class QueryResult(columns: Seq[NamedType], rows: Seq[QueryResultRow]):
  def rowCount: Int    = rows.size
  def columnCount: Int = columns.size
