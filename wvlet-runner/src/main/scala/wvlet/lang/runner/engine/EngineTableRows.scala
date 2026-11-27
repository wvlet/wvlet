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
package wvlet.lang.runner.engine

import wvlet.lang.compiler.Name
import wvlet.lang.engine.EngineResult
import wvlet.lang.model.DataType
import wvlet.lang.runner.TableRows

import scala.collection.immutable.ListMap

/**
  * Converts an engine result into the runner's [[TableRows]], so `test` assertions, printers, and
  * the web UI read engine results like any connector's
  */
object EngineTableRows:

  /** The rows of `result`, at most `rowLimit` of them */
  def of(result: EngineResult, rowLimit: Int): TableRows =
    val schema = DataType.SchemaType(
      None,
      Name.NoTypeName,
      result
        .schema
        .fields
        .map(f => DataType.NamedType(Name.termName(f.name), f.ptype.toDataType))
        .toList
    )
    val names = result.schema.names
    if names.distinct.size < names.size then
      // Duplicate column names collapse in a row map: keep the positional rows too
      val rows = result.toExternalRows(rowLimit)
      val maps = rows.map(r => ListMap.from(names.indices.map(i => names(i) -> r(i))))
      TableRows(schema, maps, result.numRows, Some(rows))
    else
      TableRows(schema, result.toRowMaps(rowLimit), result.numRows)

end EngineTableRows
