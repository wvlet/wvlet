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
package wvlet.lang.compiler

enum DBType(
    // CREATE OR REPLACE is supported
    val supportCreateOrReplace: Boolean = false,

    // True if describe ... can be nested like select * (describe select ...)
    val supportDescribeSubQuery: Boolean = false,
    val supportSaveAsFile: Boolean = false,
    // CREATE TABLE ... WITH (options...) is supported
    val supportCreateTableWithOption: Boolean = false
):

  case DuckDB
      extends DBType(
        supportCreateOrReplace = true,
        supportDescribeSubQuery = true,
        supportSaveAsFile = true
      )

  case Trino
      extends DBType(
        // Note: Trino connector may support `create or replace table` depending on the connector.
        supportCreateOrReplace = false,
        supportCreateTableWithOption = true
      )

  case Hive       extends DBType
  case BigQuery   extends DBType
  case MySQL      extends DBType
  case PostgreSQL extends DBType
  case SQLite     extends DBType
  case Redshift   extends DBType
  case Snowflake  extends DBType
  case ClickHouse extends DBType
  case Oracle     extends DBType
  case SQLServer  extends DBType
  case InMemory   extends DBType
  case Generic    extends DBType

end DBType

object DBType:

  private val dbTypeMap = DBType.values.map(x => x.toString.toLowerCase -> x).toMap

  def fromString(name: String): DBType =
    dbTypeMap.get(name.toLowerCase) match
      case Some(t) =>
        t
      case None =>
        // Fallback to generic
        Generic

end DBType
