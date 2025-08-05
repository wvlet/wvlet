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

import wvlet.lang.compiler.SQLDialect.MapSyntax.KeyValue

enum DBType(
    // CREATE OR REPLACE is supported
    val supportCreateOrReplace: Boolean = false,

    // True if describe ... can be nested like select * (describe select ...)
    val supportDescribeSubQuery: Boolean = false,
    val supportSaveAsFile: Boolean = false,
    // CREATE TABLE ... WITH (options...) is supported
    val supportCreateTableWithOption: Boolean = false,
    val supportStructExpr: Boolean = false,
    val supportRowExpr: Boolean = false,
    val supportAsOfJoin: Boolean = false,
    val arrayConstructorSyntax: SQLDialect.ArraySyntax = SQLDialect.ArraySyntax.ArrayLiteral,
    // MAP {key: value, ...} syntax or MAP(ARRAY[k1, k2, ...], ARRAY[v1, v2, ...]) syntax
    val mapConstructorSyntax: SQLDialect.MapSyntax = KeyValue,
    // values 1, 2, ...   or (values 1, 2, ...)
    val requireParenForValues: Boolean = false,
    // IF(condition, true_value[, false_value]) function is supported
    val supportIfFunction: Boolean = false
):

  case DuckDB
      extends DBType(
        supportCreateOrReplace = true,
        supportDescribeSubQuery = true,
        supportSaveAsFile = true,
        supportStructExpr = true,
        supportAsOfJoin = true,
        mapConstructorSyntax = SQLDialect.MapSyntax.KeyValue,
        supportIfFunction = true
      )

  case Trino
      extends DBType(
        // Note: Trino connector may support `create or replace table` depending on the connector.
        supportCreateOrReplace = false,
        supportCreateTableWithOption = true,
        supportRowExpr = true,
        arrayConstructorSyntax = SQLDialect.ArraySyntax.ArrayPrefix,
        mapConstructorSyntax = SQLDialect.MapSyntax.ArrayPair,
        requireParenForValues = true
      )

  case Hive
      extends DBType(
        supportCreateOrReplace = false,
        supportCreateTableWithOption = true,
        supportStructExpr = true,
        supportRowExpr = false,
        arrayConstructorSyntax = SQLDialect.ArraySyntax.ArrayPrefix,
        mapConstructorSyntax = SQLDialect.MapSyntax.ArrayPair,
        requireParenForValues = false
      )

  case BigQuery   extends DBType(supportIfFunction = true)
  case MySQL      extends DBType(supportIfFunction = true)
  case PostgreSQL extends DBType
  case SQLite     extends DBType(supportIfFunction = true)
  case Redshift   extends DBType
  case Snowflake  extends DBType
  case ClickHouse extends DBType(supportIfFunction = true)
  case Oracle     extends DBType
  case SQLServer  extends DBType
  case InMemory
      extends DBType(
        // Basically same with DuckDB
        supportCreateOrReplace = true,
        supportDescribeSubQuery = true,
        supportSaveAsFile = true,
        supportStructExpr = true,
        supportAsOfJoin = true,
        mapConstructorSyntax = SQLDialect.MapSyntax.KeyValue,
        supportIfFunction = true
      )

  case Generic extends DBType

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

object SQLDialect:

  enum ArraySyntax:
    // ARRAY[1, 2, 3] syntax
    case ArrayPrefix
    // [1, 2, 3] syntax
    case ArrayLiteral

  enum MapSyntax:
    // MAP {key: value, ...} syntax
    case KeyValue
    // MAP(ARRAY[k1, k2, ...], ARRAY[v1, v2, ...]) syntax
    case ArrayPair

end SQLDialect
