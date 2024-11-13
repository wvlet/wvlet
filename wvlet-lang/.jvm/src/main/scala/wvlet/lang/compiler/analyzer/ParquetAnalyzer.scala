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
package wvlet.lang.compiler.analyzer

import org.duckdb.DuckDBConnection
import wvlet.airframe.control.Control
import wvlet.airframe.control.Control.withResource
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Name
import wvlet.lang.model.DataType.{EmptyRelationType, NamedType, SchemaType}
import wvlet.lang.model.{DataType, RelationType}

import java.io.File
import java.sql.DriverManager

object ParquetAnalyzer:

  private def withConnection[U](f: DuckDBConnection => U): U =
    Class.forName("org.duckdb.DuckDBDriver")
    DriverManager.getConnection("jdbc:duckdb:") match
      case conn: DuckDBConnection =>
        try f(conn)
        finally conn.close()
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException("duckdb connection is unavailable")

  def guessSchema(path: String): RelationType =
    if !new File(path).exists then
      EmptyRelationType
    else
      // Use DuckDB to analyze the schema of the Parquet file
      val sql = s"select * from '${path}' limit 0"

      withConnection { conn =>
        withResource(conn.createStatement().executeQuery(sql)) { rs =>
          val metadata = rs.getMetaData
          val columns = (1 to metadata.getColumnCount).map { i =>
            val name     = metadata.getColumnName(i)
            val dataType = metadata.getColumnTypeName(i).toLowerCase
            // TODO support non-primitive type parsing
            NamedType(Name.termName(name), DataType.parse(dataType))
          }
          SchemaType(None, Name.typeName(RelationType.newRelationTypeName), columns)
        }
      }

end ParquetAnalyzer
