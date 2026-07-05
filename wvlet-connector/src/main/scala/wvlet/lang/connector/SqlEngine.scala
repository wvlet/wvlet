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
package wvlet.lang.connector

import wvlet.lang.catalog.Catalog
import wvlet.lang.catalog.SQLFunction
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.query.QueryProgressMonitor

/**
  * SQL execution capability of a [[Connector]]. [[DBConnector]] is the base implementation for the
  * built-in engines (DuckDB, Trino, Snowflake).
  *
  * Default `using` arguments are declared here so that implementations don't redeclare them.
  */
trait SqlEngine extends AutoCloseable:
  /** SQL dialect of this engine, used by the SQL generator */
  def dbType: DBType

  def execute(sql: String)(using
      progressMonitor: QueryProgressMonitor = QueryProgressMonitor.noOp
  ): Boolean

  def executeUpdate(sql: String)(using
      progressMonitor: QueryProgressMonitor = QueryProgressMonitor.noOp
  ): Int

  def getCatalog(catalogName: String, defaultSchema: String): Catalog

  def listFunctions(catalog: String): List[SQLFunction]
