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
package wvlet.lang.catalog

import wvlet.lang.compiler.DBType

/**
  * A [[Catalog]] that defers building its backing catalog (and thus opening any database
  * connection) until the first metadata lookup. Profiles can activate many connectors at once; only
  * the ones a query actually references should connect.
  *
  * `catalogName` and `dbType` come from the connector config so that name resolution and SQL
  * dialect selection never force materialization on their own.
  */
class LazyCatalog(
    override val catalogName: String,
    override val dbType: DBType,
    build: () => Catalog
) extends Catalog:

  private lazy val underlying: Catalog = build()

  override def listSchemaNames: Seq[String]                       = underlying.listSchemaNames
  override def listSchemas: Seq[Catalog.TableSchema]              = underlying.listSchemas
  override def getSchema(schemaName: String): Catalog.TableSchema = underlying.getSchema(schemaName)
  override def schemaExists(schemaName: String): Boolean = underlying.schemaExists(schemaName)
  override def createSchema(schema: Catalog.TableSchema, createMode: Catalog.CreateMode): Unit =
    underlying.createSchema(schema, createMode)

  override def listTableNames(schemaName: String): Seq[String] = underlying.listTableNames(
    schemaName
  )

  override def listTables(schemaName: String): Seq[Catalog.TableDef] = underlying.listTables(
    schemaName
  )

  override def findTable(schemaName: String, tableName: String): Option[Catalog.TableDef] =
    underlying.findTable(schemaName, tableName)

  override def getTable(schemaName: String, tableName: String): Catalog.TableDef = underlying
    .getTable(schemaName, tableName)

  override def tableExists(schemaName: String, tableName: String): Boolean = underlying.tableExists(
    schemaName,
    tableName
  )

  override def createTable(table: Catalog.TableDef, createMode: Catalog.CreateMode): Unit =
    underlying.createTable(table, createMode)

  override def listFunctions: Seq[SQLFunction] = underlying.listFunctions

  override def updateColumns(
      schemaName: String,
      tableName: String,
      columns: Seq[Catalog.TableColumn]
  ): Unit = underlying.updateColumns(schemaName, tableName, columns)

  override def updateTableProperties(
      schemaName: String,
      tableName: String,
      properties: Map[String, Any]
  ): Unit = underlying.updateTableProperties(schemaName, tableName, properties)

  override def updateDatabaseProperties(schemaName: String, properties: Map[String, Any]): Unit =
    underlying.updateDatabaseProperties(schemaName, properties)

end LazyCatalog
