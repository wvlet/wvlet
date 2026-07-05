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

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Catalog
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.catalog.SQLFunction
import wvlet.lang.compiler.DBType
import wvlet.lang.model.DataType.SchemaType

/**
  * Exposes a source connector's [[CatalogProvider]] as a read-only [[Catalog]] so that the compiler
  * can resolve `from <connector>.<table>` references against it. Source connectors have a single
  * flat namespace: every table lives in the synthetic `main` schema
  */
class ConnectorCatalogAdapter(override val catalogName: String, provider: CatalogProvider)
    extends Catalog:

  override def dbType: DBType = DBType.Generic

  private def readOnly: Nothing =
    throw StatusCode
      .NOT_IMPLEMENTED
      .newException(s"Catalog '${catalogName}' is a read-only data source")

  override def listSchemaNames: Seq[String]          = Seq("main")
  override def listSchemas: Seq[Catalog.TableSchema] = Seq(
    Catalog.TableSchema(Some(catalogName), "main")
  )

  override def getSchema(schemaName: String): Catalog.TableSchema = Catalog.TableSchema(
    Some(catalogName),
    schemaName
  )

  override def schemaExists(schemaName: String): Boolean = schemaName == "main"
  override def createSchema(schema: Catalog.TableSchema, createMode: Catalog.CreateMode): Unit =
    readOnly

  override def listTableNames(schemaName: String): Seq[String] = provider.listTables.map(_.name)
  override def listTables(schemaName: String): Seq[Catalog.TableDef] = provider
    .listTables
    .flatMap(t => findTable(schemaName, t.name))

  override def findTable(schemaName: String, tableName: String): Option[Catalog.TableDef] = provider
    .schemaOf(tableName)
    .collect { case s: SchemaType =>
      Catalog.TableDef(
        TableName(Some(catalogName), Some("main"), tableName),
        columns = s.fields.map(f => Catalog.TableColumn(f.name.name, f.dataType))
      )
    }

  override def getTable(schemaName: String, tableName: String): Catalog.TableDef = findTable(
    schemaName,
    tableName
  ).getOrElse(
    throw StatusCode
      .TABLE_NOT_FOUND
      .newException(s"Table '${tableName}' is not found in ${catalogName}")
  )

  override def tableExists(schemaName: String, tableName: String): Boolean =
    findTable(schemaName, tableName).isDefined

  override def createTable(table: Catalog.TableDef, createMode: Catalog.CreateMode): Unit = readOnly

  override def listFunctions: Seq[SQLFunction] = Nil

  override def updateColumns(
      schemaName: String,
      tableName: String,
      columns: Seq[Catalog.TableColumn]
  ): Unit = readOnly

  override def updateTableProperties(
      schemaName: String,
      tableName: String,
      properties: Map[String, Any]
  ): Unit = readOnly

  override def updateDatabaseProperties(schemaName: String, properties: Map[String, Any]): Unit =
    readOnly

end ConnectorCatalogAdapter
