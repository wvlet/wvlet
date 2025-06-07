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

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.DBType
import wvlet.log.LogSupport

/**
  * A static catalog implementation that loads catalog metadata from persisted storage without
  * making any remote calls. This is used for offline compilation and analysis.
  */
case class StaticCatalog(
    override val catalogName: String,
    override val dbType: DBType,
    schemas: List[Catalog.TableSchema],
    tablesBySchema: Map[String, List[Catalog.TableDef]],
    functions: List[SQLFunction]
) extends Catalog
    with LogSupport:

  // Build efficient lookup structures
  private lazy val schemaMap: Map[String, Catalog.TableSchema] = schemas.map(s => s.name -> s).toMap

  private lazy val tableMap: Map[String, Map[String, Catalog.TableDef]] = tablesBySchema.map {
    case (schemaName, tables) =>
      schemaName -> tables.map(t => t.name -> t).toMap
  }

  override def listSchemaNames: Seq[String] = schemas.map(_.name)

  override def listSchemas: Seq[Catalog.TableSchema] = schemas

  override def getSchema(schemaName: String): Catalog.TableSchema =
    schemaMap.get(schemaName) match
      case Some(schema) =>
        schema
      case None =>
        throw StatusCode
          .SCHEMA_NOT_FOUND
          .newException(s"Schema ${schemaName} is not found in static catalog")

  override def schemaExists(schemaName: String): Boolean = schemaMap.contains(schemaName)

  override def createSchema(schemaName: Catalog.TableSchema, createMode: Catalog.CreateMode): Unit =
    throw StatusCode.NOT_IMPLEMENTED.newException("Cannot create schema in static catalog")

  override def listTableNames(schemaName: String): Seq[String] = tablesBySchema
    .get(schemaName)
    .map(_.map(_.name))
    .getOrElse(Seq.empty)

  override def listTables(schemaName: String): Seq[Catalog.TableDef] = tablesBySchema.getOrElse(
    schemaName,
    List.empty
  )

  override def findTable(schemaName: String, tableName: String): Option[Catalog.TableDef] = tableMap
    .get(schemaName)
    .flatMap(_.get(tableName))

  override def getTable(schemaName: String, tableName: String): Catalog.TableDef = findTable(
    schemaName,
    tableName
  ).getOrElse {
    throw StatusCode
      .TABLE_NOT_FOUND
      .newException(s"Table ${schemaName}.${tableName} is not found in static catalog")
  }

  override def tableExists(schemaName: String, tableName: String): Boolean =
    findTable(schemaName, tableName).isDefined

  override def createTable(tableName: Catalog.TableDef, createMode: Catalog.CreateMode): Unit =
    throw StatusCode.NOT_IMPLEMENTED.newException("Cannot create table in static catalog")

  override def listFunctions: Seq[SQLFunction] = functions

  override def updateColumns(
      schemaName: String,
      tableName: String,
      columns: Seq[Catalog.TableColumn]
  ): Unit = throw StatusCode.NOT_IMPLEMENTED.newException("Cannot update columns in static catalog")

  override def updateTableProperties(
      schemaName: String,
      tableName: String,
      properties: Map[String, Any]
  ): Unit =
    throw StatusCode
      .NOT_IMPLEMENTED
      .newException("Cannot update table properties in static catalog")

  override def updateDatabaseProperties(schemaName: String, properties: Map[String, Any]): Unit =
    throw StatusCode
      .NOT_IMPLEMENTED
      .newException("Cannot update database properties in static catalog")

end StaticCatalog
