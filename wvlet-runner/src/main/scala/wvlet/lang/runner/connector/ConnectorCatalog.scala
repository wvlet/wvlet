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
package wvlet.lang.runner.connector

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import wvlet.lang.StatusCode
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.catalog.{Catalog, SQLFunction}
import wvlet.lang.compiler.DBType
import wvlet.lang.runner.ThreadUtil
import wvlet.log.LogSupport

import java.util.concurrent.TimeUnit

class ConnectorCatalog(val catalogName: String, defaultSchema: String, dbConnector: DBConnector)
    extends Catalog
    with LogSupport:

  private val tablesInSchemaCache = Caffeine
    .newBuilder()
    .expireAfterWrite(5, TimeUnit.MINUTES)
    .build { (schema: String) =>
      debug(s"Loading tables in schema ${catalogName}.${schema}")
      dbConnector.listTableDefs(catalogName, schema)
    }

  ThreadUtil.runBackgroundTask(() => init())

  private def init(): Unit = tablesInSchemaCache.get(defaultSchema)

  override def dbType: DBType = dbConnector.dbType

  // implement Catalog interface
  override def listSchemaNames: Seq[String] = dbConnector.listSchemaNames(catalogName)

  override def listSchemas: Seq[Catalog.TableSchema] = dbConnector.listSchemas(catalogName)

  override def getSchema(schemaName: String): Catalog.TableSchema = dbConnector
    .getSchema(catalogName, schemaName)
    .getOrElse {
      throw StatusCode.SCHEMA_NOT_FOUND.newException(s"schemaName ${schemaName} is not found")
    }

  override def schemaExists(schemaName: String): Boolean =
    dbConnector.getSchema(catalogName, schemaName).isDefined

  override def createSchema(schema: Catalog.TableSchema, createMode: Catalog.CreateMode): Unit =
    createMode match
      case Catalog.CreateMode.FAIL_IF_EXISTS =>
        if schemaExists(schema.name) then
          throw StatusCode
            .SCHEMA_ALREADY_EXISTS
            .newException(s"schemaName ${schema.name} already exists")
      case _ =>
      // ok
    dbConnector.createSchema(schema.catalog.getOrElse(catalogName), schema.name)

  override def listTableNames(schemaName: String): Seq[String] = tablesInSchemaCache
    .get(schemaName)
    .map(_.name)

  override def listTables(schemaName: String): Seq[Catalog.TableDef] = tablesInSchemaCache
    .get(schemaName)

  override def findTable(schemaName: String, tableName: String): Option[Catalog.TableDef] =
    tablesInSchemaCache
      .get(schemaName)
      .find(_.name == tableName)
      .orElse {
        // If an entry is not found in the cache, check again
        dbConnector.getTableDef(catalogName, schemaName, tableName)
      }

  override def getTable(schemaName: String, tableName: String): Catalog.TableDef = findTable(
    schemaName,
    tableName
  ).getOrElse {
    throw StatusCode
      .TABLE_NOT_FOUND
      .newException(s"tableName ${schemaName}.${tableName} is not found")
  }

  override def tableExists(schemaName: String, tableName: String): Boolean =
    findTable(schemaName, tableName).isDefined

  override def createTable(tableName: Catalog.TableDef, createMode: Catalog.CreateMode): Unit = ???

  override def listFunctions: Seq[SQLFunction] = dbConnector.listFunctions(catalogName)

  override def updateColumns(
      schemaName: String,
      tableName: String,
      columns: Seq[Catalog.TableColumn]
  ): Unit = ???

  override def updateTableProperties(
      schemaName: String,
      tableName: String,
      properties: Map[String, Any]
  ): Unit = ???

  override def updateDatabaseProperties(schemaName: String, properties: Map[String, Any]): Unit =
    ???

end ConnectorCatalog
