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

import wvlet.lang.StatusCode
import wvlet.lang.catalog.Catalog.CreateMode
import wvlet.lang.compiler.DBType

class InMemoryCatalog(val catalogName: String, functions: Seq[SQLFunction]) extends Catalog:
  // schemaName name -> DatabaseHolder
  private val schemas = collection.mutable.Map.empty[String, SchemaHolder]

  private case class SchemaHolder(schema: Catalog.TableSchema):
    // table name -> table holder
    val tables = collection.mutable.Map.empty[String, Catalog.TableDef]

    def updateDatabase(schemaName: Catalog.TableSchema): SchemaHolder =
      val newDb = SchemaHolder(schemaName)
      newDb.tables ++= tables
      newDb

  override def dbType: DBType = DBType.InMemory

  override def listSchemaNames: Seq[String] = synchronized {
    schemas.values.map(_.schema.name).toSeq
  }

  override def listSchemas: Seq[Catalog.TableSchema] = synchronized {
    schemas.values.map(_.schema).toSeq
  }

  private def getSchemaHolder(name: String): SchemaHolder = synchronized {
    schemas.get(name) match
      case Some(d) =>
        d
      case None =>
        throw StatusCode.SCHEMA_NOT_FOUND.newException(s"schemaName ${name} is not found")
  }

  override def getSchema(schema: String): Catalog.TableSchema = getSchemaHolder(schema).schema

  override def schemaExists(schema: String): Boolean = schemas.get(schema).nonEmpty

  override def createSchema(schema: Catalog.TableSchema, createMode: CreateMode): Unit =
    synchronized {
      schemas.get(schema.name) match
        case Some(_) =>
          createMode match
            case CreateMode.CREATE_IF_NOT_EXISTS =>
            // ok
            case CreateMode.FAIL_IF_EXISTS =>
              throw StatusCode
                .SCHEMA_ALREADY_EXISTS
                .newException(s"schemaName ${schema.name} already exists")
        case None =>
          schemas += schema.name -> SchemaHolder(schema)
    }

  override def listTableNames(schemaName: String): Seq[String] = synchronized {
    val db = getSchemaHolder(schemaName)
    db.tables.values.map(_.name).toSeq
  }

  override def listTables(schemaName: String): Seq[Catalog.TableDef] = synchronized {
    val db = getSchemaHolder(schemaName)
    db.tables.values.toSeq
  }

  override def findTable(schemaName: String, tableName: String): Option[Catalog.TableDef] =
    synchronized {
      schemas
        .get(schemaName)
        .flatMap { d =>
          d.tables.get(tableName)
        }
    }

  override def getTable(schemaName: String, tableName: String): Catalog.TableDef = synchronized {
    val db = getSchemaHolder(schemaName)
    db.tables.get(tableName) match
      case Some(tbl) =>
        tbl
      case None =>
        throw StatusCode
          .TABLE_NOT_FOUND
          .newException(s"table ${schemaName}.${tableName} is not found")
  }

  override def tableExists(schemaName: String, tableName: String): Boolean = synchronized {
    schemas.get(schemaName) match
      case None =>
        false
      case Some(d) =>
        d.tables.contains(tableName)
  }

  override def createTable(table: Catalog.TableDef, createMode: CreateMode): Unit =
    val schemaName = table
      .schema
      .getOrElse {
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Missing schemaName for create table request: ${table.name}")
      }
    synchronized {
      val d = getSchemaHolder(schemaName)
      d.tables.get(table.name) match
        case Some(tbl) =>
          createMode match
            case CreateMode.CREATE_IF_NOT_EXISTS =>
            // ok
            case CreateMode.FAIL_IF_EXISTS =>
              throw StatusCode
                .TABLE_ALREADY_EXISTS
                .newException(s"table ${schemaName}.${table.name} already exists")
        case None =>
          d.tables += table.name -> table
    }

  override def listFunctions: Seq[SQLFunction] = functions

  private def updateTable(schemaName: String, tableName: String)(
      updater: Catalog.TableDef => Catalog.TableDef
  ): Unit = synchronized {
    val d = getSchemaHolder(schemaName)
    d.tables.get(tableName) match
      case Some(oldTbl) =>
        d.tables += tableName -> updater(oldTbl)
      case None =>
        throw StatusCode
          .TABLE_NOT_FOUND
          .newException(s"table ${schemaName}.${tableName} is not found")
  }

  override def updateColumns(
      schemaName: String,
      tableName: String,
      columns: Seq[Catalog.TableColumn]
  ): Unit = updateTable(schemaName, tableName)(tbl => tbl.copy(columns = columns))

  override def updateTableProperties(
      schemaName: String,
      tableName: String,
      properties: Map[String, Any]
  ): Unit = updateTable(schemaName, tableName)(tbl => tbl.copy(properties = properties))

  override def updateDatabaseProperties(schemaName: String, properties: Map[String, Any]): Unit =
    synchronized {
      schemas.get(schemaName) match
        case Some(db) =>
          schemas += schemaName -> db.updateDatabase(db.schema.copy(properties = properties))
        case None =>
          throw StatusCode.SCHEMA_NOT_FOUND.newException(s"schemaName ${schemaName} is not found")
    }

end InMemoryCatalog
