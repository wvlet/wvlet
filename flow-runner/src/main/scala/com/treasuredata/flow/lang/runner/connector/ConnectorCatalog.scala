package com.treasuredata.flow.lang.runner.connector

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.catalog.{Catalog, SQLFunction}

class ConnectorCatalog(val catalogName: String, dbConnector: DBConnector) extends Catalog:
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

  override def namespace: Option[String] = None

  override def listTableNames(schemaName: String): Seq[String] = dbConnector
    .listTables(catalogName, schemaName)
    .map(_.name)

  override def listTables(schemaName: String): Seq[Catalog.TableDef] = dbConnector
    .listTableDefs(catalogName, schemaName)

  override def findTable(schemaName: String, tableName: String): Option[Catalog.TableDef] =
    dbConnector.getTableDef(catalogName, schemaName, tableName)

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

  override def listFunctions: Seq[SQLFunction] = ???

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
