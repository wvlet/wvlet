package com.treasuredata.flow.lang.catalog

import wvlet.log.LogSupport
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.model.DataType

import Catalog.*

/**
  * connector -> catalog* -> schema* -> table* -> column*
  *   - catalog
  *   - schema
  *   - table
  *   - column
  */
trait Catalog extends LogSupport:
  def catalogName: String

  def namespace: Option[String]

  def listSchemaNames: Seq[String]
  def listSchemas: Seq[Catalog.TableSchema]
  def getSchema(schemaName: String): Catalog.TableSchema
  def schemaExists(schemaName: String): Boolean
  def createSchema(schemaName: Catalog.TableSchema, createMode: Catalog.CreateMode): Unit

  def listTableNames(schemaName: String): Seq[String]
  def listTables(schemaName: String): Seq[Catalog.TableDef]
  def findTable(schemaName: String, tableName: String): Option[Catalog.TableDef]
  def getTable(schemaName: String, tableName: String): Catalog.TableDef
  def tableExists(schemaName: String, tableName: String): Boolean
  def createTable(tableName: Catalog.TableDef, createMode: Catalog.CreateMode): Unit

  def findTableFromQName(contextDatabase: String, qname: NameExpr): Option[Catalog.TableDef] =
    qname.fullName.split(".").toList match
      case catalog :: schema :: tbl :: Nil =>
        if catalog == catalogName then
          findTable(schema, tbl)
        else
          None
      case schema :: tbl :: Nil =>
        findTable(schema, tbl)
      case _ =>
        findTable(contextDatabase, qname.toString)

  def listFunctions: Seq[SQLFunction]

  def updateColumns(schemaName: String, tableName: String, columns: Seq[Catalog.TableColumn]): Unit
  def updateTableProperties(
      schemaName: String,
      tableName: String,
      properties: Map[String, Any]
  ): Unit

  def updateDatabaseProperties(schemaName: String, properties: Map[String, Any]): Unit

end Catalog

object Catalog:
  /**
    * A schema is a collection of tables
    *
    * @param name
    * @param description
    * @param metadata
    */
  case class TableSchema(
      name: String,
      description: String = "",
      properties: Map[String, Any] = Map.empty
  )

  /**
    * Table and its column definition
    * @param schema
    * @param name
    * @param columns
    * @param description
    * @param properties
    */
  case class TableDef(
      schema: Option[String],
      name: String,
      columns: Seq[TableColumn],
      description: String = "",
      properties: Map[String, Any] = Map.empty
  ):
    def fullName: String = s"${schema.map(db => s"${db}.").getOrElse("")}${name}"

    def column(name: String): TableColumn = columns
      .find(_.name == name)
      .getOrElse {
        throw StatusCode
          .COLUMN_NOT_FOUND
          .newException(s"Column ${name} is not found in ${fullName}")
      }

  case class TableColumn(name: String, dataType: DataType, properties: Map[String, Any] = Map.empty)

  sealed trait CreateMode

  object CreateMode:
    object CREATE_IF_NOT_EXISTS extends CreateMode
    object FAIL_IF_EXISTS       extends CreateMode

end Catalog
