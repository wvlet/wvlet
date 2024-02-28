package com.treasuredata.flow.lang.catalog

import wvlet.log.LogSupport
import com.treasuredata.flow.lang.model.expr.*
import Catalog.CreateMode
import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.model.DataType

trait Catalog extends LogSupport:

  def catalogName: String

  def namespace: Option[String]

  def listDatabases: Seq[String]
  def getDatabase(database: String): Catalog.Database
  def databaseExists(database: String): Boolean
  def createDatabase(catalogDatabase: Catalog.Database, createMode: CreateMode): Unit

  def listTables(database: String): Seq[String]
  def findTable(database: String, table: String): Option[Catalog.Table]
  def getTable(database: String, table: String): Catalog.Table
  def tableExists(database: String, table: String): Boolean
  def createTable(table: Catalog.Table, createMode: CreateMode): Unit

  def findFromQName(contextDatabase: String, qname: QName): Option[Catalog.Table] =
    qname.parts match
      case catalog :: db :: tbl :: Nil =>
        if catalog == catalogName then findTable(db, tbl)
        else None
      case db :: tbl :: Nil =>
        findTable(db, tbl)
      case _ =>
        findTable(contextDatabase, qname.toString)

  def listFunctions: Seq[SQLFunction]

  def updateTableSchema(database: String, table: String, schema: Catalog.TableSchema): Unit
  def updateTableProperties(database: String, table: String, properties: Map[String, Any]): Unit
  def updateDatabaseProperties(database: String, properties: Map[String, Any]): Unit

//case class DatabaseIdentifier(database: String, catalog: Option[String])
//case class TableIdentifier(table: String, database: Option[String], catalog: Option[String])

object Catalog:

  def newTable(database: String, table: String, schema: TableSchema): Table =
    Table(database = Some(database), name = table, schema = schema)

  def newSchema: TableSchema = TableSchema(columns = Seq.empty)

  /**
    * A database defined in the catalog
    *
    * @param name
    * @param description
    * @param metadata
    */
  case class Database(name: String, description: String = "", properties: Map[String, Any] = Map.empty)

  case class Table(
      database: Option[String],
      name: String,
      schema: TableSchema,
      description: String = "",
      properties: Map[String, Any] = Map.empty
  ):
    def withDatabase(db: String): Table = copy(database = Some(db))
    def fullName: String                = s"${database.map(db => s"${db}.").getOrElse("")}${name}"

    def column(name: String): TableColumn = schema.columns.find(_.name == name).getOrElse {
      throw StatusCode.COLUMN_NOT_FOUND.newException(s"Column ${name} is not found in ${fullName}")
    }

  case class TableSchema(columns: Seq[TableColumn]):
    def addColumn(c: TableColumn): TableSchema =
      this.copy(columns = columns :+ c)

    def addColumn(name: String, dataType: DataType, properties: Map[String, Any] = Map.empty): TableSchema =
      this.copy(columns = columns :+ TableColumn(name, dataType, properties))

  case class TableColumn(name: String, dataType: DataType, properties: Map[String, Any] = Map.empty)

  sealed trait CreateMode

  object CreateMode:
    object CREATE_IF_NOT_EXISTS extends CreateMode
    object FAIL_IF_EXISTS       extends CreateMode
