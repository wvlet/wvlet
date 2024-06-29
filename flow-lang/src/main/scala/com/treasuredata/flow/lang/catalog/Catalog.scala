package com.treasuredata.flow.lang.catalog

import wvlet.log.LogSupport
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.model.DataType

import Catalog.*

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

  def findFromQName(contextDatabase: String, qname: Name): Option[Catalog.Table] =
    qname.fullName.split(".").toList match
      case catalog :: db :: tbl :: Nil =>
        if catalog == catalogName then
          findTable(db, tbl)
        else
          None
      case db :: tbl :: Nil =>
        findTable(db, tbl)
      case _ =>
        findTable(contextDatabase, qname.toString)

  def listFunctions: Seq[SQLFunction]

  def updateTableSchema(database: String, table: String, schema: Catalog.TableSchema): Unit
  def updateTableProperties(database: String, table: String, properties: Map[String, Any]): Unit
  def updateDatabaseProperties(database: String, properties: Map[String, Any]): Unit

end Catalog

//case class DatabaseIdentifier(database: String, catalog: Option[String])
//case class TableIdentifier(table: String, database: Option[String], catalog: Option[String])

object Catalog:

  def newTable(database: String, table: String, schema: TableSchema): Table = Table(
    database = Some(database),
    name = table,
    schema = schema
  )

  def newSchema: TableSchema = TableSchema(columns = Seq.empty)

  /**
    * A database defined in the catalog
    *
    * @param name
    * @param description
    * @param metadata
    */
  case class Database(
      name: String,
      description: String = "",
      properties: Map[String, Any] = Map.empty
  )

  case class Table(
      database: Option[String],
      name: String,
      schema: TableSchema,
      description: String = "",
      properties: Map[String, Any] = Map.empty
  ):
    def withDatabase(db: String): Table = copy(database = Some(db))
    def fullName: String                = s"${database.map(db => s"${db}.").getOrElse("")}${name}"

    def column(name: String): TableColumn = schema
      .columns
      .find(_.name == name)
      .getOrElse {
        throw StatusCode
          .COLUMN_NOT_FOUND
          .newException(s"Column ${name} is not found in ${fullName}")
      }

  case class TableSchema(columns: Seq[TableColumn]):
    def addColumn(c: TableColumn): TableSchema = this.copy(columns = columns :+ c)

    def addColumn(
        name: String,
        dataType: DataType,
        properties: Map[String, Any] = Map.empty
    ): TableSchema = this.copy(columns = columns :+ TableColumn(name, dataType, properties))

  case class TableColumn(name: String, dataType: DataType, properties: Map[String, Any] = Map.empty)

  sealed trait CreateMode

  object CreateMode:
    object CREATE_IF_NOT_EXISTS extends CreateMode
    object FAIL_IF_EXISTS       extends CreateMode

end Catalog

class InMemoryCatalog(
    val catalogName: String,
    val namespace: Option[String],
    functions: Seq[SQLFunction]
) extends Catalog:

  // database name -> DatabaseHolder
  private val databases = collection.mutable.Map.empty[String, DatabaseHolder]

  private case class DatabaseHolder(db: Catalog.Database):
    // table name -> table holder
    val tables = collection.mutable.Map.empty[String, Catalog.Table]

    def updateDatabase(database: Catalog.Database): DatabaseHolder =
      val newDb = DatabaseHolder(database)
      newDb.tables ++= tables
      newDb

  override def listDatabases: Seq[String] = synchronized {
    databases.values.map(_.db.name).toSeq
  }

  private def getDatabaseHolder(name: String): DatabaseHolder = synchronized {
    databases.get(name) match
      case Some(d) =>
        d
      case None =>
        throw StatusCode.DATABASE_NOT_FOUND.newException(s"database ${name} is not found")
  }

  override def getDatabase(database: String): Catalog.Database = getDatabaseHolder(database).db

  override def databaseExists(database: String): Boolean = databases.get(database).nonEmpty

  override def createDatabase(newDatabase: Catalog.Database, createMode: CreateMode): Unit =
    synchronized {
      databases.get(newDatabase.name) match
        case Some(_) =>
          createMode match
            case CreateMode.CREATE_IF_NOT_EXISTS =>
            // ok
            case CreateMode.FAIL_IF_EXISTS =>
              throw StatusCode
                .DATABASE_ALREADY_EXISTS
                .newException(s"database ${newDatabase.name} already exists")
        case None =>
          databases += newDatabase.name -> DatabaseHolder(newDatabase)
    }

  override def listTables(database: String): Seq[String] = synchronized {
    val db = getDatabaseHolder(database)
    db.tables.values.map(_.name).toSeq
  }

  override def findTable(database: String, table: String): Option[Catalog.Table] = synchronized {
    databases
      .get(database)
      .flatMap { d =>
        d.tables.get(table)
      }
  }

  override def getTable(database: String, table: String): Catalog.Table = synchronized {
    val db = getDatabaseHolder(database)
    db.tables.get(table) match
      case Some(tbl) =>
        tbl
      case None =>
        throw StatusCode.TABLE_NOT_FOUND.newException(s"table ${database}.${table} is not found")
  }

  override def tableExists(database: String, table: String): Boolean = synchronized {
    databases.get(database) match
      case None =>
        false
      case Some(d) =>
        d.tables.contains(table)
  }

  override def createTable(table: Catalog.Table, createMode: CreateMode): Unit =
    val database = table
      .database
      .getOrElse {
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Missing database for create table request: ${table.name}")
      }
    synchronized {
      val d = getDatabaseHolder(database)
      d.tables.get(table.name) match
        case Some(tbl) =>
          createMode match
            case CreateMode.CREATE_IF_NOT_EXISTS =>
            // ok
            case CreateMode.FAIL_IF_EXISTS =>
              throw StatusCode
                .TABLE_ALREADY_EXISTS
                .newException(s"table ${database}.${table.name} already exists")
        case None =>
          d.tables += table.name -> table
    }

  override def listFunctions: Seq[SQLFunction] = functions

  private def updateTable(database: String, table: String)(
      updater: Catalog.Table => Catalog.Table
  ): Unit = synchronized {
    val d = getDatabaseHolder(database)
    d.tables.get(table) match
      case Some(oldTbl) =>
        d.tables += table -> updater(oldTbl)
      case None =>
        throw StatusCode.TABLE_NOT_FOUND.newException(s"table ${database}.${table} is not found")
  }

  override def updateTableSchema(
      database: String,
      table: String,
      schema: Catalog.TableSchema
  ): Unit = updateTable(database, table)(tbl => tbl.copy(schema = schema))

  override def updateTableProperties(
      database: String,
      table: String,
      properties: Map[String, Any]
  ): Unit = updateTable(database, table)(tbl => tbl.copy(properties = properties))

  override def updateDatabaseProperties(database: String, properties: Map[String, Any]): Unit =
    synchronized {
      databases.get(database) match
        case Some(db) =>
          databases += database -> db.updateDatabase(db.db.copy(properties = properties))
        case None =>
          throw StatusCode.DATABASE_NOT_FOUND.newException(s"database ${database} is not found")
    }

end InMemoryCatalog
