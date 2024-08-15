package com.treasuredata.flow.lang.catalog

import wvlet.log.LogSupport
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.model.DataType
import Catalog.*
import com.treasuredata.flow.lang.model.DataType.{NamedType, SchemaType}
import com.treasuredata.flow.lang.compiler.Name

/**
  * connector -> catalog* -> schema* -> table* -> column*
  *   - catalog
  *   - schema
  *   - table
  *   - column
  */
trait Catalog extends LogSupport:
  def catalogName: String

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
      catalog: Option[String],
      name: String,
      description: String = "",
      properties: Map[String, Any] = Map.empty
  )

  case class TableName(catalog: Option[String], schema: Option[String], name: String):
    def fullName: String =
      (catalog, schema) match
        case (Some(c), Some(s)) =>
          s"${c}.${s}.${name}"
        case (None, Some(s)) =>
          s"${s}.${name}"
        case (_, _) =>
          name

  /**
    * Table and its column definition
    * @param schema
    * @param name
    * @param columns
    * @param description
    * @param properties
    */
  case class TableDef(
      tableName: TableName,
      columns: Seq[TableColumn],
      description: String = "",
      properties: Map[String, Any] = Map.empty
  ):
    def fullName: String       = tableName.fullName
    def name                   = tableName.name
    def schema: Option[String] = tableName.schema

    lazy val schemaType: SchemaType =
      val fields = columns.map { c =>
        NamedType(Name.termName(c.name), c.dataType)
      }
      SchemaType(
        // TODO resolve parent schema catalog types
        None,
        Name.typeName(name),
        fields
      )

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
