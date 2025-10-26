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

import wvlet.log.LogSupport
import wvlet.lang.model.expr.*
import wvlet.lang.api.Span
import wvlet.lang.api.StatusCode
import wvlet.lang.model.DataType
import Catalog.*
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.Name

import java.lang.invoke.MethodHandles.loop

/**
  * connector -> catalog* -> schema* -> table* -> column*
  *   - catalog
  *   - schema
  *   - table
  *   - column
  */
trait Catalog extends LogSupport:
  def dbType: DBType
  def catalogName: String

  def listSchemaNames: Seq[String]
  def listSchemas: Seq[Catalog.TableSchema]
  def getSchema(schemaName: String): Catalog.TableSchema
  def schemaExists(schemaName: String): Boolean
  def createSchema(schemaName: Catalog.TableSchema, createMode: Catalog.CreateMode): Unit

  def listTableNames(schemaName: String): Seq[String]
  def listTables(schemaName: String): Seq[Catalog.TableDef]
  def findTable(schemaName: String, tableName: String): Option[Catalog.TableDef]

  def getTable(tableName: TableName): Option[Catalog.TableDef] =
    tableName.catalog match
      case c if c.isEmpty || c.exists(_ == catalogName) =>
        if tableName.schema.isDefined then
          findTable(tableName.schema.get, tableName.name)
        else
          None
      case _ =>
        None

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
    if catalog.nonEmpty then
      require(
        schema.nonEmpty,
        s"Schema must be specified if catalog is specified: ${catalog.get}.???.${name}"
      )

    override def toString: String = fullName
    def qName: List[String]       =
      (catalog, schema) match
        case (Some(c), Some(s)) =>
          List(c, s, name)
        case (None, Some(s)) =>
          List(s, name)
        case _ =>
          List(name)

    def fullName: String =
      // Quote the catalog and schema names if they contain special characters
      qName.mkString(".")

    def toExpr: NameExpr =
      def loop(prefix: String, rest: List[String]): NameExpr =
        val qual = NameExpr.fromString(prefix)
        if rest.isEmpty then
          qual
        else
          DotRef(qual, loop(rest.head, rest.tail), DataType.NoType, Span.NoSpan)
      val q = qName
      loop(q.head, q.tail)

  end TableName

  object TableName:
    def apply(s: String): TableName = parse(s)
    def parse(s: String): TableName =
      s.split("\\.").toList match
        case tbl :: Nil =>
          TableName(None, None, tbl)
        case sc :: tbl :: Nil =>
          TableName(None, Some(sc), tbl)
        case ct :: sc :: tbl :: Nil =>
          TableName(Some(ct), Some(sc), tbl)
        case _ =>
          throw StatusCode.SYNTAX_ERROR.newException(s"Invalid table name: ${s}")

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
    def name: String           = tableName.name
    def schema: Option[String] = tableName.schema

    lazy val schemaType: SchemaType =
      val fields = columns.map { c =>
        NamedType(Name.termName(c.name), c.dataType)
      }
      SchemaType(
        // TODO resolve parent schema catalog types
        None,
        Name.typeName(name),
        fields.toList
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
