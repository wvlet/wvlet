package com.treasuredata.flow.lang.model.expr

import com.treasuredata.flow.lang.model.DataType.{EmbeddedRecordType, NamedType}
import com.treasuredata.flow.lang.model.{DataType, NodeLocation}
import wvlet.log.LogSupport

/**
  * Used for matching column name with Attribute
  *
  * @param database
  * @param table
  * @param columnName
  */
case class ColumnPath(database: Option[String], table: Option[String], columnName: String)

object ColumnPath:
  def fromQName(fullName: String): Option[ColumnPath] =
    // TODO Should we handle quotation in the name or just reject such strings?
    fullName.split("\\.").toList match
      case List(db, t, c) =>
        Some(ColumnPath(Some(db), Some(t), c))
      case List(t, c) =>
        Some(ColumnPath(None, Some(t), c))
      case List(c) =>
        Some(ColumnPath(None, None, c))
      case _ =>
        None

/**
  * Attribute is used for column names of relational table inputs and outputs
  */
trait Attribute extends LeafExpression with LogSupport:
  override def attributeName: String = name

  def name: String

  def fullName: String =
    s"${prefix}${name}"

  def prefix: String = qualifier.map(q => s"${q}.").getOrElse("")

  // (database name)?.(table name) given in the original SQL
  def qualifier: Option[String]

  def withQualifier(newQualifier: String): Attribute = withQualifier(Some(newQualifier))

  def withQualifier(newQualifier: Option[String]): Attribute

  def alias: Option[String] =
    this match
      case a: Alias => Some(a.name)
      case _        => None

  def withAlias(newAlias: String): Attribute = withAlias(Some(newAlias))

  def withAlias(newAlias: Option[String]): Attribute =
    newAlias match
      case None => this
      case Some(alias) =>
        this match
          case a: Alias =>
            if name != alias then a.copy(name = alias)
            else a
          case other if other.name == alias =>
            // No need to have alias
            other
          case other =>
            Alias(qualifier, alias, other, other.tableAlias, None)

  def tableAlias: Option[String]

  def withTableAlias(tableAlias: String): Attribute = withTableAlias(Some(tableAlias))

  def withTableAlias(tableAlias: Option[String]): Attribute

  /**
    * Return columns used for generating this attribute
    */
  def inputColumns: Seq[Attribute]

  /**
    * Return columns generated from this attribute
    */
  def outputColumns: Seq[Attribute]

  /**
    * Return true if this Attribute matches with a given column path
    *
    * @param columnPath
    * @return
    */
  def matchesWith(columnPath: ColumnPath): Boolean =
    def matchesWith(columnName: String): Boolean =
      this match
        case a: AllColumns =>
          a.inputColumns.exists(_.name == columnName)
        case a: Attribute if a.name == columnName =>
          true
        case _ =>
          false

    columnPath.table match
      // TODO handle (catalog).(database).(table) names in the qualifier
      case Some(tableName) =>
        (qualifier.contains(tableName) || tableAlias.contains(tableName)) && matchesWith(columnPath.columnName)
      case None =>
        matchesWith(columnPath.columnName)

case class UnresolvedAttribute(
    override val qualifier: Option[String],
    name: String,
    tableAlias: Option[String],
    nodeLocation: Option[NodeLocation]
) extends Attribute:
  override def toString: String = s"UnresolvedAttribute(${fullName})"
  override lazy val resolved    = false

  override def withQualifier(newQualifier: Option[String]): UnresolvedAttribute =
    this.copy(qualifier = newQualifier)

  override def withTableAlias(tableAlias: Option[String]): Attribute =
    this.copy(tableAlias = tableAlias)

  override def inputColumns: Seq[Attribute]  = Seq.empty
  override def outputColumns: Seq[Attribute] = Seq.empty

case class AllColumns(
    override val qualifier: Option[String],
    columns: Option[Seq[Attribute]],
    tableAlias: Option[String],
    nodeLocation: Option[NodeLocation]
) extends Attribute
    with LogSupport:
  override def name: String = "*"

  override def children: Seq[Expression] =
    // AllColumns is a reference to the input attributes.
    // Return empty so as not to traverse children from here.
    Seq.empty

  override def inputColumns: Seq[Attribute] =
    columns match
      case Some(columns) =>
        columns.flatMap {
          case a: AllColumns => a.inputColumns
          case a             => Seq(a)
        }
      case None => Nil

  override def outputColumns: Seq[Attribute] =
    inputColumns.map(_.withTableAlias(tableAlias).withQualifier(qualifier))

  override def dataType: DataType =
    columns
      .map(cols => EmbeddedRecordType(cols.map(x => NamedType(x.name, x.dataType))))
      .getOrElse(DataType.UnknownType)

  override def withQualifier(newQualifier: Option[String]): Attribute =
    this.copy(qualifier = newQualifier)

  override def withTableAlias(tableAlias: Option[String]): Attribute =
    this.copy(tableAlias = tableAlias)

  override def toString =
    columns match
      case Some(attrs) if attrs.nonEmpty =>
        val inputs = attrs
          .map(a => s"${a.fullName}:${a.dataTypeName}").mkString(", ")
        s"AllColumns(${inputs})"
      case _ =>
        s"AllColumns(${fullName})"

  override lazy val resolved = columns.isDefined

case class Alias(
    qualifier: Option[String],
    name: String,
    expr: Expression,
    tableAlias: Option[String],
    nodeLocation: Option[NodeLocation]
) extends Attribute:
  override def inputColumns: Seq[Attribute]  = Seq(this)
  override def outputColumns: Seq[Attribute] = inputColumns

  override def children: Seq[Expression] = Seq(expr)

  override def withQualifier(newQualifier: Option[String]): Attribute =
    this.copy(qualifier = newQualifier)

  override def withTableAlias(tableAlias: Option[String]): Attribute =
    this.copy(tableAlias = tableAlias)

  override def toString: String =
    s"<${fullName}> := ${expr}"

  override def dataType: DataType = expr.dataType

/**
  * An attribute that produces a single column value with a given expression.
  *
  * @param expr
  * @param qualifier
  * @param nodeLocation
  */
case class SingleColumn(
    expr: Expression,
    qualifier: Option[String],
    tableAlias: Option[String],
    nodeLocation: Option[NodeLocation]
) extends Attribute:
  override def name: String       = expr.attributeName
  override def dataType: DataType = expr.dataType

  override def inputColumns: Seq[Attribute]  = Seq(this)
  override def outputColumns: Seq[Attribute] = inputColumns

  override def children: Seq[Expression] = Seq(expr)
  override def toString                  = s"${fullName}:${dataTypeName} := ${expr}"

  override def withQualifier(newQualifier: Option[String]): Attribute =
    this.copy(qualifier = newQualifier)

  override def withTableAlias(tableAlias: Option[String]): Attribute =
    this.copy(tableAlias = tableAlias)

/**
  * A single column merged from multiple input expressions (e.g., union, join)
  * @param inputs
  * @param qualifier
  * @param nodeLocation
  */
case class MultiSourceColumn(
    inputs: Seq[Expression],
    qualifier: Option[String],
    tableAlias: Option[String],
    nodeLocation: Option[NodeLocation]
) extends Attribute:
  // require(inputs.nonEmpty, s"The inputs of MultiSourceColumn should not be empty: ${this}", nodeLocation)

  override def toString: String = s"${fullName}:${dataTypeName} := {${inputs.mkString(", ")}}"

  override def inputColumns: Seq[Attribute] =
    inputs.map {
      case a: Attribute => a
      case e: Expression =>
        SingleColumn(e, qualifier, None, e.nodeLocation)
    }

  override def outputColumns: Seq[Attribute] = Seq(this)

  override def children: Seq[Expression] =
    // MultiSourceColumn is a reference to the multiple columns. Do not traverse here
    Seq.empty

  override def name: String =
    inputs.head.attributeName

  override def dataType: DataType =
    inputs.head.dataType

  override def withQualifier(newQualifier: Option[String]): Attribute =
    this.copy(qualifier = newQualifier)

  override def withTableAlias(tableAlias: Option[String]): Attribute =
    this.copy(tableAlias = tableAlias)
