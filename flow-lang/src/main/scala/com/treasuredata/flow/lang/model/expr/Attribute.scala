package com.treasuredata.flow.lang.model.expr

import com.treasuredata.flow.lang.model.DataType.{EmbeddedRecordType, NamedType}
import com.treasuredata.flow.lang.model.{DataType, NodeLocation}
import wvlet.airframe.ulid.ULID
import wvlet.log.LogSupport

/**
  * Attribute is used for column names of relational table inputs and outputs
  */
trait Attribute extends LeafExpression with LogSupport:
  override def attributeName: String = name.leafName

  def name: Name
  def fullName: String = name.fullName

  def typeDescription: String = dataTypeName

  def alias: Option[Name] =
    this match
      case a: Alias => Some(a.name)
      case _        => None

  def withAlias(newAlias: Name): Attribute = withAlias(Some(newAlias))

  def withAlias(newAlias: Option[Name]): Attribute =
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
          case other => Alias(alias, other, None)

  /**
    * * Returns the index of this attribute in the input or output columns
    * @return
    */
  lazy val attributeIndex: AttributeIndex = AttributeIndex.fromAttribute(this)

  /**
    * Return columns (attributes) used for generating this attribute
    */
  def inputAttributes: Seq[Attribute]

  /**
    * Return columns (attributes) generated from this attribute
    */
  def outputAttributes: Seq[Attribute]

  def sourceColumns: Seq[SourceColumn] = Seq.empty

//  /**
//    * Return true if this Attribute matches with a given column path
//    *
//    * @param columnPath
//    * @return
//    */
//  def matchesWith(columnPath: ColumnPath): Boolean =
//    def matchesWith(columnName: String): Boolean =
//      this match
//        case a: AllColumns =>
//          a.inputAttributes.exists(_.name == columnName)
//        case a: Attribute if a.name == columnName =>
//          true
//        case _ =>
//          false
//
//    columnPath.table match
//      // TODO handle (catalog).(database).(table) names in the qualifier
//      case Some(tableName) =>
//        (qualifier.contains(tableName) || tableAlias.contains(tableName)) && matchesWith(columnPath.columnName)
//      case None =>
//        matchesWith(columnPath.columnName)

/**
  * A reference to an [[Attribute]] object with an globally unique ID
  *
  * @param attr
  */
case class AttributeRef(attr: Attribute)(val exprId: ULID = ULID.newULID) extends Attribute:
  override def name: Name       = attr.name
  override def toString: String = s"AttributeRef(${attr})"

  override def nodeLocation: Option[NodeLocation] = attr.nodeLocation

  override def inputAttributes: Seq[Attribute]  = attr.inputAttributes
  override def outputAttributes: Seq[Attribute] = attr.inputAttributes

  override def hashCode(): Int = super.hashCode()
  override def equals(obj: Any): Boolean =
    obj match
      case that: AttributeRef => that.attr == this.attr
      case _                  => false

/**
  * An attribute that produces a single column value with a given expression.
  *
  * @param expr
  * @param qualifier
  * @param nodeLocation
  */
case class SingleColumn(
    override val name: Name,
    expr: Expression,
    nodeLocation: Option[NodeLocation]
) extends Attribute:
  override def dataType: DataType = expr.dataType

  override def inputAttributes: Seq[Attribute] = Seq(this)

  override def outputAttributes: Seq[Attribute] = inputAttributes

  override def children: Seq[Expression] = Seq(expr)

  override def toString = s"${fullName}:${dataTypeName} := ${expr}"

case class UnresolvedAttribute(
    override val name: Name,
    nodeLocation: Option[NodeLocation]
) extends Attribute:
  override def toString: String = s"UnresolvedAttribute(${fullName})"
  override lazy val resolved    = false

  override def inputAttributes: Seq[Attribute]  = Seq.empty
  override def outputAttributes: Seq[Attribute] = Seq.empty

case class AllColumns(
    override val name: Name,
    columns: Option[Seq[Attribute]],
    nodeLocation: Option[NodeLocation]
) extends Attribute
    with LogSupport:

  override def children: Seq[Expression] =
    // AllColumns is a reference to the input attributes.
    // Return empty so as not to traverse children from here.
    Seq.empty

  override def inputAttributes: Seq[Attribute] =
    columns match
      case Some(columns) =>
        columns.flatMap {
          case a: AllColumns => a.inputAttributes
          case a             => Seq(a)
        }
      case None => Nil

  override def outputAttributes: Seq[Attribute] = inputAttributes

  override def dataType: DataType = columns
    .map(cols => EmbeddedRecordType(cols.map(x => NamedType(x.name, x.dataType))))
    .getOrElse(DataType.UnknownType)

  override def toString =
    columns match
      case Some(attrs) if attrs.nonEmpty =>
        val inputs = attrs
          .map(a => s"${a.fullName}:${a.dataTypeName}").mkString(", ")
        s"AllColumns(${inputs})"
      case _ => s"AllColumns(${fullName})"

  override lazy val resolved = columns.isDefined

case class Alias(
    name: Name,
    expr: Expression,
    nodeLocation: Option[NodeLocation]
) extends Attribute:
  override def inputAttributes: Seq[Attribute]  = Seq(this)
  override def outputAttributes: Seq[Attribute] = inputAttributes

  override def children: Seq[Expression] = Seq(expr)

  override def toString: String = s"<${fullName}> := ${expr}"

  override def dataType: DataType = expr.dataType

/**
  * A single column merged from multiple input expressions (e.g., union, join)
  * @param inputs
  * @param qualifier
  * @param nodeLocation
  */
case class MultiSourceColumn(
    name: Name,
    inputs: Seq[Expression],
    nodeLocation: Option[NodeLocation]
) extends Attribute:
  // require(inputs.nonEmpty, s"The inputs of MultiSourceColumn should not be empty: ${this}", nodeLocation)

  override def toString: String = s"${fullName}:${dataTypeName} := {${inputs.mkString(", ")}}"

  override def inputAttributes: Seq[Attribute] = inputs.map {
    case a: Attribute  => a
    case e: Expression => SingleColumn(name, e, e.nodeLocation)
  }

  override def outputAttributes: Seq[Attribute] = Seq(this)

  override def children: Seq[Expression] =
    // MultiSourceColumn is a reference to the multiple columns. Do not traverse here
    Seq.empty

  override def dataType: DataType = inputs.head.dataType
