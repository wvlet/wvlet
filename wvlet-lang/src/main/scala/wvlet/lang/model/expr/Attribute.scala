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
package wvlet.lang.model.expr

import wvlet.lang.model.DataType.{EmbeddedRecordType, NamedType}
import wvlet.lang.model.{DataType, NodeLocation}
import wvlet.lang.compiler.Name
import wvlet.airframe.ulid.ULID
import wvlet.lang.compiler.parser.Span
import wvlet.lang.compiler.parser.Span.NoSpan
import wvlet.log.LogSupport

/**
  * Attribute is used for column names of relational table inputs and outputs
  */
trait Attribute extends LeafExpression with LogSupport:
  override def attributeName: String = nameExpr.leafName

  def nameExpr: NameExpr
  def fullName: String

  def typeDescription: String = dataTypeName

  def alias: Option[NameExpr] =
    this match
      case a: Alias =>
        Some(a.nameExpr)
      case _ =>
        None

  // def withDataType(newDataType: DataType): Attribute

  def withAlias(newAlias: NameExpr): Attribute = withAlias(Some(newAlias))

  def withAlias(newAlias: Option[NameExpr]): Attribute =
    newAlias match
      case None =>
        this
      case Some(alias) =>
        this match
          case a: Alias =>
            if nameExpr != alias then
              a.copy(nameExpr = alias)
            else
              a
          case other if other.nameExpr == alias =>
            // No need to have alias
            other
          case other =>
            Alias(alias, other, NoSpan)

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

end Attribute

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
  override def fullName: String   = attr.fullName
  override def nameExpr: NameExpr = attr.nameExpr
  override def toString: String   = s"AttributeRef(${attr})"

  override def span: Span = attr.span

  // override def withDataType(newDataType: DataType): Attribute = this.copy(attr = attr.withDataType(newDataType))

  override def inputAttributes: Seq[Attribute]  = attr.inputAttributes
  override def outputAttributes: Seq[Attribute] = attr.inputAttributes

  override def hashCode(): Int = super.hashCode()
  override def equals(obj: Any): Boolean =
    obj match
      case that: AttributeRef =>
        that.attr == this.attr
      case _ =>
        false

/**
  * An attribute that produces a single column value with a given expression.
  *
  * @param expr
  * @param qualifier
  * @param nodeLocation
  */
case class SingleColumn(override val nameExpr: NameExpr, expr: Expression, span: Span)
    extends Attribute:
  override def fullName: String   = nameExpr.fullName
  override def dataType: DataType = expr.dataType

  override def inputAttributes: Seq[Attribute] = Seq(this)

  override def outputAttributes: Seq[Attribute] = inputAttributes

  override def children: Seq[Expression] = Seq(expr)

  override def toString = s"${fullName}:${dataTypeName} := ${expr}"

case class UnresolvedAttribute(override val nameExpr: NameExpr, span: Span) extends Attribute:
  override def fullName: String = nameExpr.fullName
  override def toString: String = s"UnresolvedAttribute(${fullName})"
  override lazy val resolved    = false

  override def inputAttributes: Seq[Attribute]  = Seq.empty
  override def outputAttributes: Seq[Attribute] = Seq.empty

case class AllColumns(override val nameExpr: NameExpr, columns: Option[Seq[Attribute]], span: Span)
    extends Attribute
    with LogSupport:
  override def fullName: String = nameExpr.fullName

  override def children: Seq[Expression] =
    // AllColumns is a reference to the input attributes.
    // Return empty so as not to traverse children from here.
    Seq.empty

  override def inputAttributes: Seq[Attribute] =
    columns match
      case Some(columns) =>
        columns.flatMap {
          case a: AllColumns =>
            a.inputAttributes
          case a =>
            Seq(a)
        }
      case None =>
        Nil

  override def outputAttributes: Seq[Attribute] = inputAttributes

  override def dataType: DataType = columns
    .map(cols =>
      EmbeddedRecordType(cols.map(x => NamedType(Name.termName(x.nameExpr.leafName), x.dataType)))
    )
    .getOrElse(DataType.UnknownType)

  override def toString =
    columns match
      case Some(attrs) if attrs.nonEmpty =>
        val inputs = attrs.map(a => s"${a.fullName}:${a.dataTypeName}").mkString(", ")
        s"AllColumns(${inputs})"
      case _ =>
        s"AllColumns(${fullName})"

  override lazy val resolved = columns.isDefined

end AllColumns

case class Alias(nameExpr: NameExpr, expr: Expression, span: Span) extends Attribute:
  override def fullName: String = nameExpr.fullName

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
case class MultiSourceColumn(nameExpr: NameExpr, inputs: Seq[Expression], span: Span)
    extends Attribute:
  // require(inputs.nonEmpty, s"The inputs of MultiSourceColumn should not be empty: ${this}", nodeLocation)
  override def fullName: String = nameExpr.fullName
  override def toString: String = s"${fullName}:${dataTypeName} := {${inputs.mkString(", ")}}"

  override def inputAttributes: Seq[Attribute] = inputs.map {
    case a: Attribute =>
      a
    case e: Expression =>
      SingleColumn(nameExpr, e, e.span)
  }

  override def outputAttributes: Seq[Attribute] = Seq(this)

  override def children: Seq[Expression] =
    // MultiSourceColumn is a reference to the multiple columns. Do not traverse here
    Seq.empty

  override def dataType: DataType = inputs.head.dataType
