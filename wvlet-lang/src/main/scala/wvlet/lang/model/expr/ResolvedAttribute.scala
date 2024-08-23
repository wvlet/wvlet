package wvlet.lang.model.expr

import wvlet.lang.catalog.Catalog
import wvlet.lang.compiler.{Name, TermName}
import wvlet.lang.model.{DataType, NodeLocation}
import wvlet.lang.model.plan.*
import wvlet.log.LogSupport

case class SourceColumn(table: Catalog.TableDef, column: Catalog.TableColumn):
  def fullName: String = s"${table.name}.${column.name}"

case class ResolvedAttribute(
    name: TermName,
    override val dataType: DataType,
    // If this attribute directly refers to a table column, its source column will be set.
    sourceColumn: Option[SourceColumn],
    nodeLocation: Option[NodeLocation]
) extends Attribute
    with LogSupport:

  override def nameExpr: NameExpr = NameExpr.fromString(name.name)
  override def fullName: String   = nameExpr.toString
  override lazy val resolved      = true

  override def inputAttributes: Seq[Attribute]  = Seq(this)
  override def outputAttributes: Seq[Attribute] = inputAttributes

  override def toString =
    sourceColumn match
      case Some(c) =>
        s"*${typeDescription} <- ${c.fullName}"
      case None =>
        s"${nameExpr}: *${typeDescription}"

  override def sourceColumns: Seq[SourceColumn] = sourceColumn.toSeq
