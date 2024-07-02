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
package wvlet.lang.model.logical

import wvlet.lang.model.SourceLocation
import wvlet.lang.model.expression.*
import wvlet.lang.model.expression.Expression.SelectItem
import wvlet.lang.model.formatter.{FormatOption, PrintContext}

trait LogicalPlan:
  def modelName: String =
    val n = this.getClass.getSimpleName
    n.stripSuffix("$")
  def nodeLocation: Option[SourceLocation] = None
  def toExpr(context: PrintContext = PrintContext.default): String

  /**
    * Return the child plans of this plan
    * @return
    *   child plans
    */
  def childPlans: Seq[LogicalPlan] = Seq.empty

  def inputAttributes: AttributeList  = ???
  def outputAttributes: AttributeList = ???

object LogicalPlan:

  case class SchemaDef(
      name: String,
      schemaItem: Seq[SchemaItem]
  )(sourceLocation: Option[SourceLocation])
      extends LogicalPlan:
    override def toExpr(context: PrintContext): String =
      val s = new StringBuilder()
      s ++= s"schema ${name}:"
      for x <- schemaItem do
        s ++= context.newline
        s ++= context.indent(1)
        s ++= x.toExpr(context)
      s.result()

  case class SchemaItem(
      name: String,
      typeName: String
  )(sourceLocation: Option[SourceLocation])
      extends Expression:
    override def toExpr(context: PrintContext): String = s"${name}: ${typeName}"

  trait Relation extends LogicalPlan
  trait UnaryRelation extends LogicalPlan:
    def input: Relation

  case class FLOWRQuery(
      forItems: Seq[ForItem],
      whereClause: Option[Where] = None,
      selectClause: Option[Select] = None
  )(override val nodeLocation: Option[SourceLocation] = None)
      extends Relation:

    override def toExpr(context: PrintContext): String =
      val s = new StringBuilder()
      if forItems.nonEmpty then s ++= "for"
      forItems.size match
        case 1 =>
          s ++= " "
          s ++= forItems.head.toExpr(context)
        case _ =>
          for x <- forItems do
            s ++= "\n"
            s ++= context.indent(1)
            s ++= x.toExpr(context)
      whereClause.foreach { w =>
        s ++= context.newline
        s ++= w.toExpr(context)
      }
      selectClause.foreach { r =>
        s ++= context.newline
        s ++= r.toExpr(context)
      }
      context.indentBlock(s.result())

  case class ForItem(id: String, in: Expression)(nodeLocation: Option[SourceLocation]) extends Expression:
    override def toExpr(context: PrintContext): String = s"${id} in ${in.toExpr(context)}"

  case class Where(filterExpr: Expression)(nodeLocation: Option[SourceLocation]) extends Expression:
    override def toExpr(context: PrintContext): String = s"where ${filterExpr.toExpr(context)}"

  case class Select(exprs: Seq[SelectItem])(nodeLocation: Option[SourceLocation]) extends Expression:
    override def toExpr(context: PrintContext): String =
      s"return ${exprs.map(_.toExpr(context)).mkString(", ")}"
