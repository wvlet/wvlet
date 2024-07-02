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
package wvlet.lang.model.expression

import wvlet.airframe.sql.model.Expression.DecimalLiteral
import wvlet.lang.model.SourceLocation
import wvlet.lang.model.formatter.PrintContext

trait Expression:
  def toExpr(context: PrintContext): String
  def sourceLocation: Option[SourceLocation] = None

object Expression:
  case class Identifier(name: String)(nodeLocation: Option[SourceLocation]) extends Expression:
    override def toExpr(context: PrintContext): String = name
  case class QName(names: Seq[String])(nodeLocation: Option[SourceLocation]) extends Expression:
    override def toString                      = names.mkString(".")
    override def toExpr(context: PrintContext) = names.mkString(".")

  case class SelectItem(alias: Option[QName], expr: Expression) extends Expression:
    override def toExpr(context: PrintContext) =
      val exprStr = expr.toExpr(context)
      alias match
        case Some(a) => s"${a}: ${exprStr}"
        case None    => exprStr

  sealed trait Literal extends Expression:
    override def toExpr(context: PrintContext) = stringValue
    def stringValue: String

  case class IntegerLiteral(stringValue: String, value: Int)(sourceLocation: Option[SourceLocation]) extends Literal
  case class LongLiteral(stringValue: String, value: Long)(sourceLocation: Option[SourceLocation])   extends Literal
  case class FloatLiteral(stringValue: String, value: Float)(sourceLocation: Option[SourceLocation]) extends Literal
  case class DoubleLiteral(stringValue: String, value: Double)(
      sourceLocation: Option[SourceLocation]
  ) extends Literal
  case class ExpLiteral(stringValue: String, value: Double)(sourceLocation: Option[SourceLocation]) extends Literal
  case class NullLiteral(stringValue: String)(sourceLocation: Option[SourceLocation])               extends Literal
  sealed trait BooleanLiteral extends Literal:
    def booleanValue: Boolean
  case class TrueLiteral(stringValue: String)(sourceLocation: Option[SourceLocation]) extends BooleanLiteral:
    override def booleanValue = true
  case class FalseLiteral(stringValue: String)(soruceLocation: Option[SourceLocation]) extends BooleanLiteral:
    override def booleanValue = false
  case class DecimalLiteral(stringValue: String, value: BigDecimal)(
      sourceLocation: Option[SourceLocation]
  ) extends Literal
  case class StringLiteral(stringValue: String)(sourceLocation: Option[SourceLocation]) extends Literal

  sealed trait ConditionalExpression extends Expression
  case class Equal(left: Expression, right: Expression) extends ConditionalExpression:
    override def toExpr(context: PrintContext) =
      s"${left.toExpr(context)} = ${right.toExpr(context)}"
  case class NotEqual(left: Expression, right: Expression) extends ConditionalExpression:
    override def toExpr(context: PrintContext) =
      s"${left.toExpr(context)} != ${right.toExpr(context)}"
  case class LessThan(left: Expression, right: Expression) extends ConditionalExpression:
    override def toExpr(context: PrintContext) =
      s"${left.toExpr(context)} < ${right.toExpr(context)}"
  case class LessThanOrEqual(left: Expression, right: Expression) extends ConditionalExpression:
    override def toExpr(context: PrintContext) =
      s"${left.toExpr(context)} <= ${right.toExpr(context)}"
  case class GreaterThan(left: Expression, right: Expression) extends ConditionalExpression:
    override def toExpr(context: PrintContext) =
      s"${left.toExpr(context)} > ${right.toExpr(context)}"
  case class GreaterThanOrEqual(left: Expression, right: Expression) extends ConditionalExpression:
    override def toExpr(context: PrintContext) =
      s"${left.toExpr(context)} >= ${right.toExpr(context)}"
