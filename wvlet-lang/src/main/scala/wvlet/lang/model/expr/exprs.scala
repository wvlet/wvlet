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

import wvlet.lang.api.Span
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.compiler.parser.SqlToken
import wvlet.lang.compiler.{Name, TermName, TypeName}
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.*
import wvlet.lang.model.expr.NameExpr.requiresQuotation
import wvlet.lang.model.plan.*

/**
  * Native expression for running code implemented in Scala
  * @param name
  * @param nodeLocation
  */
case class NativeExpression(name: String, retType: Option[DataType], span: Span)
    extends LeafExpression:
  override def dataType: DataType = retType.getOrElse(DataType.UnknownType)

/**
  */
case class ParenthesizedExpression(child: Expression, span: Span) extends UnaryExpression:
  override def dataType: DataType = child.dataType

case class TypedExpression(child: Expression, tpe: DataType, span: Span) extends UnaryExpression:
  override def dataType: DataType = tpe

case class TableAlias(name: NameExpr, alias: NameExpr, span: Span) extends LeafExpression:
  override def dataType: DataType = DataType.UnknownType

/**
  * variable name, function name, type name, etc. The name might have a qualifier.
  */
sealed trait NameExpr extends Expression:
  // name parts
  def nameParts: List[String] = List(leafName)
  def leafName: String        = nameParts.last
  def fullName: String        = nameParts.mkString(".")

  def nonLeafName: String = fullName.stripSuffix(s".${leafName}")
  def nonEmpty: Boolean   = !isEmpty
  def isEmpty: Boolean =
    // TODO: This part is a bit ad-hoc as EmptyName can be copied during the tree transformation, so
    // we can't use the object equality like this eq EmptyName
    this.leafName == "<empty>"

  def isGroupingKeyIndex: Boolean = "_[0-9]+".r.matches(fullName)

  def toTermName: TermName = Name.termName(leafName)
  def toTypeName: TypeName = Name.typeName(leafName)

  def map[A](f: Expression => A): Option[A] =
    if this.isEmpty then
      None
    else
      Some(f(this))

  def toSQLAttributeName: String = nameParts
    .map { s =>
      if s.startsWith("\"") && s.endsWith("\"") then
        s
      else if requiresQuotation(s) then
        s""""${s}""""
      else
        s
    }
    .mkString(".")

  def toWvletAttributeName: String = nameParts
    .map { s =>
      if s.startsWith("`") && s.endsWith("`") then
        s
      else if requiresQuotation(s) then
        s"""`${s}`"""
      else
        s
    }
    .mkString(".")

end NameExpr

object NameExpr:
  val EmptyName: Identifier                                = UnquotedIdentifier("<empty>", NoSpan)
  def fromString(s: String, span: Span = NoSpan): NameExpr = UnquotedIdentifier(s, span)

  private val sqlKeywords = SqlToken.keywords.map(_.str).toSet

  def requiresQuotation(s: String): Boolean =
    !s.matches("^[\\*_a-zA-Z][_a-zA-Z0-9\\*\\.]*$") || sqlKeywords.contains(s)

case class Wildcard(span: Span) extends LeafExpression with Identifier:
  override def unquotedValue: String = "*"

  override def qualifier: Expression = NameExpr.EmptyName

/**
  * Reference to the current input, denoted by '_'
  *
  * @param dataType
  * @param nodeLocation
  */
case class ContextInputRef(override val dataType: DataType, span: Span)
    extends LeafExpression
    with Identifier:
  override def unquotedValue: String = "_"

/**
  * Name with qualifier
  */
abstract trait QualifiedName extends NameExpr:
  def qualifier: Expression

/**
  * Refer to the name that can be from the base expression: (qualifier).(name)
  * @param qualifier
  * @param name
  * @param dataType
  * @param nodeLocation
  */
case class DotRef(
    qualifier: Expression,
    name: NameExpr,
    override val dataType: DataType,
    span: Span
) extends QualifiedName:
  override def nameParts: List[String] =
    qualifier match
      case q: NameExpr =>
        q.nameParts ++ name.nameParts
      case _ =>
        List(qualifier.toString) ++ name.nameParts

  override def toString: String          = s"DotRef(${qualifier}:${qualifier.dataType},${name})"
  override def children: Seq[Expression] = Seq(qualifier)

sealed trait Identifier extends QualifiedName with LeafExpression:
  override def qualifier: Expression   = NameExpr.EmptyName
  override def nameParts: List[String] = List(unquotedValue)

  // Unquoted value
  def unquotedValue: String

  override def attributeName: String = leafName

  override lazy val resolved: Boolean = false
  def toResolved(dataType: DataType): Identifier = ResolvedIdentifier(
    this.unquotedValue,
    dataType,
    span
  )

case class ResolvedIdentifier(
    override val unquotedValue: String,
    override val dataType: DataType,
    span: Span
) extends Identifier:
  override def toResolved(dataType: DataType) =
    if this.dataType == dataType then
      this
    else
      this.copy(dataType = dataType)

  override lazy val resolved: Boolean = dataType.isResolved

// Used for group by 1, 2, 3 ...
case class DigitIdentifier(override val unquotedValue: String, span: Span) extends Identifier

case class UnquotedIdentifier(override val unquotedValue: String, span: Span) extends Identifier

/**
  * Double quoted indentifier like "(column name)" for SQL. In Wvlet, use BackQuotedIdentifier
  * @param unquotedValue
  * @param span
  */
case class DoubleQuotedIdentifier(override val unquotedValue: String, span: Span) extends Identifier

/**
  * Backquote is used for table or column names that conflicts with reserved words
  * @param unquotedValue
  * @param nodeLocation
  */
case class BackQuotedIdentifier(
    override val unquotedValue: String,
    override val dataType: DataType,
    span: Span
) extends Identifier:
  override def leafName: String                                     = unquotedValue
  override def fullName: String                                     = unquotedValue
  override def toResolved(dataType: DataType): BackQuotedIdentifier = this.copy(dataType = dataType)

case class BackquoteInterpolatedIdentifier(
    prefix: NameExpr,
    parts: List[Expression],
    override val dataType: DataType,
    span: Span
) extends Identifier:
  override def children: Seq[Expression] = parts
  override def fullName: String          = "<backquote interpolation>"
  override def unquotedValue: String     = "<backquote interpolation>"

sealed trait JoinCriteria extends Expression
case object NoJoinCriteria extends JoinCriteria with LeafExpression:
  override def span: Span = NoSpan

case class NaturalJoin(span: Span) extends JoinCriteria with LeafExpression:
  override def toString: String = "NaturalJoin"

sealed trait JoinOnTheSameColumns extends JoinCriteria:
  def columns: List[NameExpr]

case class JoinUsing(columns: List[NameExpr], span: Span) extends JoinOnTheSameColumns:
  override def children: Seq[Expression] = columns

case class ResolvedJoinUsing(keys: List[MultiSourceColumn], span: Span)
    extends JoinOnTheSameColumns:
  override def columns: List[NameExpr] = keys.map { k =>
    k.nameExpr
  }

  override def children: Seq[Expression] = columns
  override lazy val resolved: Boolean    = true

case class JoinOn(expr: Expression, span: Span) extends JoinCriteria with UnaryExpression:
  override def child: Expression = expr
  override def toString: String  = s"JoinOn(${expr})"

/**
  * Join condition used only when join keys are resolved
  */
case class JoinOnEq(keys: Seq[Expression], span: Span) extends JoinCriteria with LeafExpression:
  // require(keys.forall(_.resolved), s"all keys of JoinOnEq must be resolved: ${keys}", nodeLocation)

  override def children: Seq[Expression] = keys

case class SortItem(
    sortKey: Expression,
    ordering: Option[SortOrdering] = None,
    nullOrdering: Option[NullOrdering],
    span: Span
) extends Expression
    with UnaryExpression:
  override def dataType: DataType = sortKey.dataType
  override def child: Expression  = sortKey

// Sort ordering
enum SortOrdering(val expr: String):
  case Ascending  extends SortOrdering("asc")
  case Descending extends SortOrdering("desc")

enum NullOrdering(val expr: String):
  case NullIsFirst    extends NullOrdering("nulls first")
  case NullIsLast     extends NullOrdering("nulls last")
  case UndefinedOrder extends NullOrdering("")

// Window functions
case class Window(
    partitionBy: List[Expression],
    orderBy: List[SortItem],
    frame: Option[WindowFrame],
    span: Span
) extends Expression:
  override def children: Seq[Expression] = partitionBy ++ orderBy ++ frame.toSeq

enum FrameType(val expr: String):
  case RangeFrame extends FrameType("range")
  case RowsFrame  extends FrameType("rows")

enum FrameBound(val expr: String, val wvExpr: String):
  case UnboundedPreceding extends FrameBound("unbounded preceding", "")
  case UnboundedFollowing extends FrameBound("unbounded following", "")
  case Preceding(n: Long) extends FrameBound(s"${n} preceding", s"-${n}")
  case Following(n: Long) extends FrameBound(s"${n} following", s"${n}")
  case CurrentRow         extends FrameBound("current row", "0")

case class WindowFrame(frameType: FrameType, start: FrameBound, end: FrameBound, span: Span)
    extends Expression
    with LeafExpression:

  override def toString: String =
    val s = Seq.newBuilder[String]
    s += frameType.toString
    s += "BETWEEN"
    s += start.toString
    s += "AND"
    s += end.toString
    s.result().mkString(" ")

// Function
case class FunctionApply(
    base: Expression,
    args: List[FunctionArg],
    window: Option[Window],
    span: Span
) extends Expression:
  override def children: Seq[Expression] = Seq(base) ++ args ++ window.toSeq
  override def dataType: DataType        = base.dataType

case class WindowApply(base: Expression, window: Window, span: Span) extends Expression:
  override def children: Seq[Expression] = Seq(base, window)
  override def dataType: DataType        = base.dataType

case class FunctionArg(
    name: Option[TermName],
    value: Expression,
    isDistinct: Boolean,
    orderBy: List[SortItem] = Nil,
    span: Span
) extends Expression:
  override def children: Seq[Expression] = value +: orderBy
  override def dataType: DataType        = value.dataType

case class ArrayAccess(arrayExpr: Expression, index: Expression, span: Span) extends Expression:
  override def children: Seq[Expression] = Seq(arrayExpr, index)

case class LambdaExpr(args: List[Identifier], body: Expression, span: Span)
    extends Expression
    with UnaryExpression:
  def child = body

case class ListExpr(exprs: List[Expression], span: Span) extends Expression:
  override def children: Seq[Expression] = exprs

//case class Ref(name: QName, nodeLocation: Option[NodeLocation]) extends Expression with LeafExpression

// Conditional expression
sealed trait ConditionalExpression extends Expression:
  override def dataType: DataType = DataType.BooleanType

case class NoOp(span: Span) extends ConditionalExpression with LeafExpression

case class Eq(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "="

case class NotEq(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "!="
  // require(operatorName == "<>" || operatorName == "!=", "NotEq.operatorName must be either <> or !=", nodeLocation)

sealed trait LogicalConditionalExpression extends ConditionalExpression with BinaryExpression

case class And(left: Expression, right: Expression, span: Span)
    extends LogicalConditionalExpression:
  override def operatorName: String = "and"

case class Or(left: Expression, right: Expression, span: Span) extends LogicalConditionalExpression:
  override def operatorName: String = "or"

case class Not(child: Expression, span: Span) extends ConditionalExpression with UnaryExpression

case class LessThan(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "<"

case class LessThanOrEq(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "<="

case class GreaterThan(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = ">"

case class GreaterThanOrEq(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = ">="

case class Between(e: Expression, a: Expression, b: Expression, span: Span)
    extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(e, a, b)

case class NotBetween(e: Expression, a: Expression, b: Expression, span: Span)
    extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(e, a, b)

case class IsNull(child: Expression, span: Span) extends ConditionalExpression with UnaryExpression:
  override def toString: String = s"IsNull(${child})"

case class IsNotNull(child: Expression, span: Span)
    extends ConditionalExpression
    with UnaryExpression:
  override def toString: String = s"IsNotNull(${child})"

case class In(a: Expression, list: List[Expression], span: Span) extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ list

case class NotIn(a: Expression, list: List[Expression], span: Span) extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ list

case class InRelation(a: Expression, in: Relation, span: Span) extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ in.childExpressions

case class NotInRelation(a: Expression, in: Relation, span: Span) extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ in.childExpressions

case class TupleIn(tuple: Expression, list: List[Expression], span: Span)
    extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(tuple) ++ list

case class TupleNotIn(tuple: Expression, list: List[Expression], span: Span)
    extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(tuple) ++ list

case class TupleInRelation(tuple: Expression, in: Relation, span: Span)
    extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(tuple) ++ in.childExpressions

case class TupleNotInRelation(tuple: Expression, in: Relation, span: Span)
    extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(tuple) ++ in.childExpressions

case class Like(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "like"

case class NotLike(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "not like"

case class Contains(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "contains"

case class DistinctFrom(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "is distinct from"

case class NotDistinctFrom(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "is not distinct from"

case class IfExpr(cond: Expression, onTrue: Expression, onFalse: Expression, span: Span)
    extends Expression:
  override def children: Seq[Expression] = Seq(cond, onTrue, onFalse)

case class CaseExpr(
    target: Option[Expression],
    whenClauses: List[WhenClause],
    elseClause: Option[Expression],
    span: Span
) extends Expression:

  override def children: Seq[Expression] =
    val b = Seq.newBuilder[Expression]
    target.foreach(b += _)
    b ++= whenClauses
    elseClause.foreach(b += _)
    b.result()

case class WhenClause(condition: Expression, result: Expression, span: Span) extends Expression:
  override def children: Seq[Expression] = Seq(condition, result)

case class Exists(child: Expression, span: Span) extends Expression with UnaryExpression

enum TestType(val expr: String):
  case ShouldBe         extends TestType("should be")
  case ShouldNotBe      extends TestType("should not be")
  case ShouldContain    extends TestType("should contain")
  case ShouldNotContain extends TestType("should not contain")

case class ShouldExpr(testType: TestType, left: Expression, right: Expression, span: Span)
    extends Expression:
  override def dataType: DataType        = DataType.BooleanType
  override def children: Seq[Expression] = Seq(left, right)

// Arithmetic expr
enum BinaryExprType(val expr: String):
  case Add       extends BinaryExprType("+")
  case Subtract  extends BinaryExprType("-")
  case Multiply  extends BinaryExprType("*")
  case Divide    extends BinaryExprType("/")
  case DivideInt extends BinaryExprType("//")
  case Modulus   extends BinaryExprType("%")

sealed trait ArithmeticExpression extends Expression

case class ArithmeticBinaryExpr(
    exprType: BinaryExprType,
    left: Expression,
    right: Expression,
    span: Span
) extends ArithmeticExpression
    with BinaryExpression:

  override def dataType: DataType =
    left.dataType match
      case l if l == right.dataType =>
        l
      case DataType.IntType =>
        right.dataType match
          case DataType.BooleanType =>
            DataType.IntType
          case DataType.IntType =>
            DataType.IntType
          case DataType.LongType =>
            DataType.LongType
          case DataType.FloatType =>
            DataType.FloatType
          case DataType.DoubleType =>
            DataType.DoubleType
          case DataType.DecimalType(_, _) =>
            right.dataType
          case _ =>
            DataType.UnknownType
      case DataType.LongType =>
        right.dataType match
          case DataType.BooleanType =>
            DataType.LongType
          case DataType.IntType =>
            DataType.IntType
          case DataType.LongType =>
            DataType.LongType
          case DataType.FloatType =>
            DataType.FloatType
          case DataType.DoubleType =>
            DataType.DoubleType
          case DataType.DecimalType(_, _) =>
            right.dataType
          case _ =>
            DataType.UnknownType
      case DataType.FloatType =>
        right.dataType match
          case DataType.BooleanType =>
            DataType.FloatType
          case DataType.IntType =>
            DataType.FloatType
          case DataType.LongType =>
            DataType.FloatType
          case DataType.DoubleType =>
            DataType.DoubleType
          case DataType.DecimalType(_, _) =>
            right.dataType
          case _ =>
            DataType.UnknownType
      case DataType.DoubleType =>
        right.dataType match
          case DataType.BooleanType =>
            DataType.DoubleType
          case DataType.IntType =>
            DataType.DoubleType
          case DataType.LongType =>
            DataType.DoubleType
          case DataType.FloatType =>
            DataType.DoubleType
          case DataType.DecimalType(_, _) =>
            right.dataType
          case _ =>
            DataType.UnknownType
      case DataType.BooleanType =>
        right.dataType match
          case DataType.IntType =>
            DataType.IntType
          case DataType.LongType =>
            DataType.LongType
          case DataType.FloatType =>
            DataType.FloatType
          case DataType.DoubleType =>
            DataType.DoubleType
          case DataType.DecimalType(_, _) =>
            right.dataType
          case _ =>
            DataType.UnknownType
      case _ =>
        DataType.UnknownType

  override def operatorName: String = exprType.expr.toLowerCase

  override def toString: String = s"${exprType}(left:$left, right:$right)"

end ArithmeticBinaryExpr

case class ArithmeticUnaryExpr(sign: Sign, child: Expression, span: Span)
    extends ArithmeticExpression
    with UnaryExpression

enum Sign(val symbol: String):
  case NoSign   extends Sign("")
  case Positive extends Sign("+")
  case Negative extends Sign("-")

// Set quantifier
sealed trait SetQuantifier extends LeafExpression:
  def isDistinct: Boolean
  override def toString: String = getClass.getSimpleName

case class All(span: Span) extends SetQuantifier:
  override def isDistinct: Boolean = false

case class DistinctSet(span: Span) extends SetQuantifier:
  override def toString: String    = "DISTINCT"
  override def isDistinct: Boolean = true

case class This(override val dataType: DataType, span: Span) extends LeafExpression

// Literal
sealed trait Literal extends Expression:
  /**
    * String representation of the literal, which may include quotation.
    *
    * This is for preserving the original representation of the literal (e.g., "0.123")
    * @return
    */
  def stringValue: String

  /**
    * SQL representation of this literal
    * @return
    */
  def sqlExpr: String       = stringValue
  def unquotedValue: String = stringValue

case class NullLiteral(span: Span) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.NullType
  override def stringValue: String = "null"

sealed trait BooleanLiteral extends Literal:
  override def dataType: DataType = DataType.BooleanType
  def booleanValue: Boolean

case class TrueLiteral(span: Span) extends BooleanLiteral with LeafExpression:
  override def stringValue: String   = "true"
  override def booleanValue: Boolean = true

case class FalseLiteral(span: Span) extends BooleanLiteral with LeafExpression:
  override def stringValue: String   = "false"
  override def booleanValue: Boolean = false

sealed trait StringLiteral extends Literal with LeafExpression:
  override def dataType: DataType = DataType.StringType

object StringLiteral:
  def fromString(s: String, span: Span = NoSpan): StringLiteral =
    if s.contains("\n") then
      TripleQuoteString(s, span)
    else if s.contains("\"") then
      SingleQuoteString(s, span)
    else
      DoubleQuoteString(s, span)

case class SingleQuoteString(override val unquotedValue: String, span: Span) extends StringLiteral:
  override def stringValue: String =
    // Need to escape `'` inside the string for SQL
    s"'${unquotedValue.replaceAll("'", "''")}'"

  override def sqlExpr: String = stringValue

case class DoubleQuoteString(override val unquotedValue: String, span: Span) extends StringLiteral:
  override def stringValue: String = s""""${unquotedValue}""""
  override def sqlExpr: String =
    // In SQL, double quote string means identifiers
    // So replace double quote with single quote
    s"'${unquotedValue.replaceAll("'", "''")}'"

case class TripleQuoteString(override val unquotedValue: String, span: Span) extends StringLiteral:
  override def stringValue: String = s"\"\"\"${unquotedValue}\"\"\""
  override def sqlExpr: String =
    // SQL doesn't support multi-line triple quotes,
    // So split the string into multiple lines
    val lines = unquotedValue.split("\n")
    val parts: List[String] =
      lines
        .map { line =>
          StringLiteral.fromString(line, span).sqlExpr
        }
        .toList
    parts.mkString(" || ")

case class StringPart(value: String, span: Span) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.StringType
  override def stringValue: String = value

case class JsonLiteral(value: String, span: Span) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.JsonType
  override def stringValue: String = value

case class TimeLiteral(value: String, span: Span) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.TimestampType(TimestampField.TIME, false)
  override def stringValue: String = value

case class TimestampLiteral(value: String, span: Span) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.TimestampType(TimestampField.TIMESTAMP, false)
  override def stringValue: String = value

case class DecimalLiteral(value: String, override val stringValue: String, span: Span)
    extends Literal
    with LeafExpression:
  override lazy val dataType: DataType =
    value.split("\\.") match
      case Array(decimal, frac) =>
        val p = decimal.length + frac.length
        val s = frac.length
        DataType.DecimalType(IntConstant(p), IntConstant(s))
      case Array(decimal) =>
        DataType.DecimalType(IntConstant(decimal.length), IntConstant(0))
      case _ =>
        DataType.DecimalType(
          TypeVariable(Name.typeName("precision")),
          TypeVariable(Name.typeName("scale"))
        )

case class CharLiteral(value: String, span: Span) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.CharType(None)
  override def stringValue: String = value

case class DoubleLiteral(value: Double, override val stringValue: String, span: Span)
    extends Literal
    with LeafExpression:
  override def dataType: DataType = DataType.DoubleType
  override def sqlExpr: String    = value.toString

case class LongLiteral(value: Long, override val stringValue: String, span: Span)
    extends Literal
    with LeafExpression:
  override def dataType: DataType = DataType.LongType
  override def sqlExpr: String    = value.toString

case class GenericLiteral(tpe: DataType, value: String, span: Span)
    extends Literal
    with LeafExpression:
  override def stringValue: String = value
  override def sqlExpr             = s"${tpe.typeName} ${value}"

case class BinaryLiteral(binary: String, span: Span) extends Literal with LeafExpression:
  override def stringValue: String = binary

case class IntervalLiteral(
    value: String,
    sign: Sign,
    startField: IntervalField,
    end: Option[IntervalField],
    span: Span
) extends Literal:
  override def children: Seq[Expression] = Nil
  override def stringValue: String =
    if end.isEmpty then
      if sign == Sign.NoSign then
        s"'${value}' ${startField}"
      else
        s"${sign.symbol} '${value}' ${startField}"
    else
      s"${sign.symbol} between '${value}' ${startField} and ${end.get}"

  override def sqlExpr: String = s"interval ${stringValue}"

object IntervalField:
  def unapply(name: String): Option[IntervalField] = IntervalField.values.find(_.name == name)

enum IntervalField(val name: String):
  override def toString: String = name

  case Year           extends IntervalField("year")
  case Quarter        extends IntervalField("quarter")
  case Month          extends IntervalField("month")
  case Week           extends IntervalField("week")
  case Day            extends IntervalField("day")
  case DayOfWeek      extends IntervalField("day_of_week")
  case DayOfYear      extends IntervalField("day_of_year")
  case YearOfWeek     extends IntervalField("year_of_week")
  case Hour           extends IntervalField("hour")
  case Minute         extends IntervalField("minute")
  case Second         extends IntervalField("second")
  case TimezoneHour   extends IntervalField("timezone_hour")
  case TimezoneMinute extends IntervalField("timezone_minute")

// Value constructor
case class ArrayConstructor(values: List[Expression], span: Span) extends Expression:

  def elementType: DataType =
    val elemTypes = values.map(_.dataType).distinct
    if elemTypes.size == 1 then
      elemTypes.head
    else
      AnyType

  override def dataType: DataType        = ArrayType(elementType)
  override def children: Seq[Expression] = values

case class RowConstructor(values: List[Expression], span: Span) extends Expression:
  override def dataType: DataType        = EmbeddedRecordType(values.map(_.dataType))
  override def children: Seq[Expression] = values
  override def toString: String          = s"Row(${values.mkString(", ")})"

case class StructValue(fields: List[StructField], span: Span) extends Expression:
  override def children: Seq[Expression] = fields

case class StructField(name: String, value: Expression, span: Span) extends Expression:
  override def children: Seq[Expression] = Seq(value)

case class MapValue(entries: List[MapEntry], span: Span) extends Expression:
  override def children: Seq[Expression] = entries

case class MapEntry(key: Expression, value: Expression, span: Span) extends Expression:
  override def children: Seq[Expression] = Seq(key, value)

abstract sealed class CurrentTimeBase(name: String, precision: Option[Int]) extends LeafExpression

case class CurrentTime(precision: Option[Int], span: Span)
    extends CurrentTimeBase("current_time", precision)

case class CurrentDate(precision: Option[Int], span: Span)
    extends CurrentTimeBase("current_date", precision)

case class CurrentTimestamp(precision: Option[Int], span: Span)
    extends CurrentTimeBase("current_timestamp", precision)

case class CurrentLocalTime(precision: Option[Int], span: Span)
    extends CurrentTimeBase("localtime", precision)

case class CurrentLocalTimeStamp(precision: Option[Int], span: Span)
    extends CurrentTimeBase("localtimestamp", precision)

// 1-origin parameter
case class Parameter(index: Int, span: Span) extends LeafExpression

// Named parameter for prepared statements (e.g., $name)
case class NamedParameter(name: String, span: Span) extends LeafExpression

case class SubQueryExpression(query: Relation, span: Span) extends Expression:
  override def children: Seq[Expression] = query.childExpressions

case class Cast(expr: Expression, tpe: DataType, tryCast: Boolean = false, span: Span)
    extends UnaryExpression:
  override def child: Expression = expr

case class SchemaProperty(key: NameExpr, value: Expression, span: Span) extends Expression:
  override def children: Seq[Expression] = Seq(key, value)

sealed trait TableElement extends Expression

case class ColumnDef(
    columnName: NameExpr,
    tpe: DataType,
    span: Span,
    notNull: Boolean = false,
    comment: Option[String] = None,
    defaultValue: Option[Expression] = None,
    properties: List[(NameExpr, Expression)] = Nil,
    position: Option[String] = None // FIRST, LAST, or AFTER column_name
) extends TableElement
    with UnaryExpression:
  override def toString: String  = s"${columnName.leafName}:${tpe.wvExpr}"
  override def child: Expression = columnName
//
//case class ColumnType(tpe: NameExpr, span: Span) extends LeafExpression
//
//case class ColumnDefLike(tableName: NameExpr, includeProperties: Boolean, span: Span)
//    extends TableElement
//    with UnaryExpression:
//  override def child: Expression = tableName

// Aggregation
trait GroupingKey extends UnaryExpression:
  def name: NameExpr
  def index: Option[Int]
  override def child: Expression

case class UnresolvedGroupingKey(name: NameExpr, child: Expression, span: Span) extends GroupingKey:
  override def dataType: DataType = child.dataType
  override def index: Option[Int] = None
  override def toString: String = s"GroupingKey(${index.map(i => s"${i}:").getOrElse("")}${child})"
  override lazy val resolved: Boolean = child.dataType.isResolved

case class Extract(interval: IntervalField, expr: Expression, span: Span) extends Expression:
  override def children: Seq[Expression] = Seq(expr)

case class InterpolatedString(
    prefix: NameExpr,
    parts: List[Expression],
    override val dataType: DataType,
    isTripleQuote: Boolean,
    span: Span
) extends Expression:
  override def children: Seq[Expression] = parts
