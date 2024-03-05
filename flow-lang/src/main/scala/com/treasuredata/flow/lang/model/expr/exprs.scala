package com.treasuredata.flow.lang.model.expr

import com.treasuredata.flow.lang.model.DataType.{AnyType, ArrayType, EmbeddedRecordType, TimestampField, TypeVariable}
import com.treasuredata.flow.lang.model.{DataType, NodeLocation}
import com.treasuredata.flow.lang.model.plan.*

import java.util.Locale

/**
  */
case class ParenthesizedExpression(child: Expression, nodeLocation: Option[NodeLocation]) extends UnaryExpression

// Qualified name (QName), such as table and column names
case class QName(parts: List[String], nodeLocation: Option[NodeLocation]) extends LeafExpression:
  def fullName: String          = parts.mkString(".")
  override def toString: String = fullName

object QName:
  def apply(s: String, nodeLocation: Option[NodeLocation]): QName =
    QName(s.split("\\.").map(unquote).toList, nodeLocation)

  def unquote(s: String): String =
    if s.startsWith("\"") && s.endsWith("\"") then s.substring(1, s.length - 1)
    else s

case class Dereference(base: Expression, next: Expression, nodeLocation: Option[NodeLocation]) extends Expression:
  override def toString: String          = s"Dereference(${base} => ${next})"
  override def children: Seq[Expression] = Seq(base, next)

sealed trait Identifier extends LeafExpression:
  def value: String
  def expr: String
  override def attributeName: String  = value
  override lazy val resolved: Boolean = false
  def toResolved: ResolvedIdentifier  = ResolvedIdentifier(this)

case class ResolvedIdentifier(id: Identifier) extends Identifier:
  override def value: String                      = id.value
  override def expr: String                       = id.expr
  override def toResolved: ResolvedIdentifier     = super.toResolved
  override def nodeLocation: Option[NodeLocation] = id.nodeLocation
  override lazy val resolved: Boolean             = true

case class DigitId(value: String, nodeLocation: Option[NodeLocation]) extends Identifier:
  override def toString: String = s"Id(${value})"
  override def expr: String     = value

case class UnquotedIdentifier(value: String, nodeLocation: Option[NodeLocation]) extends Identifier:
  override def toString: String = s"Id(${value})"
  override def expr: String     = value

case class BackQuotedIdentifier(value: String, nodeLocation: Option[NodeLocation]) extends Identifier:
  override def toString     = s"Id(`${value}`)"
  override def expr: String = s"`${value}`"

case class QuotedIdentifier(value: String, nodeLocation: Option[NodeLocation]) extends Identifier:
  override def toString     = s"""Id("${value}")"""
  override def expr: String = s"\"${value}\""

sealed trait JoinCriteria extends Expression

case class NaturalJoin(nodeLocation: Option[NodeLocation]) extends JoinCriteria with LeafExpression:
  override def toString: String = "NaturalJoin"

case class JoinUsing(columns: Seq[Identifier], nodeLocation: Option[NodeLocation]) extends JoinCriteria:
  override def children: Seq[Expression] = columns
  override def toString: String          = s"JoinUsing(${columns.mkString(",")})"

case class ResolvedJoinUsing(keys: Seq[MultiSourceColumn], nodeLocation: Option[NodeLocation]) extends JoinCriteria:
  override def children: Seq[Expression] = keys
  override def toString: String          = s"ResolvedJoinUsing(${keys.mkString(",")})"
  override lazy val resolved: Boolean    = true

case class JoinOn(expr: Expression, nodeLocation: Option[NodeLocation]) extends JoinCriteria with UnaryExpression:
  override def child: Expression = expr
  override def toString: String  = s"JoinOn(${expr})"

/**
  * Join condition used only when join keys are resolved
  */
case class JoinOnEq(keys: Seq[Expression], nodeLocation: Option[NodeLocation]) extends JoinCriteria with LeafExpression:
  // require(keys.forall(_.resolved), s"all keys of JoinOnEq must be resolved: ${keys}", nodeLocation)

  override def children: Seq[Expression] = keys

  override def toString: String = s"JoinOnEq(${keys.mkString(", ")})"

case class SortItem(
    sortKey: Expression,
    ordering: Option[SortOrdering] = None,
    nullOrdering: Option[NullOrdering],
    nodeLocation: Option[NodeLocation]
) extends Expression
    with UnaryExpression:
  override def child: Expression = sortKey

  override def toString: String = s"SortItem(sortKey:${sortKey}, ordering:${ordering}, nullOrdering:${nullOrdering})"

// Sort ordering
sealed trait SortOrdering

object SortOrdering:
  case object Ascending extends SortOrdering:
    override def toString = "ASC"

  case object Descending extends SortOrdering:
    override def toString = "DESC"

sealed trait NullOrdering

object NullOrdering:
  case object NullIsFirst extends NullOrdering:
    override def toString = "NULLS FIRST"

  case object NullIsLast extends NullOrdering:
    override def toString = "NULLS LAST"

  case object UndefinedOrder extends NullOrdering

// Window functions
case class Window(
    partitionBy: Seq[Expression],
    orderBy: Seq[SortItem],
    frame: Option[WindowFrame],
    nodeLocation: Option[NodeLocation]
) extends Expression:
  override def children: Seq[Expression] = partitionBy ++ orderBy ++ frame.toSeq

  override def toString: String =
    s"Window(partitionBy:${partitionBy.mkString(", ")}, orderBy:${orderBy.mkString(", ")}, frame:${frame})"

sealed trait FrameType

case object RangeFrame extends FrameType:
  override def toString = "RANGE"

case object RowsFrame extends FrameType:
  override def toString = "ROWS"

sealed trait FrameBound

case object UnboundedPreceding extends FrameBound:
  override def toString: String = "UNBOUNDED PRECEDING"

case object UnboundedFollowing extends FrameBound:
  override def toString: String = "UNBOUNDED FOLLOWING"

case class Preceding(n: Long) extends FrameBound:
  override def toString: String = s"${n} PRECEDING"

case class Following(n: Long) extends FrameBound:
  override def toString: String = s"${n} FOLLOWING"

case object CurrentRow extends FrameBound:
  override def toString: String = "CURRENT ROW"

case class WindowFrame(
    frameType: FrameType,
    start: FrameBound,
    end: Option[FrameBound],
    nodeLocation: Option[NodeLocation]
) extends Expression
    with LeafExpression:

  override def toString: String =
    val s = Seq.newBuilder[String]
    s += frameType.toString
    if end.isDefined then s += "BETWEEN"
    s += start.toString
    if end.isDefined then
      s += "AND"
      s += end.get.toString
    s.result().mkString(" ")

// Function
case class FunctionCall(
    name: String,
    args: Seq[Expression],
    // isDistinct: Boolean,
    // filter: Option[Expression],
    // window: Option[Window],
    nodeLocation: Option[NodeLocation]
) extends Expression:

  override def dataType: DataType =
    if functionName == "count" then DataType.LongType
    else
      // TODO: Resolve the function return type using a function catalog
      DataType.UnknownType

  override def children: Seq[Expression] = args // ++ filter.toSeq ++ window.toSeq
  def functionName: String               = name.toString.toLowerCase(Locale.US)

  override def toString =
    s"FunctionCall(${name}, ${args.mkString(", ")})" // , distinct:${isDistinct}, window:${window})"

case class LambdaExpr(body: Expression, args: Seq[String], nodeLocation: Option[NodeLocation])
    extends Expression
    with UnaryExpression:
  def child = body

case class Ref(name: QName, nodeLocation: Option[NodeLocation]) extends Expression with LeafExpression

// Conditional expression
sealed trait ConditionalExpression                  extends Expression
case class NoOp(nodeLocation: Option[NodeLocation]) extends ConditionalExpression with LeafExpression

case class Eq(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "="

case class NotEq(left: Expression, right: Expression, operatorName: String, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression {
  // require(operatorName == "<>" || operatorName == "!=", "NotEq.operatorName must be either <> or !=", nodeLocation)
}

case class And(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "AND"

case class Or(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "OR"

case class Not(child: Expression, nodeLocation: Option[NodeLocation]) extends ConditionalExpression with UnaryExpression

case class LessThan(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "<"

case class LessThanOrEq(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "<="

case class GreaterThan(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = ">"

case class GreaterThanOrEq(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = ">="

case class Between(e: Expression, a: Expression, b: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(e, a, b)

case class NotBetween(e: Expression, a: Expression, b: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(e, a, b)

case class IsNull(child: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with UnaryExpression:
  override def toString: String = s"IsNull(${child})"

case class IsNotNull(child: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with UnaryExpression:
  override def toString: String = s"IsNotNull(${child})"

case class In(a: Expression, list: Seq[Expression], nodeLocation: Option[NodeLocation]) extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ list
  override def toString: String          = s"In(${a} <in> [${list.mkString(", ")}])"

case class NotIn(a: Expression, list: Seq[Expression], nodeLocation: Option[NodeLocation])
    extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ list
  override def toString: String          = s"NotIn(${a} <not in> [${list.mkString(", ")}])"

case class InSubQuery(a: Expression, in: Relation, nodeLocation: Option[NodeLocation]) extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ in.childExpressions
  override def toString: String          = s"InSubQuery(${a} <in> ${in})"

case class NotInSubQuery(a: Expression, in: Relation, nodeLocation: Option[NodeLocation]) extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ in.childExpressions
  override def toString: String          = s"NotInSubQuery(${a} <not in> ${in})"

case class Like(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "LIKE"

case class NotLike(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "NOT LIKE"

case class DistinctFrom(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "IS DISTINCT FROM"

case class NotDistinctFrom(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "IS NOT DISTINCT FROM"

case class IfExpr(
    cond: ConditionalExpression,
    onTrue: Expression,
    onFalse: Expression,
    nodeLocation: Option[NodeLocation]
) extends Expression:
  override def children: Seq[Expression] = Seq(cond, onTrue, onFalse)

case class CaseExpr(
    operand: Option[Expression],
    whenClauses: Seq[WhenClause],
    defaultValue: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends Expression:

  override def children: Seq[Expression] =
    val b = Seq.newBuilder[Expression]
    operand.foreach(b += _)
    b ++= whenClauses
    defaultValue.foreach(b += _)
    b.result()

case class WhenClause(condition: Expression, result: Expression, nodeLocation: Option[NodeLocation]) extends Expression:
  override def children: Seq[Expression] = Seq(condition, result)

case class Exists(child: Expression, nodeLocation: Option[NodeLocation]) extends Expression with UnaryExpression

// Arithmetic expr
abstract sealed class BinaryExprType(val symbol: String)
case object Add      extends BinaryExprType("+")
case object Subtract extends BinaryExprType("-")
case object Multiply extends BinaryExprType("*")
case object Divide   extends BinaryExprType("/")
case object Modulus  extends BinaryExprType("%")

sealed trait ArithmeticExpression extends Expression

case class ArithmeticBinaryExpr(
    exprType: BinaryExprType,
    left: Expression,
    right: Expression,
    nodeLocation: Option[NodeLocation]
) extends ArithmeticExpression
    with BinaryExpression:

  override def dataType: DataType =
    if left.dataType == right.dataType then left.dataType
    else
      // TODO type escalation e.g., (Double) op (Long) -> (Double)
      DataType.UnknownType

  override def operatorName: String = exprType.symbol

  override def toString: String =
    s"${exprType}(left:$left, right:$right)"

case class ArithmeticUnaryExpr(sign: Sign, child: Expression, nodeLocation: Option[NodeLocation])
    extends ArithmeticExpression
    with UnaryExpression

abstract sealed class Sign(val symbol: String)
case object Positive extends Sign("+")
case object Negative extends Sign("-")

// Set quantifier
sealed trait SetQuantifier extends LeafExpression:
  def isDistinct: Boolean
  override def toString: String = getClass.getSimpleName

case class All(nodeLocation: Option[NodeLocation]) extends SetQuantifier:
  override def isDistinct: Boolean = false

case class DistinctSet(nodeLocation: Option[NodeLocation]) extends SetQuantifier:
  override def toString: String    = "DISTINCT"
  override def isDistinct: Boolean = true

// Literal
sealed trait Literal extends Expression:
  def stringValue: String

case class NullLiteral(nodeLocation: Option[NodeLocation]) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.NullType
  override def stringValue: String = "null"
  override def toString: String    = "Literal(NULL)"

sealed trait BooleanLiteral extends Literal:
  override def dataType: DataType = DataType.BooleanType
  def booleanValue: Boolean

case class TrueLiteral(nodeLocation: Option[NodeLocation]) extends BooleanLiteral with LeafExpression:
  override def stringValue: String   = "true"
  override def toString: String      = "Literal(TRUE)"
  override def booleanValue: Boolean = true

case class FalseLiteral(nodeLocation: Option[NodeLocation]) extends BooleanLiteral with LeafExpression:
  override def stringValue: String   = "false"
  override def toString: String      = "Literal(FALSE)"
  override def booleanValue: Boolean = false

case class StringLiteral(value: String, nodeLocation: Option[NodeLocation]) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.StringType
  override def stringValue: String = value
  override def toString            = s"StringLiteral('${value}')"

case class TimeLiteral(value: String, nodeLocation: Option[NodeLocation]) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.TimestampType(TimestampField.TIME, false)
  override def stringValue: String = value
  override def toString            = s"Literal(TIME '${value}')"

case class TimestampLiteral(value: String, nodeLocation: Option[NodeLocation]) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.TimestampType(TimestampField.TIMESTAMP, false)
  override def stringValue: String = value
  override def toString            = s"Literal(TIMESTAMP '${value}')"

case class DecimalLiteral(value: String, nodeLocation: Option[NodeLocation]) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.DecimalType(TypeVariable("precision"), TypeVariable("scale"))
  override def stringValue: String = value
  override def toString            = s"Literal(DECIMAL '${value}')"

case class CharLiteral(value: String, nodeLocation: Option[NodeLocation]) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.CharType(None)
  override def stringValue: String = value
  override def toString            = s"Literal(CHAR '${value}')"

case class DoubleLiteral(value: Double, nodeLocation: Option[NodeLocation]) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.DoubleType
  override def stringValue: String = value.toString
  override def toString            = s"DoubleLiteral(${value.toString})"

case class LongLiteral(value: Long, nodeLocation: Option[NodeLocation]) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.LongType
  override def stringValue: String = value.toString
  override def toString            = s"LongLiteral(${value.toString})"

case class IntervalLiteral(
    value: String,
    sign: Sign,
    startField: IntervalField,
    end: Option[IntervalField],
    nodeLocation: Option[NodeLocation]
) extends Literal:
  override def children: Seq[Expression] = Seq(startField) ++ end.toSeq
  override def stringValue: String       = s"${sign.symbol} '${value}' ${startField}"

  override def toString: String =
    s"Literal(INTERVAL ${sign.symbol} '${value}' ${startField})"

case class GenericLiteral(tpe: String, value: String, nodeLocation: Option[NodeLocation])
    extends Literal
    with LeafExpression:
  override def stringValue: String = value
  override def toString            = s"Literal(${tpe} '${value}')"

case class BinaryLiteral(binary: String, nodeLocation: Option[NodeLocation]) extends Literal with LeafExpression:
  override def stringValue: String = binary

sealed trait IntervalField extends LeafExpression:
  override def toString(): String = getClass.getSimpleName

case class Year(nodeLocation: Option[NodeLocation])    extends IntervalField
case class Quarter(nodeLocation: Option[NodeLocation]) extends IntervalField
case class Month(nodeLocation: Option[NodeLocation])   extends IntervalField
case class Week(nodeLocation: Option[NodeLocation])    extends IntervalField
case class Day(nodeLocation: Option[NodeLocation])     extends IntervalField

case class DayOfWeek(nodeLocation: Option[NodeLocation]) extends IntervalField:
  override def toString(): String = "day_of_week"

case class DayOfYear(nodeLocation: Option[NodeLocation]) extends IntervalField:
  override def toString(): String = "day_of_year"

case class YearOfWeek(nodeLocation: Option[NodeLocation]) extends IntervalField:
  override def toString(): String = "year_of_week"

case class Hour(nodeLocation: Option[NodeLocation])   extends IntervalField
case class Minute(nodeLocation: Option[NodeLocation]) extends IntervalField
case class Second(nodeLocation: Option[NodeLocation]) extends IntervalField

case class TimezoneHour(nodeLocation: Option[NodeLocation]) extends IntervalField:
  override def toString(): String = "timezone_hour"

case class TimezoneMinute(nodeLocation: Option[NodeLocation]) extends IntervalField:
  override def toString(): String = "timezone_minute"

// Value constructor
case class ArrayConstructor(values: Seq[Expression], nodeLocation: Option[NodeLocation]) extends Expression:

  def elementType: DataType =
    val elemTypes = values.map(_.dataType).distinct
    if elemTypes.size == 1 then elemTypes.head
    else AnyType

  override def dataType: DataType        = ArrayType(elementType)
  override def children: Seq[Expression] = values
  override def toString: String          = s"Array(${values.mkString(", ")})"

case class RowConstructor(values: Seq[Expression], nodeLocation: Option[NodeLocation]) extends Expression:

  override def dataType: DataType =
    EmbeddedRecordType(values.map(_.dataType))

  override def children: Seq[Expression] = values
  override def toString: String          = s"Row(${values.mkString(", ")})"

abstract sealed class CurrentTimeBase(name: String, precision: Option[Int]) extends LeafExpression

case class CurrentTime(precision: Option[Int], nodeLocation: Option[NodeLocation])
    extends CurrentTimeBase("current_time", precision)

case class CurrentDate(precision: Option[Int], nodeLocation: Option[NodeLocation])
    extends CurrentTimeBase("current_date", precision)

case class CurrentTimestamp(precision: Option[Int], nodeLocation: Option[NodeLocation])
    extends CurrentTimeBase("current_timestamp", precision)

case class CurrentLocalTime(precision: Option[Int], nodeLocation: Option[NodeLocation])
    extends CurrentTimeBase("localtime", precision)

case class CurrentLocalTimeStamp(precision: Option[Int], nodeLocation: Option[NodeLocation])
    extends CurrentTimeBase("localtimestamp", precision)

// 1-origin parameter
case class Parameter(index: Int, nodeLocation: Option[NodeLocation]) extends LeafExpression

case class SubQueryExpression(query: Relation, nodeLocation: Option[NodeLocation]) extends Expression:
  override def children: Seq[Expression] = query.childExpressions

case class Cast(expr: Expression, tpe: String, tryCast: Boolean = false, nodeLocation: Option[NodeLocation])
    extends UnaryExpression:
  override def child: Expression = expr

case class SchemaProperty(key: Identifier, value: Expression, nodeLocation: Option[NodeLocation]) extends Expression:
  override def children: Seq[Expression] = Seq(key, value)

sealed trait TableElement extends Expression

case class ColumnDef(columnName: Identifier, tpe: ColumnType, nodeLocation: Option[NodeLocation])
    extends TableElement
    with UnaryExpression:
  override def toString: String  = s"${columnName.value}:${tpe.tpe}"
  override def child: Expression = columnName

case class ColumnType(tpe: String, nodeLocation: Option[NodeLocation]) extends LeafExpression

case class ColumnDefLike(tableName: QName, includeProperties: Boolean, nodeLocation: Option[NodeLocation])
    extends TableElement
    with UnaryExpression:
  override def child: Expression = tableName

// Aggregation
trait GroupingKey extends UnaryExpression:
  def index: Option[Int]
  override def child: Expression

case class UnresolvedGroupingKey(child: Expression, nodeLocation: Option[NodeLocation]) extends GroupingKey:
  override def index: Option[Int]     = None
  override def toString: String       = s"GroupingKey(${index.map(i => s"${i}:").getOrElse("")}${child})"
  override lazy val resolved: Boolean = false

case class Extract(interval: IntervalField, expr: Expression, nodeLocation: Option[NodeLocation]) extends Expression:
  override def children: Seq[Expression] = Seq(expr)
  override def toString                  = s"Extract(interval:${interval}, ${expr})"
