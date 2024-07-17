package com.treasuredata.flow.lang.model.expr

import com.treasuredata.flow.lang.compiler.TermName
import com.treasuredata.flow.lang.model.DataType.{
  AnyType,
  ArrayType,
  EmbeddedRecordType,
  TimestampField,
  TypeVariable
}
import com.treasuredata.flow.lang.model.{DataType, NodeLocation}
import com.treasuredata.flow.lang.model.plan.*

import java.util.Locale

/**
  */
case class ParenthesizedExpression(child: Expression, nodeLocation: Option[NodeLocation])
    extends UnaryExpression:
  override def dataType: DataType = child.dataType

/**
  * variable name, function name, type name, etc. The name might have a qualifier.
  */
sealed trait NameExpr extends Expression:
  /* string expression of the name */
  def strExpr: String = fullName
  def leafName: String
  def fullName: String
  def isEmpty: Boolean =
    // TODO: This part is a bit ad-hoc as EmptyName can be copied during the tree transformation, so
    // we can't use the object equality like this eq EmptyName
    this.leafName == "<empty>"

object NameExpr:
  val EmptyName: Identifier           = UnquotedIdentifier("<empty>", None)
  def fromString(s: String): NameExpr = UnquotedIdentifier(s, None)

case class Wildcard(nodeLocation: Option[NodeLocation]) extends LeafExpression with NameExpr:
  override def leafName: String = "*"
  override def fullName: String = "*"

/**
  * Reference to the current input, denoted by '_'
  * @param dataType
  * @param nodeLocation
  */
case class ContextInputRef(override val dataType: DataType, nodeLocation: Option[NodeLocation])
    extends LeafExpression
    with Identifier:
  override def leafName: String      = "_"
  override def fullName: String      = "_"
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
    nodeLocation: Option[NodeLocation]
) extends QualifiedName:
  override def leafName: String = name.leafName
  override def fullName: String =
    qualifier match
      case q: NameExpr =>
        s"${q.fullName}.${name.fullName}"
      case _ =>
        // TODO print right expr
        s"${qualifier}.${name.fullName}"

  override def toString: String          = s"Ref(${qualifier},${name})"
  override def children: Seq[Expression] = Seq(qualifier)

sealed trait Identifier extends QualifiedName with LeafExpression:
  override def qualifier: Expression = NameExpr.EmptyName
  override def fullName: String      = leafName
  override def leafName: String      = strExpr
  // Unquoted value
  def unquotedValue: String

  override def attributeName: String  = strExpr
  override lazy val resolved: Boolean = false
  def toResolved(dataType: DataType): ResolvedIdentifier = ResolvedIdentifier(
    this.strExpr,
    dataType,
    nodeLocation
  )

case class ResolvedIdentifier(
    override val unquotedValue: String,
    override val dataType: DataType,
    nodeLocation: Option[NodeLocation]
) extends Identifier:
  override def strExpr: String = unquotedValue
  override def toResolved(dataType: DataType) =
    if this.dataType == dataType then
      this
    else
      this.copy(dataType = dataType)

  override lazy val resolved: Boolean = true

// Used for group by 1, 2, 3 ...
case class DigitIdentifier(override val unquotedValue: String, nodeLocation: Option[NodeLocation])
    extends Identifier:
  override def strExpr          = unquotedValue
  override def toString: String = s"Id(${unquotedValue})"

case class UnquotedIdentifier(
    override val unquotedValue: String,
    nodeLocation: Option[NodeLocation]
) extends Identifier:
  override def strExpr          = unquotedValue
  override def toString: String = s"Id(${unquotedValue})"

/**
  * Backquote is used for table or column names that conflicts with reserved words
  * @param unquotedValue
  * @param nodeLocation
  */
case class BackQuotedIdentifier(
    override val unquotedValue: String,
    nodeLocation: Option[NodeLocation]
) extends Identifier:
  override def strExpr: String = s"`${unquotedValue}`"
  override def toString        = s"Id(`${unquotedValue}`)"

sealed trait JoinCriteria extends Expression
case object NoJoinCriteria extends JoinCriteria with LeafExpression:
  override def nodeLocation: Option[NodeLocation] = None

case class NaturalJoin(nodeLocation: Option[NodeLocation]) extends JoinCriteria with LeafExpression:
  override def toString: String = "NaturalJoin"

case class JoinUsing(columns: Seq[NameExpr], nodeLocation: Option[NodeLocation])
    extends JoinCriteria:
  override def children: Seq[Expression] = columns
  override def toString: String          = s"JoinUsing(${columns.mkString(",")})"

case class ResolvedJoinUsing(keys: Seq[MultiSourceColumn], nodeLocation: Option[NodeLocation])
    extends JoinCriteria:
  override def children: Seq[Expression] = keys
  override def toString: String          = s"ResolvedJoinUsing(${keys.mkString(",")})"
  override lazy val resolved: Boolean    = true

case class JoinOn(expr: Expression, nodeLocation: Option[NodeLocation])
    extends JoinCriteria
    with UnaryExpression:
  override def child: Expression = expr
  override def toString: String  = s"JoinOn(${expr})"

/**
  * Join condition used only when join keys are resolved
  */
case class JoinOnEq(keys: Seq[Expression], nodeLocation: Option[NodeLocation])
    extends JoinCriteria
    with LeafExpression:
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
  override def dataType: DataType = sortKey.dataType
  override def child: Expression  = sortKey

  override def toString: String =
    s"SortItem(sortKey:${sortKey}, ordering:${ordering}, nullOrdering:${nullOrdering})"

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
    partitionBy: Seq[Expression],
    orderBy: Seq[SortItem],
    frame: Option[WindowFrame],
    nodeLocation: Option[NodeLocation]
) extends Expression:
  override def children: Seq[Expression] = partitionBy ++ orderBy ++ frame.toSeq

  override def toString: String =
    s"Window(partitionBy:${partitionBy.mkString(", ")}, orderBy:${orderBy.mkString(", ")}, frame:${frame})"

enum FrameType(val expr: String):
  case RangeFrame extends FrameType("range")
  case RowsFrame  extends FrameType("rows")

enum FrameBound(val expr: String):
  case UnboundedPreceding extends FrameBound("unbounded preceding")
  case UnboundedFollowing extends FrameBound("unbounded following")
  case Preceding(n: Long) extends FrameBound(s"${n} preceding")
  case Following(n: Long) extends FrameBound(s"${n} following")
  case CurrentRow         extends FrameBound("current row")

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
    if end.isDefined then
      s += "BETWEEN"
    s += start.toString
    if end.isDefined then
      s += "AND"
      s += end.get.toString
    s.result().mkString(" ")

// Function
case class FunctionApply(
    base: Expression,
    args: List[FunctionArg],
    nodeLocation: Option[NodeLocation]
) extends Expression:
  override def children: Seq[Expression] = args
  override def dataType: DataType        = base.dataType

case class FunctionArg(
    name: Option[TermName],
    value: Expression,
    nodeLocation: Option[NodeLocation]
) extends Expression:
  override def children: Seq[Expression] = Seq.empty
  override def dataType: DataType        = value.dataType

case class LambdaExpr(body: Expression, args: Seq[String], nodeLocation: Option[NodeLocation])
    extends Expression
    with UnaryExpression:
  def child = body

//case class Ref(name: QName, nodeLocation: Option[NodeLocation]) extends Expression with LeafExpression

// Conditional expression
sealed trait ConditionalExpression extends Expression:
  override def dataType: DataType = DataType.BooleanType

case class NoOp(nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with LeafExpression

case class Eq(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "="

case class NotEq(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "!="
  // require(operatorName == "<>" || operatorName == "!=", "NotEq.operatorName must be either <> or !=", nodeLocation)

case class And(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "AND"

case class Or(left: Expression, right: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "OR"

case class Not(child: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with UnaryExpression

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

case class NotBetween(
    e: Expression,
    a: Expression,
    b: Expression,
    nodeLocation: Option[NodeLocation]
) extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(e, a, b)

case class IsNull(child: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with UnaryExpression:
  override def toString: String = s"IsNull(${child})"

case class IsNotNull(child: Expression, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression
    with UnaryExpression:
  override def toString: String = s"IsNotNull(${child})"

case class In(a: Expression, list: Seq[Expression], nodeLocation: Option[NodeLocation])
    extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ list
  override def toString: String          = s"In(${a} <in> [${list.mkString(", ")}])"

case class NotIn(a: Expression, list: Seq[Expression], nodeLocation: Option[NodeLocation])
    extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ list
  override def toString: String          = s"NotIn(${a} <not in> [${list.mkString(", ")}])"

case class InSubQuery(a: Expression, in: Relation, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ in.childExpressions
  override def toString: String          = s"InSubQuery(${a} <in> ${in})"

case class NotInSubQuery(a: Expression, in: Relation, nodeLocation: Option[NodeLocation])
    extends ConditionalExpression:
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

case class WhenClause(condition: Expression, result: Expression, nodeLocation: Option[NodeLocation])
    extends Expression:
  override def children: Seq[Expression] = Seq(condition, result)

case class Exists(child: Expression, nodeLocation: Option[NodeLocation])
    extends Expression
    with UnaryExpression

enum TestType(val expr: String):
  case ShouldBe         extends TestType("should be")
  case ShouldNotBe      extends TestType("should not be")
  case ShouldContain    extends TestType("should contain")
  case ShouldNotContain extends TestType("should not contain")

case class ShouldExpr(
    testType: TestType,
    left: Expression,
    right: Expression,
    nodeLocation: Option[NodeLocation]
) extends Expression:
  override def dataType: DataType        = DataType.BooleanType
  override def children: Seq[Expression] = Seq(left, right)

// Arithmetic expr
enum BinaryExprType(val expr: String):
  case Add      extends BinaryExprType("+")
  case Subtract extends BinaryExprType("-")
  case Multiply extends BinaryExprType("*")
  case Divide   extends BinaryExprType("/")
  case Modulus  extends BinaryExprType("%")

sealed trait ArithmeticExpression extends Expression

case class ArithmeticBinaryExpr(
    exprType: BinaryExprType,
    left: Expression,
    right: Expression,
    nodeLocation: Option[NodeLocation]
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

  override def operatorName: String = exprType.expr

  override def toString: String = s"${exprType}(left:$left, right:$right)"

end ArithmeticBinaryExpr

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

case class This(override val dataType: DataType, nodeLocation: Option[NodeLocation])
    extends LeafExpression

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

case class TrueLiteral(nodeLocation: Option[NodeLocation])
    extends BooleanLiteral
    with LeafExpression:
  override def stringValue: String   = "true"
  override def toString: String      = "Literal(TRUE)"
  override def booleanValue: Boolean = true

case class FalseLiteral(nodeLocation: Option[NodeLocation])
    extends BooleanLiteral
    with LeafExpression:
  override def stringValue: String   = "false"
  override def toString: String      = "Literal(FALSE)"
  override def booleanValue: Boolean = false

case class StringLiteral(value: String, nodeLocation: Option[NodeLocation])
    extends Literal
    with LeafExpression:
  override def dataType: DataType  = DataType.StringType
  override def stringValue: String = value
  override def toString            = s"StringLiteral('${value}')"

case class TripleQuoteLiteral(value: String, nodeLocation: Option[NodeLocation])
    extends Literal
    with LeafExpression:
  override def dataType: DataType  = DataType.StringType
  override def stringValue: String = value
  override def toString            = s"TripleQuoteLiteral('${value}')"

case class JsonLiteral(value: String, nodeLocation: Option[NodeLocation])
    extends Literal
    with LeafExpression:
  override def dataType: DataType  = DataType.JsonType
  override def stringValue: String = value
  override def toString            = s"JsonLiteral('${value}')"

case class TimeLiteral(value: String, nodeLocation: Option[NodeLocation])
    extends Literal
    with LeafExpression:
  override def dataType: DataType  = DataType.TimestampType(TimestampField.TIME, false)
  override def stringValue: String = value
  override def toString            = s"Literal(TIME '${value}')"

case class TimestampLiteral(value: String, nodeLocation: Option[NodeLocation])
    extends Literal
    with LeafExpression:
  override def dataType: DataType  = DataType.TimestampType(TimestampField.TIMESTAMP, false)
  override def stringValue: String = value
  override def toString            = s"Literal(TIMESTAMP '${value}')"

case class DecimalLiteral(value: String, nodeLocation: Option[NodeLocation])
    extends Literal
    with LeafExpression:
  override def dataType: DataType = DataType
    .DecimalType(TypeVariable("precision"), TypeVariable("scale"))

  override def stringValue: String = value
  override def toString            = s"Literal(DECIMAL '${value}')"

case class CharLiteral(value: String, nodeLocation: Option[NodeLocation])
    extends Literal
    with LeafExpression:
  override def dataType: DataType  = DataType.CharType(None)
  override def stringValue: String = value
  override def toString            = s"Literal(CHAR '${value}')"

case class DoubleLiteral(value: Double, nodeLocation: Option[NodeLocation])
    extends Literal
    with LeafExpression:
  override def dataType: DataType  = DataType.DoubleType
  override def stringValue: String = value.toString
  override def toString            = s"DoubleLiteral(${value.toString})"

case class LongLiteral(value: Long, nodeLocation: Option[NodeLocation])
    extends Literal
    with LeafExpression:
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

  override def toString: String = s"Literal(INTERVAL ${sign.symbol} '${value}' ${startField})"

case class GenericLiteral(tpe: String, value: String, nodeLocation: Option[NodeLocation])
    extends Literal
    with LeafExpression:
  override def stringValue: String = value
  override def toString            = s"Literal(${tpe} '${value}')"

case class BinaryLiteral(binary: String, nodeLocation: Option[NodeLocation])
    extends Literal
    with LeafExpression:
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
case class ArrayConstructor(values: Seq[Expression], nodeLocation: Option[NodeLocation])
    extends Expression:

  def elementType: DataType =
    val elemTypes = values.map(_.dataType).distinct
    if elemTypes.size == 1 then
      elemTypes.head
    else
      AnyType

  override def dataType: DataType        = ArrayType(elementType)
  override def children: Seq[Expression] = values
  override def toString: String          = s"Array(${values.mkString(", ")})"

case class RowConstructor(values: Seq[Expression], nodeLocation: Option[NodeLocation])
    extends Expression:

  override def dataType: DataType = EmbeddedRecordType(values.map(_.dataType))

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

case class SubQueryExpression(query: Relation, nodeLocation: Option[NodeLocation])
    extends Expression:
  override def children: Seq[Expression] = query.childExpressions

case class Cast(
    expr: NameExpr,
    tpe: NameExpr,
    tryCast: Boolean = false,
    nodeLocation: Option[NodeLocation]
) extends UnaryExpression:
  override def child: Expression = expr

case class SchemaProperty(key: NameExpr, value: Expression, nodeLocation: Option[NodeLocation])
    extends Expression:
  override def children: Seq[Expression] = Seq(key, value)

sealed trait TableElement extends Expression

case class ColumnDef(columnName: NameExpr, tpe: ColumnType, nodeLocation: Option[NodeLocation])
    extends TableElement
    with UnaryExpression:
  override def toString: String  = s"${columnName.strExpr}:${tpe.tpe}"
  override def child: Expression = columnName

case class ColumnType(tpe: NameExpr, nodeLocation: Option[NodeLocation]) extends LeafExpression

case class ColumnDefLike(
    tableName: NameExpr,
    includeProperties: Boolean,
    nodeLocation: Option[NodeLocation]
) extends TableElement
    with UnaryExpression:
  override def child: Expression = tableName

// Aggregation
trait GroupingKey extends UnaryExpression:
  def index: Option[Int]
  override def child: Expression

case class UnresolvedGroupingKey(
    name: NameExpr,
    child: Expression,
    nodeLocation: Option[NodeLocation]
) extends GroupingKey:
  override def dataType: DataType = child.dataType
  override def index: Option[Int] = None
  override def toString: String = s"GroupingKey(${index.map(i => s"${i}:").getOrElse("")}${child})"
  override lazy val resolved: Boolean = child.dataType.isResolved

case class Extract(interval: IntervalField, expr: Expression, nodeLocation: Option[NodeLocation])
    extends Expression:
  override def children: Seq[Expression] = Seq(expr)
  override def toString                  = s"Extract(interval:${interval}, ${expr})"

case class InterpolatedString(
    prefix: NameExpr,
    parts: List[Expression],
    nodeLocation: Option[NodeLocation]
) extends Expression:
  override def children: Seq[Expression] = parts
  override def toString: String          = s"InterpolatedString(${prefix}, ${parts.mkString(", ")})"
