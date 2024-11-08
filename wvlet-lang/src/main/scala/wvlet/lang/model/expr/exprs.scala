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

import wvlet.lang.api.{Span, NodeLocation}
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.compiler.{Name, TermName, TypeName}
import wvlet.lang.model.DataType.{
  AnyType,
  ArrayType,
  EmbeddedRecordType,
  TimestampField,
  TypeVariable
}
import wvlet.lang.model.expr.BinaryExprType.DivideInt
import wvlet.lang.model.expr.NameExpr.sqlKeywords
import wvlet.lang.model.DataType
import wvlet.lang.model.plan.*

import java.util.Locale

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

/**
  * variable name, function name, type name, etc. The name might have a qualifier.
  */
sealed trait NameExpr extends Expression:
  /* string expression of the name */
  def strExpr: String
  def leafName: String
  def fullName: String
  def nonEmpty: Boolean = !isEmpty
  def isEmpty: Boolean =
    // TODO: This part is a bit ad-hoc as EmptyName can be copied during the tree transformation, so
    // we can't use the object equality like this eq EmptyName
    this.leafName == "<empty>"

  def isGroupingKeyIndex: Boolean = "_[0-9]+".r.matches(fullName)

  def toTermName: TermName = Name.termName(leafName)
  def toTypeName: TypeName = Name.typeName(leafName)

  def toSQLAttributeName: String =
    val s =
      this match
        case i: Identifier =>
          i.unquotedValue
        case _ =>
          fullName
    if s.matches("^[_a-zA-Z][_a-zA-Z0-9]*$") && !sqlKeywords.contains(s) then
      s
    else
      s""""${s}""""

object NameExpr:
  val EmptyName: Identifier           = UnquotedIdentifier("<empty>", NoSpan)
  def fromString(s: String): NameExpr = UnquotedIdentifier(s, NoSpan)

  private val sqlKeywords = Set(
    // TODO enumerate more SQL keywords
    "select",
    "schema",
    "table",
    "from",
    "catalog"
  )

case class Wildcard(span: Span) extends LeafExpression with QualifiedName:
  override def leafName: String = "*"
  override def fullName: String = "*"
  override def strExpr: String  = "*"

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
  override def leafName: String      = "_"
  override def fullName: String      = "_"
  override def strExpr: String       = "_"
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
  override def leafName: String = name.leafName
  override def strExpr: String  = fullName
  override def fullName: String =
    qualifier match
      case q: NameExpr =>
        s"${q.fullName}.${name.fullName}"
      case _ =>
        // TODO print right expr
        s"${qualifier}.${name.fullName}"

  override def toString: String          = s"DotRef(${qualifier}:${qualifier.dataType},${name})"
  override def children: Seq[Expression] = Seq(qualifier)

sealed trait Identifier extends QualifiedName with LeafExpression:
  override def qualifier: Expression = NameExpr.EmptyName
  override def fullName: String      = leafName
  override def leafName: String      = strExpr

  // Unquoted value
  def unquotedValue: String

  override def attributeName: String = strExpr

  override lazy val resolved: Boolean = false
  def toResolved(dataType: DataType): ResolvedIdentifier = ResolvedIdentifier(
    this.unquotedValue,
    dataType,
    span
  )

case class ResolvedIdentifier(
    override val unquotedValue: String,
    override val dataType: DataType,
    span: Span
) extends Identifier:
  override def strExpr: String = unquotedValue
  override def toResolved(dataType: DataType) =
    if this.dataType == dataType then
      this
    else
      this.copy(dataType = dataType)

  override lazy val resolved: Boolean = true

// Used for group by 1, 2, 3 ...
case class DigitIdentifier(override val unquotedValue: String, span: Span) extends Identifier:
  override def strExpr = unquotedValue

case class UnquotedIdentifier(override val unquotedValue: String, span: Span) extends Identifier:
  override def strExpr = unquotedValue

case class DoubleQuotedIdentifier(override val unquotedValue: String, span: Span)
    extends Identifier:
  override def strExpr: String = s""""${unquotedValue}""""

/**
  * Backquote is used for table or column names that conflicts with reserved words
  * @param unquotedValue
  * @param nodeLocation
  */
case class BackQuotedIdentifier(override val unquotedValue: String, span: Span) extends Identifier:
  override def leafName: String = unquotedValue
  override def fullName: String = unquotedValue
  override def strExpr: String  = s"`${unquotedValue}`"

case class BackquoteInterpolatedString(
    prefix: NameExpr,
    parts: List[Expression],
    override val dataType: DataType,
    span: Span
) extends Identifier:
  override def children: Seq[Expression] = parts
  override def strExpr: String           = "<backquote interpolation>"
  override def unquotedValue: String     = ???

sealed trait JoinCriteria extends Expression
case object NoJoinCriteria extends JoinCriteria with LeafExpression:
  override def span: Span = NoSpan

case class NaturalJoin(span: Span) extends JoinCriteria with LeafExpression:
  override def toString: String = "NaturalJoin"

sealed trait JoinOnTheSameColumns extends JoinCriteria:
  def columns: Seq[NameExpr]

case class JoinUsing(columns: Seq[NameExpr], span: Span) extends JoinOnTheSameColumns:
  override def children: Seq[Expression] = columns

case class ResolvedJoinUsing(keys: Seq[MultiSourceColumn], span: Span) extends JoinOnTheSameColumns:
  override def columns: Seq[NameExpr] = keys.map { k =>
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
    partitionBy: Seq[Expression],
    orderBy: Seq[SortItem],
    frame: Option[WindowFrame],
    span: Span
) extends Expression:
  override def children: Seq[Expression] = partitionBy ++ orderBy ++ frame.toSeq

enum FrameType(val expr: String):
  case RangeFrame extends FrameType("range")
  case RowsFrame  extends FrameType("rows")

enum FrameBound(val expr: String):
  case UnboundedPreceding extends FrameBound("unbounded preceding")
  case UnboundedFollowing extends FrameBound("unbounded following")
  case Preceding(n: Long) extends FrameBound(s"${n} preceding")
  case Following(n: Long) extends FrameBound(s"${n} following")
  case CurrentRow         extends FrameBound("current row")

case class WindowFrame(frameType: FrameType, start: FrameBound, end: Option[FrameBound], span: Span)
    extends Expression
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
    window: Option[Window],
    span: Span
) extends Expression:
  override def children: Seq[Expression] = args
  override def dataType: DataType        = base.dataType

case class FunctionArg(name: Option[TermName], value: Expression, span: Span) extends Expression:
  override def children: Seq[Expression] = Seq(value)
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

case class And(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
  override def operatorName: String = "and"

case class Or(left: Expression, right: Expression, span: Span)
    extends ConditionalExpression
    with BinaryExpression:
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

case class In(a: Expression, list: Seq[Expression], span: Span) extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ list

case class NotIn(a: Expression, list: Seq[Expression], span: Span) extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ list

case class InSubQuery(a: Expression, in: Relation, span: Span) extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ in.childExpressions

case class NotInSubQuery(a: Expression, in: Relation, span: Span) extends ConditionalExpression:
  override def children: Seq[Expression] = Seq(a) ++ in.childExpressions

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

abstract sealed class Sign(val symbol: String)
case object Positive extends Sign("+")
case object Negative extends Sign("-")

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
  def stringValue: String
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

case class StringLiteral(value: String, span: Span) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.StringType
  override def stringValue: String = value
  override def unquotedValue: String =
    if value.startsWith("\"") then
      value.stripPrefix("\"").stripSuffix("\"")
    else if value.startsWith("'") then
      value.stripPrefix("'").stripSuffix("'")
    else
      value

case class StringPart(value: String, span: Span) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.StringType
  override def stringValue: String = value

case class TripleQuoteLiteral(value: String, span: Span) extends Literal with LeafExpression:
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

case class DecimalLiteral(value: String, span: Span) extends Literal with LeafExpression:
  override def dataType: DataType = DataType
    .DecimalType(TypeVariable(Name.typeName("precision")), TypeVariable(Name.typeName("scale")))

  override def stringValue: String = value

case class CharLiteral(value: String, span: Span) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.CharType(None)
  override def stringValue: String = value

case class DoubleLiteral(value: Double, span: Span) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.DoubleType
  override def stringValue: String = value.toString

case class LongLiteral(value: Long, span: Span) extends Literal with LeafExpression:
  override def dataType: DataType  = DataType.LongType
  override def stringValue: String = value.toString

case class IntervalLiteral(
    value: String,
    sign: Sign,
    startField: IntervalField,
    end: Option[IntervalField],
    span: Span
) extends Literal:
  override def children: Seq[Expression] = Seq(startField) ++ end.toSeq
  override def stringValue: String       = s"${sign.symbol} '${value}' ${startField}"

case class GenericLiteral(tpe: DataType, value: String, span: Span)
    extends Literal
    with LeafExpression:
  override def stringValue: String = value

case class BinaryLiteral(binary: String, span: Span) extends Literal with LeafExpression:
  override def stringValue: String = binary

sealed trait IntervalField extends LeafExpression:
  override def toString(): String = getClass.getSimpleName

case class Year(span: Span)    extends IntervalField
case class Quarter(span: Span) extends IntervalField
case class Month(span: Span)   extends IntervalField
case class Week(span: Span)    extends IntervalField
case class Day(span: Span)     extends IntervalField

case class DayOfWeek(span: Span) extends IntervalField:
  override def toString(): String = "day_of_week"

case class DayOfYear(span: Span) extends IntervalField:
  override def toString(): String = "day_of_year"

case class YearOfWeek(span: Span) extends IntervalField:
  override def toString(): String = "year_of_week"

case class Hour(span: Span)   extends IntervalField
case class Minute(span: Span) extends IntervalField
case class Second(span: Span) extends IntervalField

case class TimezoneHour(span: Span) extends IntervalField:
  override def toString(): String = "timezone_hour"

case class TimezoneMinute(span: Span) extends IntervalField:
  override def toString(): String = "timezone_minute"

// Value constructor
case class ArrayConstructor(values: Seq[Expression], span: Span) extends Expression:

  def elementType: DataType =
    val elemTypes = values.map(_.dataType).distinct
    if elemTypes.size == 1 then
      elemTypes.head
    else
      AnyType

  override def dataType: DataType        = ArrayType(elementType)
  override def children: Seq[Expression] = values

case class RowConstructor(values: Seq[Expression], span: Span) extends Expression:
  override def dataType: DataType        = EmbeddedRecordType(values.map(_.dataType))
  override def children: Seq[Expression] = values
  override def toString: String          = s"Row(${values.mkString(", ")})"

case class StructValue(fields: List[StructField], span: Span) extends Expression:
  override def children: Seq[Expression] = fields

case class StructField(name: String, value: Expression, span: Span) extends Expression:
  override def children: Seq[Expression] = Seq(value)

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

case class SubQueryExpression(query: Relation, span: Span) extends Expression:
  override def children: Seq[Expression] = query.childExpressions

case class Cast(expr: NameExpr, tpe: NameExpr, tryCast: Boolean = false, span: Span)
    extends UnaryExpression:
  override def child: Expression = expr

case class SchemaProperty(key: NameExpr, value: Expression, span: Span) extends Expression:
  override def children: Seq[Expression] = Seq(key, value)

sealed trait TableElement extends Expression

case class ColumnDef(columnName: NameExpr, tpe: ColumnType, span: Span)
    extends TableElement
    with UnaryExpression:
  override def toString: String  = s"${columnName.strExpr}:${tpe.tpe}"
  override def child: Expression = columnName

case class ColumnType(tpe: NameExpr, span: Span) extends LeafExpression

case class ColumnDefLike(tableName: NameExpr, includeProperties: Boolean, span: Span)
    extends TableElement
    with UnaryExpression:
  override def child: Expression = tableName

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
    span: Span
) extends Expression:
  override def children: Seq[Expression] = parts
