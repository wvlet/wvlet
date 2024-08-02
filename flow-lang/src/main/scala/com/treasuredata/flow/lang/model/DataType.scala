package com.treasuredata.flow.lang.model

import com.treasuredata.flow.lang.compiler.parser.DataTypeParser
import com.treasuredata.flow.lang.compiler.{Name, TermName, TypeName}
import com.treasuredata.flow.lang.model.DataType.NamedType
import com.treasuredata.flow.lang.model.expr.NameExpr
import wvlet.log.LogSupport

abstract class DataType(val typeName: TypeName, val typeParams: Seq[Type]) extends Type:
  override def toString: String = typeDescription

  def typeDescription: String =
    val typeStr =
      if typeParams.isEmpty then
        typeName.name
      else
        s"${typeName}(${typeParams.mkString(", ")})"
    if isResolved then
      typeStr
    else
      s"${typeStr}?"

  def baseTypeName: TypeName    = typeName
  override def isBound: Boolean = typeParams.forall(_.isBound)

  override def isFunctionType: Boolean = false
  override def isResolved: Boolean
  def resolved: DataType = this

/**
  * p A base type for representing LogicalPlan node types, which return table records
  * @param typeName
  * @param typeParams
  */
sealed abstract class RelationType(
    override val typeName: TypeName,
    override val typeParams: Seq[DataType]
) extends DataType(typeName, typeParams):
  def fields: Seq[NamedType]
  def find(f: Name => Boolean): Option[NamedType] = fields.find(x => f(x.name))

  override def typeDescription: String =
    val fieldDesc = fields.map(_.typeDescription).mkString(", ")
    s"${typeName}(${fieldDesc})${
        if isResolved then
          ""
        else
          "?"
      }"

object RelationType:
  private var typeCount: Int = 0
  def newRelationTypeName: String =
    typeCount += 1
    // ULID.newULIDString
    s"_t${typeCount}"

case class RelationTypeList(override val typeName: TypeName, inputRelationTypes: Seq[RelationType])
    extends RelationType(typeName, Seq.empty):
  override def isResolved: Boolean = inputRelationTypes.forall(_.isResolved)

  override def fields: Seq[NamedType] = inputRelationTypes.flatMap { r =>
    r.fields
  }

object DataType extends LogSupport:

  def parse(s: String): DataType = DataTypeParser.parse(s)

  def parse(s: String, typeParams: List[TypeParameter]): DataType =
    s match
      case "decimal" if typeParams.length == 2 =>
        DecimalType(typeParams(0), typeParams(1))
      case "array" if typeParams.length == 1 =>
        ArrayType(typeParams(0))
      case "map" if typeParams.length == 2 =>
        MapType(typeParams(0), typeParams(1))
      case _ =>
        // TODO parse more generic types
        primitiveTypeTable.getOrElse(Name.typeName(s), UnresolvedType(s))

//  def unapply(str: String): Option[DataType] =
//    Try(DataTypeParser.parse(str)).toOption

  case class UnresolvedType(leafName: String) extends DataType(Name.typeName(leafName), Seq.empty):
    override def typeDescription: String = s"${leafName}?"
    override def isResolved: Boolean     = false

  val NoType =
    new DataType(Name.NoTypeName, Seq.empty):
      override def isResolved: Boolean = true

  /**
    * Used for named column types or parameter types
    * @param name
    * @param dataType
    */
  case class NamedType(name: TermName, dataType: DataType)
      extends DataType(dataType.typeName, Seq.empty):
    override def isResolved: Boolean     = dataType.isResolved
    override def typeDescription: String = s"${name}:${dataType.typeDescription}"

  case class VarArgType(elemType: DataType) extends DataType(elemType.typeName, Seq(elemType)):
    override def isResolved: Boolean     = elemType.isResolved
    override def typeDescription: String = s"${elemType.typeDescription}*"

  /**
    * A type for representing relational table schemas
    */
  case class SchemaType(
      parent: Option[DataType],
      override val typeName: TypeName,
      columnTypes: Seq[NamedType]
  ) extends RelationType(typeName, Seq.empty):
    override def fields: Seq[NamedType] = columnTypes

    override def isResolved: Boolean = columnTypes.forall(_.isResolved)

  case object EmptyRelationType extends RelationType(Name.typeName("<empty>"), Seq.empty):
    override def typeDescription: String = "empty"
    override def isResolved: Boolean     = true
    override def fields: Seq[NamedType]  = Seq.empty

  case class UnresolvedRelationType(
      fullName: String,
      override val typeName: TypeName = Name.NoTypeName
  ) extends RelationType(typeName, Seq.empty):
    override def typeDescription: String = s"${fullName}?"
    override def isResolved: Boolean     = false
    override def fields: Seq[NamedType]  = Seq.empty

  case class AliasedType(alias: TypeName, baseType: RelationType)
      extends RelationType(baseType.typeName, Seq.empty):
    override def toString = s"${alias}:=${baseType}"

    override def fields: Seq[NamedType]  = baseType.fields
    override def typeDescription: String = typeName.name
    override def isResolved: Boolean     = baseType.isResolved
    override def resolved: RelationType  = baseType

  case class ProjectedType(
      override val typeName: TypeName,
      projectedColumns: Seq[NamedType],
      baseType: RelationType
  ) extends RelationType(typeName, Seq.empty):

    override def fields: Seq[NamedType] = projectedColumns
    override def isResolved: Boolean    = projectedColumns.forall(_.isResolved)

  /**
    * Aggregateed record types: (key1, key2, ...) -> [record1*]
    * @param typeName
    * @param groupingKeyTypes
    * @param valueType
    */
  case class AggregationType(
      override val typeName: TypeName,
      groupingKeyTypes: Seq[NamedType],
      valueType: RelationType
  ) extends RelationType(typeName, Seq.empty):

    override def fields: Seq[NamedType] =
      groupingKeyTypes :+ NamedType(Name.termName("_"), ArrayType(valueType))

    override def isResolved: Boolean = groupingKeyTypes.forall(_.isResolved) && valueType.isResolved

  case class ConcatType(override val typeName: TypeName, inputTypes: Seq[RelationType])
      extends RelationType(typeName, Seq.empty):

    override def fields: Seq[NamedType] = inputTypes.flatMap(_.fields)
    override def isResolved: Boolean    = inputTypes.forall(_.isResolved)

//  /**
//    * Type extension
//    * @param typeName
//    * @param defs
//    */
//  case class ExtensionType(
//      override val typeName: TypeName,
//      parent: Option[DataType],
//      defs: Seq[FunctionType]
//  ) extends RelationType(typeName, Seq.empty):
//
//    override def fields: Seq[NamedType] =
//      parent match
//        case Some(r: RelationType) =>
//          r.fields
//        case _ =>
//          Nil
//
//    override def isResolved = parent.exists(_.isResolved) && defs.forall(_.isResolved)

  /**
    * DataType parameter for representing concrete types like timestamp(2), and abstract types like
    * timestamp(p).
    */
  sealed abstract class TypeParameter(name: String) extends DataType(Name.typeName(name), Seq.empty)

  case class UnresolvedTypeParameter(name: String, typeBound: Option[NameExpr])
      extends TypeParameter(name):
    override def isResolved: Boolean = false

  /**
    * Constant type used for arguments of varchar(n), char(n), decimal(p, q), etc.
    */
  case class IntConstant(value: Int) extends TypeParameter(s"${value}"):
    override def isResolved: Boolean = true

  case class TypeVariable(name: String) extends TypeParameter(s"$$${name}"):
    override def isBound: Boolean    = false
    override def isResolved: Boolean = false

    override def bind(typeArgMap: Map[String, DataType]): DataType =
      typeArgMap.get(name) match
        case Some(t) =>
          t
        case None =>
          this

  case class GenericType(
      override val typeName: TypeName,
      override val typeParams: Seq[DataType] = Seq.empty
  ) extends DataType(typeName, typeParams):
    override def isBound: Boolean = typeParams.forall(_.isBound)

    override def bind(typeArgMap: Map[String, DataType]): DataType = GenericType(
      typeName,
      typeParams.map(_.bind(typeArgMap))
    )

    override def isResolved: Boolean = typeParams.forall(_.isResolved)

  case class IntervalDayTimeType(from: String, to: String)
      extends DataType(Name.typeName("interval"), Seq.empty):
    override def toString: String = s"interval from ${from} to ${to}"

    override def isResolved: Boolean = true

  enum TimestampField:
    case TIME      extends TimestampField
    case TIMESTAMP extends TimestampField

  case class TimestampType(
      field: TimestampField,
      withTimeZone: Boolean,
      precision: Option[DataType] = None
  ) extends DataType(Name.typeName(field.toString.toLowerCase), precision.toSeq):
    override def toString: String =
      val base =
        precision match
          case Some(p) =>
            s"${field.toString.toLowerCase}(${p.typeDescription})"
          case None =>
            field.toString.toLowerCase

      if withTimeZone then
        s"${base} with time zone"
      else
        base

    override def isResolved: Boolean = true

  private def primitiveTypes: Seq[DataType] = Seq(
    AnyType,
    NullType,
    BooleanType,
    ByteType,
    ShortType,
    IntType,
    LongType,
    FloatType,
    RealType,
    DoubleType,
    StringType,
    JsonType,
    DateType,
    BinaryType
  )

  private val primitiveTypeTable: Map[TypeName, DataType] =
    primitiveTypes.map(x => x.typeName -> x).toMap ++
      Map(
        "integer"   -> IntType,
        "bigint"    -> LongType,
        "tinyint"   -> ByteType,
        "smallint"  -> ShortType,
        "varchar"   -> StringType,
        "varbinary" -> BinaryType,
        "sql"       -> SQLExprType,
        "timestamp" -> TimestampType(TimestampField.TIMESTAMP, withTimeZone = false)
      ).map { case (k, v) =>
        Name.typeName(k) -> v
      }

  def getPrimitiveTypeTable: Map[TypeName, DataType] = primitiveTypeTable

  /**
    * data type names that will be mapped to GenericType
    */
  val knownGenericTypeNames: Set[TypeName] = Set(
    "char",
    // trino-specific types
    "bingtile",
    "ipaddress",
    "jsonpath",
    "joniregexp",
    "tdigest",
    "qdigest",
    "uuid",
    "hyperloglog",
    "geometry",
    "p4hyperloglog",
    // lambda
    "function"
  ).map(x => Name.typeName(x))

  def isKnownGenericTypeName(s: String): Boolean   = isKnownGenericTypeName(Name.typeName(s))
  def isKnownGenericTypeName(s: TypeName): Boolean = knownGenericTypeNames.contains(s)

  def isPrimitiveTypeName(s: String): Boolean         = isPrimitiveTypeName(Name.typeName(s))
  def isPrimitiveTypeName(tpeName: TypeName): Boolean = primitiveTypeTable.contains(tpeName)

  def getPrimitiveType(s: String): DataType = primitiveTypeTable.getOrElse(
    Name.typeName(s),
    throw new IllegalArgumentException(s"Unknown primitive type name: ${s}")
  )

  def knownPrimitiveTypes: Map[TypeName, DataType] = DataType
    .getPrimitiveTypeTable
    .map { case (name, dataType) =>
      name -> dataType
    }

  abstract class PrimitiveType(name: String) extends DataType(Name.typeName(name), Seq.empty):
    override def isResolved: Boolean = true

  // calendar date (year, month, day)
  case object DateType extends PrimitiveType("date")

  case object UnknownType extends PrimitiveType("?"):
    override def isResolved: Boolean = false

  case object AnyType     extends PrimitiveType("any")
  case object NullType    extends PrimitiveType("null")
  case object BooleanType extends PrimitiveType("boolean")

  abstract class NumericType(name: String) extends PrimitiveType(name):
    override def isResolved: Boolean = true

  case object ByteType  extends NumericType("byte")
  case object ShortType extends NumericType("short")
  case object IntType   extends NumericType("int")
  case object LongType  extends NumericType("long")

  abstract class FractionType(name: String) extends NumericType(name):
    override def isResolved: Boolean = true

  case object FloatType  extends FractionType("float")
  case object RealType   extends FractionType("real")
  case object DoubleType extends FractionType("double")

  case class CharType(length: Option[DataType])
      extends DataType(Name.typeName("char"), length.toSeq):
    override def isResolved: Boolean = length.forall(_.isResolved)

  case object StringType extends PrimitiveType("string")
  case class VarcharType(length: Option[DataType])
      extends DataType(Name.typeName("varchar"), length.toSeq):
    override def isResolved: Boolean = length.forall(_.isResolved)

  case class DecimalType(precision: TypeParameter, scale: TypeParameter)
      extends DataType(Name.typeName("decimal"), Seq(precision, scale)):
    override def isResolved: Boolean = precision.isResolved && scale.isResolved

  object DecimalType:
    def of(precision: Int, scale: Int): DecimalType = DecimalType(
      IntConstant(precision),
      IntConstant(scale)
    )

    def of(precision: DataType, scale: DataType): DecimalType =
      (precision, scale) match
        case (p: TypeParameter, s: TypeParameter) =>
          DecimalType(p, s)
        case _ =>
          throw IllegalArgumentException(s"Invalid DecimalType parameters (${precision}, ${scale})")

  case object JsonType   extends PrimitiveType("json")
  case object BinaryType extends PrimitiveType("binary")

  case class ArrayType(elemType: DataType) extends DataType(Name.typeName("array"), Seq(elemType)):
    override def isResolved: Boolean = elemType.isResolved

  case class MapType(keyType: DataType, valueType: DataType)
      extends DataType(Name.typeName(s"map"), Seq(keyType, valueType)):
    override def isResolved: Boolean = keyType.isResolved && valueType.isResolved

  case class RecordType(elems: Seq[DataType]) extends DataType(Name.typeName("record"), elems):
    override def isResolved: Boolean = elems.forall(_.isResolved)

  /**
    * For describing the type of 'select *'
    */
  case class EmbeddedRecordType(elems: Seq[DataType]) extends DataType(Name.typeName("*"), elems):
    override def typeDescription: String = elems.map(_.typeDescription).mkString(", ")

    override def isResolved: Boolean = elems.forall(_.isResolved)

  case object SQLExprType extends DataType(Name.typeName("sql"), Nil):
    override def isResolved: Boolean = true

end DataType
