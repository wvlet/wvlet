package com.treasuredata.flow.lang.model

import wvlet.airframe.ulid.{PrefixedULID, ULID}
import wvlet.log.LogSupport

abstract class DataType(val typeName: String, val typeParams: Seq[DataType]):
  override def toString: String = typeDescription

  def typeDescription: String =
    val typeStr =
      if typeParams.isEmpty then typeName
      else s"${typeName}(${typeParams.mkString(", ")})"
    if isResolved then typeStr else s"${typeStr}?"

  def baseTypeName: String = typeName

  def isBound: Boolean                                  = typeParams.forall(_.isBound)
  def bind(typeArgMap: Map[String, DataType]): DataType = this

  def isFunctionType: Boolean = false
  def isResolved: Boolean

/**
  * A base type for representing LogicalPlan node types, which return table records
  * @param typeName
  * @param typeParams
  */
sealed abstract class RelationType(override val typeName: String, override val typeParams: Seq[DataType])
    extends DataType(typeName, typeParams)

object RelationType:
  def newRelationTypeName: String = ULID.newULIDString

object DataType extends LogSupport:

//  def unapply(str: String): Option[DataType] =
//    Try(DataTypeParser.parse(str)).toOption

  case class UnresolvedType(override val typeName: String) extends DataType(typeName, Seq.empty):
    override def isResolved: Boolean = false

  case class NamedType(name: String, dataType: DataType) extends DataType(s"${name}:${dataType}", Seq.empty):
    override def isResolved: Boolean = dataType.isResolved

  /**
    * A type for representing relational table schemas
    */
  case class SchemaType(override val typeName: String, columnTypes: Seq[NamedType])
      extends RelationType(typeName, Seq.empty):
    override def typeDescription: String = typeName

    override def isResolved: Boolean = columnTypes.forall(_.isResolved)

  case object EmptyRelationType extends RelationType(RelationType.newRelationTypeName, Seq.empty):
    override def isResolved: Boolean = true

  case class UnresolvedRelationType(override val typeName: String) extends RelationType(typeName, Seq.empty):
    override def typeDescription: String = typeName
    override def isResolved: Boolean     = false

  case class AliasedType(override val typeName: String, baseType: RelationType)
      extends RelationType(typeName, Seq.empty):
    override def typeDescription: String = typeName
    override def isResolved: Boolean     = baseType.isResolved

  case class ProjectedType(override val typeName: String, projectedColumns: Seq[NamedType], baseType: RelationType)
      extends RelationType(typeName, Seq.empty):
    override def typeDescription: String = typeName
    override def isResolved: Boolean     = projectedColumns.forall(_.isResolved)

  /**
    * Aggregateed record types: (key1, key2, ...) -> [record1*]
    * @param typeName
    * @param groupingKeyTypes
    * @param valueType
    */
  case class AggregationType(override val typeName: String, groupingKeyTypes: Seq[DataType], valueType: RelationType)
      extends RelationType(typeName, Seq.empty):
    override def typeDescription: String = typeName
    override def isResolved: Boolean     = groupingKeyTypes.forall(_.isResolved) && valueType.isResolved

  case class ConcatType(override val typeName: String, inputTypes: Seq[RelationType])
      extends RelationType(typeName, Seq.empty):
    override def typeDescription: String = typeName
    override def isResolved: Boolean     = inputTypes.forall(_.isResolved)

  /**
    * Type extension
    * @param typeName
    * @param selfType
    * @param defs
    */
  case class ExtensionType(override val typeName: String, selfType: DataType, defs: Seq[FunctionType])
      extends RelationType(typeName, Seq.empty):
    override def typeDescription: String = typeName
    override def isResolved              = selfType.isResolved && defs.forall(_.isResolved)

  case class FunctionType(name: String, args: Seq[NamedType], returnType: DataType) extends DataType(name, Seq.empty):
    override def typeDescription: String =
      s"${name}(${args.mkString(", ")}): ${returnType}"

    override def isFunctionType: Boolean = true

    override def isResolved: Boolean = args.forall(_.isResolved) && returnType.isResolved

  /**
    * DataType parameter for representing concrete types like timestamp(2), and abstract types like timestamp(p).
    */
  sealed abstract class TypeParameter(name: String) extends DataType(typeName = name, typeParams = Seq.empty)

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
        case Some(t) => t
        case None    => this

  case class GenericType(override val typeName: String, override val typeParams: Seq[DataType] = Seq.empty)
      extends DataType(typeName, typeParams):
    override def isBound: Boolean = typeParams.forall(_.isBound)

    override def bind(typeArgMap: Map[String, DataType]): DataType =
      GenericType(typeName, typeParams.map(_.bind(typeArgMap)))

    override def isResolved: Boolean = typeParams.forall(_.isResolved)

  case class IntervalDayTimeType(from: String, to: String) extends DataType(s"interval", Seq.empty):
    override def toString: String = s"interval from ${from} to ${to}"

    override def isResolved: Boolean = true

  sealed trait TimestampField

  object TimestampField:
    case object TIME      extends TimestampField
    case object TIMESTAMP extends TimestampField

  case class TimestampType(field: TimestampField, withTimeZone: Boolean, precision: Option[DataType] = None)
      extends DataType(field.toString.toLowerCase, precision.toSeq):
    override def toString: String =
      val base = super.toString
      if withTimeZone then s"${base} with time zone"
      else base

    override def isResolved: Boolean = true

  private def primitiveTypes: Seq[DataType] = Seq(
    AnyType,
    NullType,
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    RealType,
    DoubleType,
    StringType,
    JsonType,
    DateType,
    BinaryType
  )

  private val primitiveTypeTable: Map[String, DataType] =
    primitiveTypes.map(x => x.typeName -> x).toMap ++
      Map(
        "int"      -> IntegerType,
        "bigint"   -> LongType,
        "tinyint"  -> ByteType,
        "smallint" -> ShortType
      )

  def getPrimitiveTypeTable: Map[String, DataType] = primitiveTypeTable

  /**
    * data type names that will be mapped to GenericType
    */
  private val knownGenericTypeNames: Set[String] = Set(
    "char",
    "varchar",
    "varbinary",
    // trino-specific types
    "bingtile",
    "ipaddress",
    "json",
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
  )

  def isKnownGenericTypeName(s: String): Boolean =
    knownGenericTypeNames.contains(s)

  def isPrimitiveTypeName(s: String): Boolean =
    primitiveTypeTable.contains(s)

  def getPrimitiveType(s: String): DataType =
    primitiveTypeTable.getOrElse(s, throw new IllegalArgumentException(s"Unknown primitive type name: ${s}"))

  def knownPrimitiveTypes: Map[String, DataType] = DataType.getPrimitiveTypeTable.map { case (name, dataType) =>
    name -> dataType
  }

  abstract class PrimitiveType(name: String) extends DataType(name, Seq.empty):
    override def isResolved: Boolean = true

  // calendar date (year, month, day)
  case object DateType extends PrimitiveType("date")

  case object UnknownType extends PrimitiveType("?"):
    override def isResolved: Boolean = false

  case object AnyType     extends PrimitiveType("any")
  case object NullType    extends PrimitiveType("null")
  case object BooleanType extends PrimitiveType("boolean")

  abstract class NumericType(typeName: String) extends PrimitiveType(typeName):
    override def isResolved: Boolean = true

  case object ByteType    extends NumericType("byte")
  case object ShortType   extends NumericType("short")
  case object IntegerType extends NumericType("integer")
  case object LongType    extends NumericType("long")

  abstract class FractionType(typeName: String) extends NumericType(typeName):
    override def isResolved: Boolean = true

  case object FloatType  extends FractionType("float")
  case object RealType   extends FractionType("real")
  case object DoubleType extends FractionType("double")

  case class CharType(length: Option[DataType]) extends DataType("char", length.toSeq):
    override def isResolved: Boolean = length.forall(_.isResolved)

  case object StringType extends PrimitiveType("string")
  case class VarcharType(length: Option[DataType]) extends DataType("varchar", length.toSeq):
    override def isResolved: Boolean = length.forall(_.isResolved)

  case class DecimalType(precision: TypeParameter, scale: TypeParameter)
      extends DataType("decimal", Seq(precision, scale)):
    override def isResolved: Boolean = precision.isResolved && scale.isResolved

  object DecimalType:
    def of(precision: Int, scale: Int): DecimalType = DecimalType(IntConstant(precision), IntConstant(scale))

    def of(precision: DataType, scale: DataType): DecimalType =
      (precision, scale) match
        case (p: TypeParameter, s: TypeParameter) => DecimalType(p, s)
        case _ =>
          throw IllegalArgumentException(s"Invalid DecimalType parameters (${precision}, ${scale})")

  case object JsonType   extends PrimitiveType("json")
  case object BinaryType extends PrimitiveType("binary")

  case class ArrayType(elemType: DataType) extends DataType(s"array", Seq(elemType)):
    override def isResolved: Boolean = elemType.isResolved

  case class MapType(keyType: DataType, valueType: DataType) extends DataType(s"map", Seq(keyType, valueType)):
    override def isResolved: Boolean = keyType.isResolved && valueType.isResolved

  case class RecordType(elems: Seq[DataType]) extends DataType("record", elems):
    override def isResolved: Boolean = elems.forall(_.isResolved)

  /**
    * For describing the type of 'select *'
    */
  case class EmbeddedRecordType(elems: Seq[DataType]) extends DataType("*", elems):
    override def typeDescription: String =
      elems.map(_.typeDescription).mkString(", ")

    override def isResolved: Boolean = elems.forall(_.isResolved)

//  def parse(typeName: String): DataType =
//    DataTypeParser.parse(typeName)
//
//  def parseArgs(typeArgs: String): List[DataType] =
//    DataTypeParser.parseTypeList(typeArgs)
