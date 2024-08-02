package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.{FlowLangException, StatusCode}
import com.treasuredata.flow.lang.compiler.{Name, SourceFile}
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.{
  ArrayType,
  DecimalType,
  GenericType,
  IntConstant,
  MapType,
  NullType,
  TimestampField,
  TimestampType
}
import com.treasuredata.flow.lang.model.expr.Identifier
import wvlet.log.LogSupport

/**
  * {{{
  *   dataType : identifier (typeParam (',' typeParam)*)?
  *   typeParam: integerLiteral | dataType
  *
  * }}}
  *
  * @param sourceFile
  */
object DataTypeParser:
  def parse(str: String): DataType = DataTypeParser(FlowScanner(SourceFile.fromString(str))).parse()
  def parse(str: String, typeParams: List[DataType]): DataType = toDataType(str, typeParams)

  private def unexpected(msg: String): FlowLangException = StatusCode.SYNTAX_ERROR.newException(msg)
  private def toDataType(typeName: String, params: List[DataType]): DataType =
    typeName match
      case p if params.isEmpty && DataType.isPrimitiveTypeName(p) =>
        DataType.getPrimitiveType(typeName)
      case "array" if params.size == 1 =>
        ArrayType(params(0))
      case "map" if params.size == 2 =>
        MapType(params(0), params(1))
      case "decimal" =>
        if params.size != 2 then
          throw unexpected(s"decimal type requires two parameters: ${params}")
        (params(0), params(1)) match
          case (p: IntConstant, s: IntConstant) =>
            DecimalType(p, s)
          case _ =>
            throw unexpected(s"Invalid decimal type parameters: ${params}")
      case _ =>
        GenericType(Name.typeName(typeName), params)

class DataTypeParser(scanner: FlowScanner) extends LogSupport:
  import DataTypeParser.*

  private def consume(expected: FlowToken): TokenData =
    val t = scanner.nextToken()
    if t.token != expected then
      throw unexpected(s"Expected ${expected} but found ${t.token}")
    t

  private def consumeIdentifier(expected: String): TokenData =
    val t = scanner.nextToken()
    if t.token != FlowToken.IDENTIFIER || t.str != expected then
      throw unexpected(s"Expected ${expected} but found ${t.token}")
    t

  def parse(): DataType = dataType()

  private def dataType(): DataType =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.NULL =>
        consume(FlowToken.NULL)
        NullType
      case FlowToken.IDENTIFIER =>
        val id       = consume(FlowToken.IDENTIFIER)
        val typeName = id.str
        typeName match
          case "timestamp" =>
            timestampType(TimestampField.TIMESTAMP)
          case "time" =>
            timestampType(TimestampField.TIME)
          case other =>
            var params: List[DataType] = Nil
            if scanner.lookAhead().token == FlowToken.L_PAREN then
              consume(FlowToken.L_PAREN)
              params = typeParams()
              consume(FlowToken.R_PAREN)

            toDataType(typeName, params)
      case _ =>
        throw unexpected(s"Unexpected token ${t.token}")

    end match

  end dataType

  def timestampType(timestampField: TimestampField): TimestampType =
    val precision: Option[DataType] =
      scanner.lookAhead().token match
        case FlowToken.L_PAREN =>
          consume(FlowToken.L_PAREN)
          val p = typeParam()
          consume(FlowToken.R_PAREN)
          Some(p)
        case _ =>
          None

    val withTimeZone: Boolean =
      val t = scanner.lookAhead()
      t.token match
        case FlowToken.WITH =>
          consume(FlowToken.WITH)
          consumeIdentifier("time")
          consumeIdentifier("zone")
          true
        case FlowToken.IDENTIFIER if t.str == "without" =>
          consume(FlowToken.IDENTIFIER)
          consumeIdentifier("time")
          consumeIdentifier("zone")
          false
        case _ =>
          false
    TimestampType(timestampField, withTimeZone, precision)

  end timestampType

  def typeParams(): List[DataType] =
    val params = List.newBuilder[DataType]
    while scanner.lookAhead().token != FlowToken.R_PAREN do
      params += typeParam()
      if scanner.lookAhead().token == FlowToken.COMMA then
        consume(FlowToken.COMMA)
    params.result()

  def typeParam(): DataType =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.INTEGER_LITERAL =>
        val i = consume(FlowToken.INTEGER_LITERAL)
        IntConstant(i.str.toInt)
      case _ =>
        dataType()

end DataTypeParser
