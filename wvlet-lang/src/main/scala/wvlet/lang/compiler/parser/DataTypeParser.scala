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
package wvlet.lang.compiler.parser

import wvlet.lang.api.{WvletLangException, StatusCode}
import wvlet.lang.compiler.{Name, SourceFile}
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.{
  ArrayType,
  DecimalType,
  FixedSizeArrayType,
  GenericType,
  IntConstant,
  MapType,
  NamedType,
  NullType,
  TimestampField,
  TimestampType,
  TypeParameter,
  TypeVariable,
  VarcharType
}
import wvlet.lang.model.expr.Identifier
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
object DataTypeParser extends LogSupport:
  def parse(str: String): DataType = DataTypeParser(WvletScanner(SourceFile.fromWvletString(str)))
    .parse()

  def parse(str: String, typeParams: List[DataType]): DataType = toDataType(str, typeParams)

  private def unexpected(msg: String): WvletLangException = StatusCode
    .SYNTAX_ERROR
    .newException(msg)

  private def toDataType(typeName: String, params: List[DataType]): DataType =
    typeName match
      case p if params.isEmpty && DataType.isPrimitiveTypeName(p) =>
        DataType.getPrimitiveType(typeName)
      case "varchar" if params.size <= 1 =>
        if params.isEmpty then
          DataType.getPrimitiveType("varchar")
        else
          VarcharType(params.headOption)
      case "array" if params.size == 1 =>
        ArrayType(params(0))
      case "map" if params.size == 2 =>
        MapType(params(0), params(1))
      case "decimal" =>
        if params.size == 0 then
          // Use the default precision and scale of DuckDb
          DecimalType(IntConstant(18), IntConstant(3))
        else if params.size != 2 then
          throw unexpected(s"decimal type requires two parameters: ${params}")
        else
          (params(0), params(1)) match
            case (p: TypeParameter, s: TypeParameter) =>
              DecimalType(p, s)
            case _ =>
              throw unexpected(s"Invalid decimal type parameters: ${params}")
      case _ =>
        GenericType(Name.typeName(typeName), params)

end DataTypeParser

class DataTypeParser(scanner: WvletScanner) extends LogSupport:
  import DataTypeParser.*

  private def consume(expected: WvletToken): TokenData[WvletToken] =
    val t = scanner.nextToken()
    if t.token != expected then
      throw unexpected(s"Expected ${expected} but found ${t.token}")
    t

  private def consumeToken(): TokenData[WvletToken] =
    val t = scanner.nextToken()
    t

  private def consumeIdentifier(expected: String): TokenData[WvletToken] =
    val t = scanner.nextToken()
    if t.token != WvletToken.IDENTIFIER || t.str.toLowerCase != expected then
      throw unexpected(s"Expected ${expected} but found ${t.token}")
    t

  def parse(): DataType = dataType()

  private def dataType(): DataType =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.NULL =>
        consume(WvletToken.NULL)
        NullType
      case s if s.isStringLiteral && t.str.toLowerCase == "null" =>
        consumeToken()
        NullType
      case token if token.isIdentifier || token.isReservedKeyword =>
        val id       = consume(t.token)
        val typeName = id.str.toLowerCase
        typeName match
          case "timestamp" =>
            timestampType(TimestampField.TIMESTAMP)
          case "time" =>
            timestampType(TimestampField.TIME)
          case other =>
            var params: List[DataType] = Nil
            val tt                     = scanner.lookAhead()
            tt.token match
              case WvletToken.L_PAREN =>
                consume(WvletToken.L_PAREN)
                params = typeParams()
                consume(WvletToken.R_PAREN)
                toDataType(typeName, params)
              case WvletToken.L_BRACKET =>
                // duckdb list types: integer[], varchar[], ...
                consume(WvletToken.L_BRACKET)
                val t2 = scanner.lookAhead()
                t2.token match
                  case WvletToken.R_BRACKET =>
                    consume(WvletToken.R_BRACKET)
                    ArrayType(toDataType(typeName, Nil))
                  case WvletToken.IDENTIFIER =>
                    consume(WvletToken.IDENTIFIER)
                    consume(WvletToken.R_BRACKET)
                    ArrayType(toDataType(typeName, Nil))
                  case WvletToken.INTEGER_LITERAL =>
                    val size = consume(WvletToken.INTEGER_LITERAL)
                    consume(WvletToken.R_BRACKET)
                    FixedSizeArrayType(toDataType(typeName, Nil), size.str.toInt)
                  case _ =>
                    throw unexpected(s"Unexpected token ${t2}")
              case _ => // do nothing
                toDataType(typeName, params)
        end match
      case _ =>
        throw unexpected(s"Unexpected token ${t}")
    end match

  end dataType

  def timestampType(timestampField: TimestampField): TimestampType =
    val precision: Option[DataType] =
      scanner.lookAhead().token match
        case WvletToken.L_PAREN =>
          consume(WvletToken.L_PAREN)
          val p = typeParam()
          consume(WvletToken.R_PAREN)
          Some(p)
        case _ =>
          None

    val withTimeZone: Boolean =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.WITH =>
          consume(WvletToken.WITH)
          consumeIdentifier("time")
          consumeIdentifier("zone")
          true
        case WvletToken.IDENTIFIER if t.str == "without" =>
          consume(WvletToken.IDENTIFIER)
          consumeIdentifier("time")
          consumeIdentifier("zone")
          false
        case _ =>
          false
    TimestampType(timestampField, withTimeZone, precision)

  end timestampType

  def typeParams(): List[DataType] =
    val params = List.newBuilder[DataType]
    while !scanner.lookAhead().token.isRightParenOrBracket do
      params += typeParam()
      if scanner.lookAhead().token == WvletToken.COMMA then
        consume(WvletToken.COMMA)
    params.result()

  def typeParam(): DataType =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.INTEGER_LITERAL =>
        val i = consume(WvletToken.INTEGER_LITERAL)
        IntConstant(i.str.toInt)
      case s if s.isStringLiteral =>
        val ts        = consumeToken()
        val paramName = ts.str.toLowerCase
        val paramType = dataType()
        NamedType(Name.termName(paramName), paramType)
      case WvletToken.IDENTIFIER if !DataType.isKnownTypeName(t.str.toLowerCase) =>
        val id = consume(WvletToken.IDENTIFIER)
        TypeVariable(Name.typeName(id.str.trim))
      case _ =>
        dataType()

end DataTypeParser
