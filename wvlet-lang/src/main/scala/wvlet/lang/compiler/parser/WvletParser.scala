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

import wvlet.lang.model.expression.Expression
import wvlet.lang.model.expression.Expression.{ConditionalExpression, Identifier, QName, SelectItem, StringLiteral}
import wvlet.lang.model.logical.LogicalPlan
import wvlet.lang.model.logical.LogicalPlan.{FLOWRQuery, ForItem, Select, SchemaDef, SchemaItem, Where}
import WvletParser.EOFToken
import wvlet.log.LogSupport

import scala.annotation.tailrec

object WvletParser:
  def parse(text: String): LogicalPlan =
    val tokens: Seq[TokenData] = WvletScanner.scan(text)
    val tokenScanner           = new TokenScanner(tokens.toIndexedSeq)
    val parser                 = new WvletParser(tokenScanner)
    parser.parseStatement

  val EOFToken = TokenData(Token.EOF, "", 0, 0)

class TokenScanner(tokens: IndexedSeq[TokenData]):
  private var cursor = 0

  def peekNext: TokenData =
    if cursor < tokens.size then tokens(cursor)
    else WvletParser.EOFToken

  def next: Unit =
    cursor += 1

class WvletParser(tokenScanner: TokenScanner) extends LogSupport:

  private def parseError(token: TokenData, expected: Token): Nothing =
    // TODO define parse error exception type
    throw new IllegalArgumentException(s"parse error: expected ${expected}, but found ${token}")

  private def peekNextToken: TokenData =
    tokenScanner.peekNext

  private def nextToken: Unit =
    tokenScanner.next

  def consumeToken(token: Token): Unit =
    val currentToken = peekNextToken
    if currentToken.token == token then nextToken
    else parseError(currentToken, token)

  def parseStatement: LogicalPlan =
    val currentToken = peekNextToken
    currentToken.token match
      case Token.FOR =>
        parseFLOWRQuery
      case Token.SELECT =>
        // select-only plan
        debug(s"select: ${currentToken}")
        val sourceLocation = currentToken.getSourceLocation
        val selectClause   = parseSelect
        FLOWRQuery(forItems = Seq.empty, selectClause = Some(selectClause))(sourceLocation)
      case Token.SCHEMA =>
        parseSchema
      case Token.EOF =>
        null
      case _ =>
        null

  def parseFLOWRQuery: FLOWRQuery =
    val currentToken = peekNextToken
    currentToken.token match    
      case Token.FOR =>
        nextToken
        val forItems: Seq[ForItem] = parseForItems

        val next = peekNextToken

        // TODO parse group by, join, etc.
        var whereClause: Option[Where]   = None
        var selectClause: Option[Select] = None
        next.token match
          case Token.WHERE =>
            whereClause = Some(parseWhere)
            peekNextToken.token match
              case Token.SELECT =>
                selectClause = Some(parseSelect)
              case _ =>

          case Token.SELECT =>
            selectClause = Some(parseSelect)
          case _ =>
        FLOWRQuery(forItems = forItems, whereClause, selectClause)(currentToken.getSourceLocation)
      case _ =>
        null

  private def parseForItems: Seq[ForItem] =
    // identifier IN expr
    val items        = Seq.newBuilder[ForItem]
    var currentToken = peekNextToken
    while currentToken.token == Token.IDENTIFIER do
      val id = currentToken.text
      nextToken
      parseIn
      val expr = parseExpression
      items += ForItem(id, expr)(currentToken.getSourceLocation)
      currentToken = peekNextToken
    items.result

  private def parseIn: Unit =
    val currentToken = peekNextToken
    if currentToken.token == Token.IN then nextToken
    else parseError(currentToken, Token.IN)

  private def parseWhere: Where =
    val currentToken = peekNextToken
    if currentToken.token == Token.WHERE then
      nextToken
      val expr = parseExpression
      Where(expr)(currentToken.getSourceLocation)
    else parseError(currentToken, Token.WHERE)

  private def parseSelect: Select =
    val currentToken = peekNextToken
    if currentToken.token == Token.SELECT then
      nextToken
      val selectItems = parseSelectItems
      Select(selectItems)(currentToken.getSourceLocation)
    else parseError(currentToken, Token.SELECT)

  private def parseSelectItems: Seq[SelectItem] =
    val items = Seq.newBuilder[SelectItem]

    @tailrec
    def loop: Unit =
      val ri = parseSelectItem
      items += ri
      if peekNextToken.token == Token.COMMA then
        nextToken
        loop
    loop
    items.result()

  private def parseSelectItem: SelectItem =
    val currentToken = peekNextToken
    currentToken.token match
      case Token.IDENTIFIER =>
        val qName = parseQualifiedName
        peekNextToken.token match
          case Token.COLON =>
            nextToken
            val expr = parseExpression
            SelectItem(Some(qName), expr)
          case _ =>
            SelectItem(None, qName)
      case _ =>
        val expr = parseExpression
        SelectItem(None, expr)

  private def parseSchema: SchemaDef =
    val schemaLoc = peekNextToken.getSourceLocation
    consumeToken(Token.SCHEMA)
    val schemaName = parseQualifiedName
    consumeToken(Token.COLON)

    val schemaItems = Seq.newBuilder[SchemaItem]

    def parseSchemaItem: Unit =
      val currentToken = peekNextToken
      trace(s"parseSchemaItem: ${currentToken}")
      currentToken.token match
        case Token.IDENTIFIER =>
          val id = currentToken.text
          nextToken
          consumeToken(Token.COLON)
          val dataType = parseDataType
          schemaItems += SchemaItem(id, dataType)(currentToken.getSourceLocation)
          parseSchemaItem
        case _ =>

    def parseSchemaItems: Unit =
      parseSchemaItem
      while peekNextToken.token == Token.COMMA do
        nextToken
        parseSchemaItem
    parseSchemaItems

    SchemaDef(schemaName.toString, schemaItems.result())(schemaLoc)

  private def parseDataType: String =
    val currentToken = peekNextToken
    currentToken.token match
      case Token.IDENTIFIER =>
        val dataType = currentToken.text
        nextToken
        dataType
      // TODO: Parse array, map, nested types, etc.
      case _ =>
        parseError(currentToken, Token.IDENTIFIER)

  private def parseExpressions: Seq[Expression] =
    val expr = parseExpression
    debug(s"parse expressions: ${expr}")
    peekNextToken.token match
      case Token.COMMA =>
        nextToken
        expr +: parseExpressions
      case _ =>
        Seq(expr)

  private def parseExpression: Expression =
    val currentToken = peekNextToken
    currentToken.token match
      case Token.IDENTIFIER =>
        val qName = parseQualifiedName
        val op    = peekNextToken
        op.token match
          // expression comparisonOperator expression
          case Token.EQ | Token.NEQ | Token.LT | Token.LTEQ | Token.GT | Token.GTEQ =>
            val ops = op.token
            nextToken
            val rhs = parseExpression
            ops match
              case Token.EQ   => Expression.Equal(qName, rhs)
              case Token.NEQ  => Expression.NotEqual(qName, rhs)
              case Token.LT   => Expression.LessThan(qName, rhs)
              case Token.LTEQ => Expression.LessThanOrEqual(qName, rhs)
              case Token.GT   => Expression.GreaterThan(qName, rhs)
              case Token.GTEQ => Expression.GreaterThanOrEqual(qName, rhs)
              case other =>
                parseError(op, Token.EQ)
          case _ =>
            qName
      case Token.STRING_LITERAL =>
        nextToken
        val text = currentToken.text
        Expression.StringLiteral(currentToken.text)(currentToken.getSourceLocation)
      case Token.TRUE =>
        nextToken
        Expression.TrueLiteral(currentToken.text)(currentToken.getSourceLocation)
      case Token.FALSE =>
        nextToken
        Expression.FalseLiteral(currentToken.text)(currentToken.getSourceLocation)
      case Token.NULL =>
        nextToken
        Expression.NullLiteral(currentToken.text)(currentToken.getSourceLocation)
      case Token.INTEGER_LITERAL =>
        nextToken
        currentToken.text match
          case hex if hex.startsWith("0x") || hex.startsWith("0X") =>
            Expression.IntegerLiteral(hex, Integer.parseInt(hex.substring(2), 16))(
              currentToken.getSourceLocation
            )
          case _ =>
            Expression.IntegerLiteral(currentToken.text, currentToken.text.toInt)(
              currentToken.getSourceLocation
            )
      case Token.DECIMAL_LITERAL =>
        nextToken
        Expression.DecimalLiteral(currentToken.text, BigDecimal(currentToken.text))(
          currentToken.getSourceLocation
        )
      case Token.FLOAT_LITERAL =>
        nextToken
        Expression.FloatLiteral(currentToken.text, currentToken.text.toFloat)(
          currentToken.getSourceLocation
        )
      case Token.DOUBLE_LITERAL =>
        nextToken
        Expression.DoubleLiteral(currentToken.text, currentToken.text.toDouble)(
          currentToken.getSourceLocation
        )
      case Token.EXP_LITERAL =>
        nextToken
        Expression.ExpLiteral(currentToken.text, java.lang.Double.parseDouble(currentToken.text))(
          currentToken.getSourceLocation
        )
      case _ =>
        parseError(currentToken, Token.IDENTIFIER)

  private def parseQualifiedName: QName =
    val qNameBuffer = Seq.newBuilder[String]
    val firstToken  = peekNextToken

    @tailrec
    def loop(t: TokenData): QName =
      t.token match
        case Token.IDENTIFIER =>
          qNameBuffer += t.text
          nextToken
          loop(peekNextToken)
        case Token.DOT =>
          nextToken
          loop(peekNextToken)
        case _ =>
          val qName = qNameBuffer.result()
          if qName.isEmpty then parseError(t, Token.IDENTIFIER)
          else QName(qName)(firstToken.getSourceLocation)

    loop(firstToken)

  private def parseIdentifierRest(lastToken: TokenData): Expression =
    val currentToken = peekNextToken
    currentToken.token match
      case Token.DOT =>
        nextToken
        val next = peekNextToken
        next.token match
          case Token.IDENTIFIER =>
            nextToken
            parseIdentifierRest(next)
          case _ =>
            parseError(next, Token.IDENTIFIER)
      case _ =>
        Identifier(lastToken.text)(lastToken.getSourceLocation)
