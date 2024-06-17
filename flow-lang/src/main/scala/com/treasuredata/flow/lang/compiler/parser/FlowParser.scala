package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.parser.FlowToken.{FOR, FROM, R_PAREN}
import com.treasuredata.flow.lang.compiler.{CompilationUnit, SourceFile}
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.plan.*
import wvlet.log.LogSupport

/**
  * {{{
  *   [Flow Language Grammar]
  *
  *   packageDef: 'package' qualifiedId statement*
  *
  *   qualifiedId: identifier ('.' identifier)*
  *
  *   identifier  : IDENTIFIER
  *               | BACKQUOTED_IDENTIFIER
  *               | reserved  # Necessary to use reserved words as identifiers
  *   IDENTIFIER  : (LETTER | '_') (LETTER | DIGIT | '_')*
  *   BACKQUOTED_IDENTIFIER: '`' (~'`' | '``')+ '`'
  *   reserved   : 'from' | 'select' | 'where' | 'group' | 'by' | 'having'
  *              | 'order' | 'limit' | 'as' | 'model' | 'type' | 'def' | 'end'
  *
  *   statements: statement+
  *
  *
  *   statement: importStatement
  *            | modelDef
  *            | query
  *            | functionDef
  *            | test
  *
  *   importStatement: 'import' importExpr (',' importExpr)*
  *   importExpr     : importRef (from str)?
  *   importRef      : qualifiedId ('.' '*')?
  *                  | qualifiedId 'as' identifier
  *
  *   modelDef   : 'model' identifier modelParams? (':' qualifiedId)? '=' modelBody
  *   modelBody  : query 'end'
  *   modelParams: '(' modelParam (',' modelParam)* ')'
  *   modelParam : identifier ':' identifier ('=' expression)?
  *
  *   query: 'from' relation
  *          queryBlock*
  *          ('order' 'by' sortItem (',' sortItem)* comma?)?
  *          ('limit' INTEGER_VALUE)?
  *
  *   relation       : relationPrimary ('as' identifier)?
  *   relationPrimary: qualifiedId
  *                  | '(' relation ')'
  *                  | str
  *
  *   typeDef    : 'type' identifier typeParams? context? ':' typeElem* 'end'
  *   typeParams : '[' typeParam (',' typeParam)* ']'
  *   typeParam  : identifier (':' identifier)?
  *   typeElem   : valDef | funDef
  *
  *   valDef     : identifier ':' identifier ('=' expression)?
  *   funDef:    : 'def' identifier defParams? (':' identifier)? ('=' expression)?
  *   funName    : identifier | symbol
  *   symbol     : '+' | '-' | '*' | '/' | '%' | '&' | '|' | '=' | '==' | '!=' | '<' | '<=' | '>' | '>=' | '&&' | '||'
  *   funParams  : '(' defParam (',' defParam)* ')'
  *   funParam   : identifier ':' identifier ('=' expression)?
  *
  *   context    : '(' 'in' contextItem (',' contextItem)* ')'
  *   contextItem: identifier (':' identifier)?
  *
  *   strInterpolation: identifier
  *                   | '"' stringPart* '"'
  *                   | '"""' stringPart* '"""'  # triple quotes string
  *   stringPart      : stringLiteral | '${' expression '}'
  *
  *
  *   expression        : booleanExpression
  *   booleanExpression : ('!' | 'not') booleanExpression
  *                     | valueExpression
  *                     | booleanExpression ('and' | 'or') booleanExpression
  *   valueExpression   : primaryExpression
  *                     | valueExpression arithmeticOperator valueExpression
  *                     | valueExpression comparisonOperator valueExpression
  *   arithmeticOperator: '+' | '-' | '*' | '/' | '%'
  *   comparisonOperator: '=' | '==' | 'is' | '!=' | 'is' 'not' | '<' | '<=' | '>' | '>='
  *
  *   // Expresion that can be chained with '.' operator
  *   primaryExpression : 'this'
  *                     | '_'
  *                     | literal
  *                     | functionCall
  *                     | '(' query ')'                                                 # subquery
  *                     | '(' expression ')'                                            # parenthesized expression
  *                     | '[' expression (',' expression)* ']'                          # array
  *                     | qualifiedId
  *                     | primaryExpression '.' primaryExpression
  *
  *   functionCall      | primaryExpression '(' functionArg? (',' functionArg)* ')'     # function call
  *                     | primaryExpression identifier expression                       # function infix
  *                     |
  *   functionArg       | (identifier '=')? expression
  *
  *   literal           : 'null' | '-'? integerLiteral | '-'? floatLiteral | booleanLiteral | stringLiteral
  *
  * }}}
  *
  * @param unit
  */
class FlowParser(unit: CompilationUnit) extends LogSupport:

  given src: SourceFile                  = unit.sourceFile
  given compilationUnit: CompilationUnit = unit

  private val scanner = FlowScanner(unit.sourceFile, ScannerConfig(skipComments = true))

  def parse(): LogicalPlan =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.PACKAGE => packageDef()
      case _ =>
        val stmts = statements()
        PackageDef(None, stmts, unit.sourceFile, t.nodeLocation)

  // private def sourceLocation: SourceLocation = SourceLocation(unit.sourceFile, nodeLocation())

  def consume(expected: FlowToken): TokenData =
    val t = scanner.nextToken()
    if t.token == expected then t
    else throw StatusCode.SYNTAX_ERROR.newException(s"Expected ${expected}, but found ${t.token}", t.sourceLocation)

  private def unexpected(t: TokenData): Nothing =
    throw StatusCode.SYNTAX_ERROR.newException(s"Unexpected token: ${t}", t.sourceLocation)

  private def unexpected(expr: Expression): Nothing =
    throw StatusCode.SYNTAX_ERROR.newException(s"Unexpected expression: ${expr}", expr.sourceLocation)

  def identifier(): Name =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.IDENTIFIER =>
        consume(FlowToken.IDENTIFIER)
        UnquotedIdentifier(t.str, t.nodeLocation)
      case FlowToken.STAR =>
        consume(FlowToken.STAR)
        Wildcard(t.nodeLocation)
      case FlowToken.UNDERSCORE =>
        consume(FlowToken.UNDERSCORE)
        ContextRef(t.nodeLocation)
      case _ =>
        reserved()

  def reserved(): Name =
    val t = scanner.nextToken()
    t.token match
      case FlowToken.FROM | FlowToken.SELECT | FlowToken.WHERE | FlowToken.GROUP | FlowToken.BY | FlowToken.HAVING |
          FlowToken.ORDER | FlowToken.LIMIT | FlowToken.AS | FlowToken.MODEL | FlowToken.TYPE | FlowToken.DEF |
          FlowToken.END =>
        UnquotedIdentifier(t.str, t.nodeLocation)
      case _ =>
        unexpected(t)

  /**
    * PackageDef := 'package' qualifiedId (statement)*
    */
  def packageDef(): PackageDef =
    val t = scanner.nextToken()
    val packageName: Option[Expression] = t.token match
      case FlowToken.PACKAGE =>
        val packageName = qualifiedId()
        Some(packageName)
      case _ =>
        None

    val stmts = statements()
    PackageDef(packageName, stmts, unit.sourceFile, t.nodeLocation)

  /**
    * statements := statement+
    * @return
    */
  def statements(): List[LogicalPlan] =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.EOF =>
        List.empty
      case _ =>
        val stmt: LogicalPlan = statement()
        stmt :: statements()

  /**
    * statement := importStatement \| query \| functionDef \| tableDef \| subscribeDef \| moduleDef \| test
    */
  def statement(): LogicalPlan =
    val t = scanner.lookAhead()
    t.token match
//      case FlowToken.IMPORT =>
//        parseImportStatement()
      case FlowToken.FROM =>
        query()
      case FlowToken.SELECT =>
        Query(select(), t.nodeLocation)
      case FlowToken.TYPE =>
        typeDef()
      case _ =>
        unexpected(t)

  def typeDef(): TypeDef =
    val t    = consume(FlowToken.TYPE)
    val name = identifier()
    consume(FlowToken.COLON)
    val elems = typeElems()
    consume(FlowToken.END)
    TypeDef(name, elems, t.nodeLocation)

  def typeElems(): List[TypeElem] =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.EOF | FlowToken.END =>
        List.empty
      case _ =>
        val e = typeElem()
        e :: typeElems()

  /**
    * {{{
    * typeElem := 'def' identifier (':' identifier)? ('=' expression)?
    *          | identifier ':' identifier ('=' expression)?
    * }}}
    * @return
    */
  def typeElem(): TypeElem =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.DEF =>
        funDef()
      case FlowToken.IDENTIFIER =>
        val name = identifier()
        consume(FlowToken.COLON)
        val valType = identifier()
        val defaultValue = scanner.lookAhead().token match
          case FlowToken.EQ =>
            consume(FlowToken.EQ)
            Some(expression())
          case _ => None
        TypeValDef(name, valType, defaultValue, t.nodeLocation)
      case _ =>
        unexpected(t)

  def funDef(): TypeDefDef =
    val t    = consume(FlowToken.DEF)
    val name = funName()
    val args: List[FunctionArg] = scanner.lookAhead().token match
      case FlowToken.L_PAREN =>
        consume(FlowToken.L_PAREN)
        val args = argExprs()
        consume(FlowToken.R_PAREN)
        args
      case _ =>
        Nil

    val defScope: List[DefScope] = scanner.lookAhead().token match
      case FlowToken.L_PAREN =>
        consume(FlowToken.L_PAREN)
        consume(FlowToken.IN)
        val scopes = List.newBuilder[DefScope]
        def nextScope: Unit =
          val t = scanner.lookAhead()
          t.token match
            case FlowToken.COMMA =>
              consume(FlowToken.COMMA)
              nextScope
            case FlowToken.R_PAREN =>
            // ok
            case _ =>
              val nameOrType = identifier()
              scanner.lookAhead().token match
                case FlowToken.COLON =>
                  consume(FlowToken.COLON)
                  val tpe = identifier()
                  scopes += DefScope(Some(nameOrType), tpe, t.nodeLocation)
                  nextScope
                case FlowToken.COMMA | FlowToken.R_PAREN =>
                  scopes += DefScope(None, nameOrType, t.nodeLocation)
                  nextScope
        nextScope
        consume(FlowToken.R_PAREN)
        scopes.result()
      case _ => Nil

    val retType: Option[Name] = scanner.lookAhead().token match
      case FlowToken.COLON =>
        consume(FlowToken.COLON)
        Some(identifier())
      case _ =>
        None

    val body: Option[Expression] = scanner.lookAhead().token match
      case FlowToken.EQ =>
        consume(FlowToken.EQ)
        Some(expression())
      case _ =>
        None
    TypeDefDef(name, args, defScope, retType, body, t.nodeLocation)

  def funName(): Name =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.IDENTIFIER =>
        identifier()
      case _ =>
        symbol()

  def symbol(): Name =
    val t = scanner.nextToken()
    t.token match
      case FlowToken.PLUS | FlowToken.MINUS | FlowToken.STAR | FlowToken.DIV | FlowToken.MOD | FlowToken.AMP |
          FlowToken.PIPE | FlowToken.EQ | FlowToken.NEQ | FlowToken.LT | FlowToken.LTEQ | FlowToken.GT |
          FlowToken.GTEQ =>
        UnquotedIdentifier(t.str, t.nodeLocation)
      case _ =>
        unexpected(t)

  /**
    * query := 'from' fromRelation queryBlock*
    */
  def query(): Relation =
    val t = consume(FlowToken.FROM)
    val r = fromRelation()
    Query(r, t.nodeLocation)

  /**
    * fromRelation := relationPrimary ('as' identifier)?
    * @return
    */
  def fromRelation(): Relation =
    val primary = relationPrimary()
    val t       = scanner.lookAhead()
    t.token match
      case FlowToken.AS =>
        consume(FlowToken.AS)
        val alias = identifier()
        AliasedRelation(primary, alias, None, t.nodeLocation)
      case _ =>
        primary

  /**
    * relationPrimary := qualifiedId \| '(' query ')' \| stringLiteral
    * @return
    */
  def relationPrimary(): Relation =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.IDENTIFIER =>
        TableRef(qualifiedId(), t.nodeLocation)
      case FlowToken.L_PAREN =>
        consume(FlowToken.L_PAREN)
        val q = query()
        consume(FlowToken.R_PAREN)
        ParenthesizedRelation(q, t.nodeLocation)
      case FlowToken.STRING_LITERAL =>
        consume(FlowToken.STRING_LITERAL)
        FileScan(t.str, t.nodeLocation)
      case _ => ???

  def select(): Relation =
    val t     = consume(FlowToken.SELECT)
    val attrs = attributeList()
    Project(EmptyRelation(t.nodeLocation), attrs, t.nodeLocation)

  def attributeList(): List[Attribute] =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.EOF =>
        List.empty
      case _ =>
        val e = attribute()
        e :: attributeList()

  def attribute(): Attribute =
    val t = scanner.lookAhead()
    SingleColumn(expression(), Qualifier.empty, t.nodeLocation)

  def expression(): Expression = booleanExpression()

  def booleanExpression(): Expression =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.EXCLAMATION | FlowToken.NOT =>
        consume(FlowToken.EXCLAMATION)
        val e = booleanExpression()
        Not(e, t.nodeLocation)
      case _ =>
        val expr = valueExpression()
        booleanExpressionRest(expr)

  def booleanExpressionRest(expression: Expression): Expression =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.AND =>
        consume(FlowToken.AND)
        val right = booleanExpression()
        And(expression, right, t.nodeLocation)
      case FlowToken.OR =>
        consume(FlowToken.OR)
        val right = booleanExpression()
        Or(expression, right, t.nodeLocation)
      case _ =>
        expression

  def valueExpression(): Expression =
    val expr = primaryExpression()
    valueExpressionRest(expr)

  def valueExpressionRest(expression: Expression): Expression =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.PLUS =>
        consume(FlowToken.PLUS)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Add, expression, right, t.nodeLocation)
      case FlowToken.MINUS =>
        consume(FlowToken.MINUS)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Subtract, expression, right, t.nodeLocation)
      case FlowToken.STAR =>
        consume(FlowToken.STAR)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Multiply, expression, right, t.nodeLocation)
      case FlowToken.DIV =>
        consume(FlowToken.DIV)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Divide, expression, right, t.nodeLocation)
      case FlowToken.MOD =>
        consume(FlowToken.MOD)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Modulus, expression, right, t.nodeLocation)
      case _ =>
        expression

  def primaryExpression(): Expression =
    val t = scanner.lookAhead()
    val expr: Expression = t.token match
      case FlowToken.THIS =>
        consume(FlowToken.THIS)
        This(t.nodeLocation)
      case FlowToken.UNDERSCORE =>
        consume(FlowToken.UNDERSCORE)
        ContextRef(t.nodeLocation)
      case FlowToken.NULL | FlowToken.INTEGER_LITERAL | FlowToken.DOUBLE_LITERAL | FlowToken.FLOAT_LITERAL |
          FlowToken.DECIMAL_LITERAL | FlowToken.EXP_LITERAL | FlowToken.STRING_LITERAL =>
        literal()
      case FlowToken.STRING_INTERPOLATION_PREFIX =>
        interpolatedString()
      case FlowToken.L_PAREN =>
        consume(FlowToken.L_PAREN)
        val t2 = scanner.lookAhead()
        t2.token match
          case FlowToken.FROM =>
            val q = query()
            consume(FlowToken.R_PAREN)
            SubQueryExpression(q, t2.nodeLocation)
          case _ =>
            val e = expression()
            consume(FlowToken.R_PAREN)
            ParenthesizedExpression(e, t.nodeLocation)
      case FlowToken.IDENTIFIER | FlowToken.STAR | FlowToken.END =>
        identifier()
      case _ =>
        unexpected(t)
    primaryExpressionRest(expr)

  def literal(): Literal =
    val t = scanner.nextToken()
    t.token match
      case FlowToken.NULL =>
        NullLiteral(t.nodeLocation)
      case FlowToken.INTEGER_LITERAL =>
        LongLiteral(t.str.toInt, t.nodeLocation)
      case FlowToken.DOUBLE_LITERAL =>
        DoubleLiteral(t.str.toDouble, t.nodeLocation)
      case FlowToken.FLOAT_LITERAL =>
        DoubleLiteral(t.str.toFloat, t.nodeLocation)
      case FlowToken.DECIMAL_LITERAL =>
        DecimalLiteral(t.str, t.nodeLocation)
      case FlowToken.EXP_LITERAL =>
        DecimalLiteral(t.str, t.nodeLocation)
      case FlowToken.STRING_LITERAL =>
        StringLiteral(t.str, t.nodeLocation)
      case _ =>
        unexpected(t)

  def interpolatedString(): InterpolatedString =
    val prefix     = consume(FlowToken.STRING_INTERPOLATION_PREFIX)
    val prefixNode = UnquotedIdentifier(prefix.str, prefix.nodeLocation)
    val parts      = List.newBuilder[Expression]

    def nextPart(): Unit =
      val t = scanner.lookAhead()
      t.token match
        case FlowToken.STRING_PART =>
          val part = consume(FlowToken.STRING_PART)
          parts += StringLiteral(part.str, part.nodeLocation)
          nextPart()
        case FlowToken.L_BRACE =>
          consume(FlowToken.L_BRACE)
          val expr = expression()
          consume(FlowToken.R_BRACE)
          parts += expr
          nextPart()
        case _ =>

    while scanner.lookAhead().token == FlowToken.STRING_PART do nextPart()
    if scanner.lookAhead().token == FlowToken.STRING_LITERAL then
      val part = consume(FlowToken.STRING_PART)
      parts += StringLiteral(part.str, part.nodeLocation)

    InterpolatedString(prefixNode, parts.result(), prefix.nodeLocation)

  def nameExpression(): Name =
    primaryExpression() match
      case n: Name => n
      case other   => unexpected(other)

  def primaryExpressionRest(expr: Expression): Expression =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.DOT =>
        expr match
          case n: Name =>
            val next = nameExpression()
            Ref(n, next, t.nodeLocation)
          case _ =>
            unexpected(expr)

      case FlowToken.L_PAREN | FlowToken.L_BRACE =>
        val args = argExprs()
        FunctionApply(expr, args, t.nodeLocation)
      case _ =>
        expr

  def argExprs(): List[FunctionArg] =
    val args = List.newBuilder[FunctionArg]

    def nextArg: Unit =
      val t = scanner.lookAhead()
      t.token match
        case FlowToken.COMMA =>
          consume(FlowToken.COMMA)
          nextArg
        case FlowToken.R_PAREN =>
        // ok
        case _ =>
          args += functionArg()
          nextArg

    nextArg
    args.result()

  def functionArg(): FunctionArg =
    val name = identifier()
    val t    = scanner.lookAhead()
    t.token match
      case FlowToken.COLON =>
        consume(FlowToken.COLON)
        val tpe = identifier()
        val defaultValue = scanner.lookAhead().token match
          case FlowToken.EQ =>
            consume(FlowToken.EQ)
            Some(expression())
          case _ =>
            None
        FunctionArg(name, tpe, defaultValue, t.nodeLocation)
      case _ =>
        unexpected(t)

//  /**
//   * importStatement := 'import' importExr
//   * @return
//   */
//  def parseImportStatement(): LogicalPlan =
//    val t = scanner.nextToken()
//    val loc = nodeLocation()
//    val packageName = qualifiedId()
//    ImportDef(packageName, unit.sourceFile, loc)
//
//  /**
//   * importExpr :=
//   * @return
//   */
//  def importExpr(): Expression =
//    val t = scanner.lookAhead()
//    t.token match
//      case FlowToken.IDENTIFIER =>
//        val id = identifier()
//        dotRef(id)
//      case _ => ???

  /**
    * qualifiedId := identifier ('.' identifier)*
    */
  def qualifiedId(): Name = dotRef(identifier())

  /**
    * dotRef := ('.' identifier)*
    * @param expr
    * @return
    */
  def dotRef(expr: Name): Name =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.DOT =>
        scanner.nextToken()
        val id = identifier()
        dotRef(Ref(expr, id, t.nodeLocation))
      case _ =>
        expr
