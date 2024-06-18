package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.parser.FlowToken.{EQ, FOR, FROM, R_PAREN}
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
  *   reserved   : 'from' | 'select' | 'where' | 'group' | 'by' | 'having' | 'join'
  *              | 'order' | 'limit' | 'as' | 'model' | 'type' | 'def' | 'end'
  *
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
  *   importStatement: 'import' importRef (from str)?
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
  *
  *   relation       : relationPrimary ('as' identifier)?
  *   relationPrimary: qualifiedId
  *                  | '(' relation ')'
  *                  | str
  *
  *   queryBlock: join
  *             | 'group' 'by' groupByItemList
  *             | 'where' booleanExpression
  *             | 'transform' transformExpr
  *             | 'select' selectExpr
  *             | 'limit' INTEGER_VALUE
  *
  *   join        : joinType? 'join' relation joinCriteria
  *               | 'cross' 'join' relation
  *   joinType    : 'inner' | 'left' | 'right' | 'full'
  *   joinCriteria: 'on' booleanExpression
  *               // using equi join keys
  *               | 'on' identifier (',' identifier)*
  *
  *   groupByItemList: groupByItem (',' groupByItem)* ','?
  *   groupByItem    : (identifier ':')? expression
  *
  *   transformExpr: transformItem (',' transformItem)* ','?
  *   transformItem: qualifiedId '=' expression
  *
  *   selectExpr: selectItem (',' selectItem)* ','?
  *   selectItem: (identifier ':')? expression
  *
  *   typeDef    : 'type' identifier typeParams? context? typeExtends? ':' typeElem* 'end'
  *   typeParams : '[' typeParam (',' typeParam)* ']'
  *   typeParam  : identifier (':' identifier)?
  *   typeExtends: 'extends' qualifiedId (',' qualifiedId)*
  *   typeElem   : valDef | funDef
  *
  *   valDef     : identifier ':' identifier ('=' expression)?
  *   funDef:    : 'def' funName defParams? (':' identifier)? ('=' expression)?
  *   funName    : identifier | symbol
  *   symbol     : '+' | '-' | '*' | '/' | '%' | '&' | '|' | '=' | '==' | '!=' | '<' | '<=' | '>' | '>=' | '&&' | '||'
  *   defParams  : '(' defParam (',' defParam)* ')'
  *   defParam   : identifier ':' identifier ('=' expression)?
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
  *                     | '(' query ')'                                                 # subquery
  *                     | '(' expression ')'                                            # parenthesized expression
  *                     | '[' expression (',' expression)* ']'                          # array
  *                     | qualifiedId
  *                     | primaryExpression '.' primaryExpression
  *                     | primaryExpression '(' functionArg? (',' functionArg)* ')'     # function call
  *                     | primaryExpression identifier expression                       # function infix
  *
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
      case FlowToken.UNDERSCORE =>
        consume(FlowToken.UNDERSCORE)
        ContextRef(t.nodeLocation)
      case _ =>
        reserved()

  def reserved(): Name =
    val t = scanner.nextToken()
    t.token match
      case FlowToken.FROM | FlowToken.SELECT | FlowToken.WHERE | FlowToken.GROUP | FlowToken.BY | FlowToken.HAVING |
          FlowToken.JOIN | FlowToken.ORDER | FlowToken.LIMIT | FlowToken.AS | FlowToken.MODEL | FlowToken.TYPE |
          FlowToken.DEF | FlowToken.END =>
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

  def statement(): LogicalPlan =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.IMPORT =>
        importStatement()
      case FlowToken.FROM =>
        query()
      case FlowToken.SELECT =>
        Query(select(), t.nodeLocation)
      case FlowToken.TYPE =>
        typeDef()
      case FlowToken.MODEL =>
        modelDef()
      case _ =>
        unexpected(t)

  def modelDef(): ModelDef =
    val t    = consume(FlowToken.MODEL)
    val name = identifier()
    val params = scanner.lookAhead().token match
      case FlowToken.L_PAREN =>
        consume(FlowToken.L_PAREN)
        val args = defArgs()
        consume(FlowToken.R_PAREN)
        args
      case _ => Nil
    val tpe: Option[Name] = scanner.lookAhead().token match
      case FlowToken.COLON =>
        // model type
        consume(FlowToken.COLON)
        Some(identifier())
      case _ =>
        None
    consume(FlowToken.EQ)
    val q = query()
    consume(FlowToken.END)
    ModelDef(name, params, tpe, q, t.nodeLocation)

  def importStatement(): ImportDef =
    val i            = consume(FlowToken.IMPORT)
    val d: ImportDef = importRef()
    scanner.lookAhead().token match
      case FlowToken.FROM =>
        consume(FlowToken.FROM)
        val fromSource = consume(FlowToken.STRING_LITERAL)
        d.copy(fromSource = Some(StringLiteral(fromSource.str, fromSource.nodeLocation)))
      case _ =>
        d

  def importRef(): ImportDef =
    val qid: Name = qualifiedId()
    val t         = scanner.lookAhead()
    t.token match
      case FlowToken.DOT =>
        consume(FlowToken.DOT)
        val w = consume(FlowToken.STAR)
        ImportDef(Ref(qid, Wildcard(w.nodeLocation), qid.nodeLocation), None, None, qid.nodeLocation)
      case FlowToken.AS =>
        // alias
        consume(FlowToken.AS)
        val alias = identifier()
        ImportDef(qid, Some(alias), None, qid.nodeLocation)
      case _ =>
        ImportDef(qid, None, None, qid.nodeLocation)

  def typeDef(): TypeDef =
    val t       = consume(FlowToken.TYPE)
    val name    = identifier()
    val scopes  = context()
    val parents = typeExtends()
    consume(FlowToken.COLON)
    val elems = typeElems()
    consume(FlowToken.END)
    if parents.size > 1 then
      throw StatusCode.SYNTAX_ERROR.newException(
        s"extending multiple types is not supported: ${name.fullName} extends ${parents.map(_.fullName).mkString(", ")}",
        t.sourceLocation
      )
    TypeDef(name, scopes, parents.headOption, elems, t.nodeLocation)

  def typeExtends(): List[Name] =
    scanner.lookAhead().token match
      case FlowToken.EXTENDS =>
        consume(FlowToken.EXTENDS)
        val parents = List.newBuilder[Name]
        def nextParent: Unit =
          val t = scanner.lookAhead()
          t.token match
            case FlowToken.COMMA =>
              consume(FlowToken.COMMA)
              nextParent
            case FlowToken.COLON =>
            // ok
            case _ =>
              parents += identifier()
              nextParent
        nextParent
        parents.result()
      case _ =>
        Nil

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
    val args: List[DefArg] = scanner.lookAhead().token match
      case FlowToken.L_PAREN =>
        consume(FlowToken.L_PAREN)
        val args = defArgs()
        consume(FlowToken.R_PAREN)
        args
      case _ =>
        Nil

    val defScope: List[DefScope] = context()

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

  def defArgs(): List[DefArg] =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.R_PAREN | FlowToken.COLON | FlowToken.EQ =>
        List.empty
      case FlowToken.COMMA =>
        consume(FlowToken.COMMA)
        defArgs()
      case _ =>
        val e = defArg()
        e :: defArgs()

  def defArg(): DefArg =
    val name = identifier()
    consume(FlowToken.COLON)
    val tpe = identifier()
    val defaultValue = scanner.lookAhead().token match
      case FlowToken.EQ =>
        consume(FlowToken.EQ)
        Some(expression())
      case _ =>
        None
    DefArg(name, tpe, defaultValue, name.nodeLocation)

  def context(): List[DefScope] = scanner.lookAhead().token match
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
              case _ =>
      nextScope
      consume(FlowToken.R_PAREN)
      scopes.result()
    case _ => Nil

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
    var rel = t.token match
      case FlowToken.AS =>
        consume(FlowToken.AS)
        val alias = identifier()
        AliasedRelation(primary, alias, None, t.nodeLocation)
      case _ =>
        primary
    rel = queryBlock(rel)
    rel

  def queryBlock(input: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.LEFT | FlowToken.RIGHT | FlowToken.INNER | FlowToken.FULL | FlowToken.CROSS | FlowToken.JOIN =>
        val joinRel = join(input)
        queryBlock(joinRel)
      case FlowToken.WHERE =>
        consume(FlowToken.WHERE)
        val cond   = booleanExpression()
        val filter = Filter(input, cond, t.nodeLocation)
        queryBlock(filter)
      case FlowToken.TRANSFORM =>
        val transform = transformExpr(input)
        queryBlock(transform)
      case FlowToken.GROUP =>
        val groupBy = groupByExpr(input)
        queryBlock(groupBy)
      case FlowToken.SELECT =>
        val select = selectExpr(input)
        queryBlock(select)
      case _ =>
        input

  def join(input: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.CROSS =>
        consume(FlowToken.CROSS)
        consume(FlowToken.JOIN)
        val right = relationPrimary()
        Join(JoinType.CrossJoin, input, right, NoJoinCriteria, t.nodeLocation)
      case FlowToken.JOIN =>
        consume(FlowToken.JOIN)
        val right  = relationPrimary()
        val joinOn = joinCriteria()
        Join(JoinType.InnerJoin, input, right, joinOn, t.nodeLocation)
      case FlowToken.LEFT | FlowToken.RIGHT | FlowToken.INNER | FlowToken.FULL =>
        val joinType = t.token match
          case FlowToken.LEFT  => JoinType.LeftOuterJoin
          case FlowToken.RIGHT => JoinType.RightOuterJoin
          case FlowToken.INNER => JoinType.InnerJoin
          case FlowToken.FULL  => JoinType.FullOuterJoin
          case _               => unexpected(t)
        consume(t.token)
        consume(FlowToken.JOIN)
        val right  = relationPrimary()
        val joinOn = joinCriteria()
        Join(joinType, input, right, joinOn, t.nodeLocation)

  def joinCriteria(): JoinCriteria =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.ON =>
        consume(FlowToken.ON)
        scanner.lookAhead().token match
          case FlowToken.IDENTIFIER =>
            val joinKeys = List.newBuilder[Name]
            def nextKey: Unit =
              val key = identifier()
              joinKeys += key
              scanner.lookAhead().token match
                case FlowToken.COMMA =>
                  consume(FlowToken.COMMA)
                  nextKey
                case _ =>
            nextKey
            JoinUsing(joinKeys.result(), t.nodeLocation)
          case _ =>
            val cond = booleanExpression()
            JoinOn(cond, t.nodeLocation)
      case _ =>
        NoJoinCriteria

  def transformExpr(input: Relation): Transform =
    val t     = consume(FlowToken.TRANSFORM)
    val items = List.newBuilder[SingleColumn]
    def nextItem: Unit =
      val t = scanner.lookAhead()
      t.token match
        case FlowToken.COMMA =>
          consume(FlowToken.COMMA)
          nextItem
        case t if t.tokenType == TokenType.Keyword =>
        // finish
        case _ =>
          val name = identifier()
          consume(FlowToken.COLON)
          val expr = expression()
          items += SingleColumn(name, expr, t.nodeLocation)
          nextItem
    nextItem
    Transform(input, items.result, t.nodeLocation)

  def groupByExpr(input: Relation): Aggregate =
    val t = consume(FlowToken.GROUP)
    consume(FlowToken.BY)
    val items = groupByItemList()
    Aggregate(input, items, t.nodeLocation)

  def groupByItemList(): List[GroupingKey] =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.IDENTIFIER =>
        val keyName = identifier()
        val key = if scanner.lookAhead().token == FlowToken.COLON then
          // (identifier ':')? expression
          consume(FlowToken.COLON)
          val aggr = expression()
          UnresolvedGroupingKey(keyName, aggr, t.nodeLocation)
        else UnresolvedGroupingKey(keyName, keyName, t.nodeLocation)
        key :: groupByItemList()
      case FlowToken.COMMA =>
        consume(FlowToken.COMMA)
        groupByItemList()
      case t if t.tokenType == TokenType.Keyword =>
        Nil
      case _ =>
        // expression only
        val e   = expression()
        val key = UnresolvedGroupingKey(NoName, e, e.nodeLocation)
        key :: groupByItemList()

  def selectExpr(input: Relation): Project =
    val t     = consume(FlowToken.SELECT)
    val items = selectItems()
    Project(input, items, t.nodeLocation)

  def selectItems(): List[Attribute] =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.COMMA =>
        consume(FlowToken.COMMA)
        selectItems()
      case FlowToken.EOF | FlowToken.END =>
        Nil
      case t if t.tokenType == TokenType.Keyword =>
        Nil
      case FlowToken.IDENTIFIER =>
        val exprOrColumName = expression()
        if scanner.lookAhead().token == FlowToken.COLON then
          consume(FlowToken.COLON)
          val expr = expression()
          exprOrColumName match
            case columnName: Name =>
              SingleColumn(columnName, expr, t.nodeLocation) :: selectItems()
            case other =>
              unexpected(t)
        else SingleColumn(NoName, exprOrColumName, t.nodeLocation) :: selectItems()
      case _ =>
        val e          = expression()
        val selectItem = SingleColumn(NoName, e, t.nodeLocation)
        selectItem :: selectItems()

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
    SingleColumn(NoName, expression(), t.nodeLocation)

  def expression(): Expression = booleanExpression()

  def booleanExpression(): Expression =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.EXCLAMATION | FlowToken.NOT =>
        consume(t.token)
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
      case FlowToken.EQ =>
        consume(FlowToken.EQ)
        scanner.lookAhead().token match
          case FlowToken.EQ =>
            consume(FlowToken.EQ)
          case _ =>
        val right = valueExpression()
        Eq(expression, right, t.nodeLocation)
      case FlowToken.NEQ =>
        consume(FlowToken.NEQ)
        val right = valueExpression()
        NotEq(expression, right, t.nodeLocation)
      case FlowToken.IS =>
        consume(FlowToken.IS)
        scanner.lookAhead().token match
          case FlowToken.NOT =>
            consume(FlowToken.NOT)
            val right = valueExpression()
            NotEq(expression, right, t.nodeLocation)
          case _ =>
            val right = valueExpression()
            Eq(expression, right, t.nodeLocation)
      case FlowToken.LT =>
        consume(FlowToken.LT)
        val right = valueExpression()
        LessThan(expression, right, t.nodeLocation)
      case FlowToken.GT =>
        consume(FlowToken.GT)
        val right = valueExpression()
        GreaterThan(expression, right, t.nodeLocation)
      case FlowToken.LTEQ =>
        consume(FlowToken.LTEQ)
        val right = valueExpression()
        LessThanOrEq(expression, right, t.nodeLocation)
      case FlowToken.GTEQ =>
        consume(FlowToken.GTEQ)
        val right = valueExpression()
        GreaterThanOrEq(expression, right, t.nodeLocation)
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
      case FlowToken.L_BRACKET =>
        array()
      case FlowToken.IDENTIFIER | FlowToken.STAR | FlowToken.END =>
        identifier()
      case _ =>
        unexpected(t)
    primaryExpressionRest(expr)

  def array(): ArrayConstructor =
    val t        = consume(FlowToken.L_BRACKET)
    val elements = List.newBuilder[Expression]
    def nextElement: Unit =
      val t = scanner.lookAhead()
      t.token match
        case FlowToken.COMMA =>
          consume(FlowToken.COMMA)
          nextElement
        case FlowToken.R_BRACKET =>
        // ok
        case _ =>
          elements += expression()
          nextElement
    nextElement
    consume(FlowToken.R_BRACKET)
    ArrayConstructor(elements.result(), t.nodeLocation)

  def literal(): Literal =
    val t = scanner.nextToken()
    t.token match
      case FlowToken.NULL =>
        NullLiteral(t.nodeLocation)
      case FlowToken.INTEGER_LITERAL =>
        LongLiteral(t.str.toLong, t.nodeLocation)
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
        consume(FlowToken.DOT)
        val next = identifier()
        scanner.lookAhead().token match
          case FlowToken.L_PAREN =>
            val sel  = Ref(expr, next, next.nodeLocation)
            val p    = consume(FlowToken.L_PAREN)
            val args = functionArgs()
            consume(FlowToken.R_PAREN)
            FunctionApply(sel, args, p.nodeLocation)
          case _ =>
            primaryExpressionRest(Ref(expr, next, t.nodeLocation))
      case FlowToken.L_PAREN =>
        expr match
          case n: Name =>
            consume(FlowToken.L_PAREN)
            val args = functionArgs()
            consume(FlowToken.R_PAREN)
            // Global function call
            FunctionApply(n, args, t.nodeLocation)
          case _ =>
            unexpected(expr)
      case _ =>
        expr

  def functionArgs(): List[FunctionArg] =
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
    val t         = scanner.lookAhead()
    val nameOrArg = expression()
    scanner.lookAhead().token match
      case FlowToken.EQ =>
        consume(FlowToken.EQ)
        val expr = expression()
        nameOrArg match
          case n: Name =>
            FunctionArg(Some(n), expr, t.nodeLocation)
          case _ =>
            unexpected(t)
      case _ =>
        FunctionArg(None, nameOrArg, t.nodeLocation)

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
    val token = scanner.lookAhead()
    token.token match
      case FlowToken.DOT =>
        scanner.nextToken()
        val id = identifier()
        dotRef(Ref(expr, id, token.nodeLocation))
      case _ =>
        expr
