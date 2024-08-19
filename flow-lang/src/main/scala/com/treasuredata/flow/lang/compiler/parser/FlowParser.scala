package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.catalog.Catalog.TableName
import com.treasuredata.flow.lang.compiler.parser.FlowToken.*
import com.treasuredata.flow.lang.compiler.{CompilationUnit, Name, SourceFile}
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.*
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.expr.NameExpr.EmptyName
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
  *               | '*'
  *               | reserved  # Necessary to use reserved words as identifiers
  *   IDENTIFIER  : (LETTER | '_') (LETTER | DIGIT | '_')*
  *   BACKQUOTED_IDENTIFIER: '`' (~'`' | '``')+ '`'
  *   reserved   : 'from' | 'select' | 'where' | 'group' | 'by' | 'having' | 'join'
  *              | 'order' | 'limit' | 'as' | 'model' | 'type' | 'def' | 'end' | 'in' | 'like'
  *
  *
  *   statements: statement+
  *
  *
  *   statement: importStatement
  *            | modelDef
  *            | query
  *            | typeDef
  *            | funDef
  *            | showCommand queryBlock*
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
  *   query: 'from' relation (',' relation)* ','?
  *          queryBlock*
  *
  *   relation       : relationPrimary ('as' identifier)?
  *   relationPrimary: qualifiedId ('(' functionArg (',' functionArg)* ')')?
  *                  | '(' relation ')'
  *                  | str               // file scan
  *                  | strInterpolation  // embedded raw SQL
  *
  *   queryBlock: join
  *             | 'group' 'by' groupByItemList
  *             | 'where' booleanExpression
  *             | 'transform' transformExpr
  *             | 'select' selectExpr
  *             | 'limit' INTEGER_VALUE
  *             | 'order' 'by' sortItem (',' sortItem)* comma?)?
  *             | 'test' COLON testExpr*
  *             | 'show' identifier
  *
  *   join        : joinType? 'join' relation joinCriteria
  *               | 'cross' 'join' relation
  *   joinType    : 'inner' | 'left' | 'right' | 'full'
  *   joinCriteria: 'on' booleanExpression
  *               // using equi join keys
  *               | 'on' identifier (',' identifier)*
  *
  *   groupByItemList: groupByItem (',' groupByItem)* ','?
  *   groupByItem    : expression ('as' identifier (':' identifier)?)?
  *
  *   transformExpr: transformItem (',' transformItem)* ','?
  *   transformItem: qualifiedId '=' expression
  *
  *   selectExpr: selectItem (',' selectItem)* ','?
  *   selectItem: (identifier '=')? expression
  *             | expression ('as' identifier)?
  *
  *   test: 'test' COLON testExpr*
  *   testExpr: booleanExpression
  *
  *   showCommand: 'show' identifier
  *
  *   sortItem:: expression ('asc' | 'desc')?
  *
  *   typeDef    : 'type' identifier typeParams? context? typeExtends? ':' typeElem* 'end'
  *   typeParams : '[' typeParam (',' typeParam)* ']'
  *   typeParam  : identifier ('of' identifier)?
  *   typeExtends: 'extends' qualifiedId (',' qualifiedId)*
  *   typeElem   : valDef | funDef
  *
  *   valDef     : identifier ':' identifier typeParams? ('=' expression)?
  *   funDef:    : 'def' funName defParams? (':' identifier '*'?)? ('=' expression)?
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
  *                     | valueExpression testOperator valueExpression
  *
  *   arithmeticOperator: '+' | '-' | '*' | '/' | '%'
  *   comparisonOperator: '=' | '==' | 'is' | '!=' | 'is' 'not' | '<' | '<=' | '>' | '>=' | 'like'
  *   testOperator      : 'should' 'not'? ('be' | 'contain')
  *
  *   // Expresion that can be chained with '.' operator
  *   primaryExpression : 'this'
  *                     | '_'
  *                     | literal
  *                     | query
  *                     | '(' query ')'                                                 # subquery
  *                     | '(' expression ')'                                            # parenthesized expression
  *                     | '[' expression (',' expression)* ']'                          # array
  *                     | 'if' booleanExpresssion 'then' expression 'else' expression   # if-then-else
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
      case FlowToken.PACKAGE =>
        packageDef()
      case _ =>
        val stmts = statements()
        PackageDef(EmptyName, stmts, unit.sourceFile, t.nodeLocation)

  // private def sourceLocation: SourceLocation = SourceLocation(unit.sourceFile, nodeLocation())

  def consume(expected: FlowToken): TokenData =
    val t = scanner.nextToken()
    if t.token == expected then
      t
    else
      throw StatusCode
        .SYNTAX_ERROR
        .newException(s"Expected ${expected}, but found ${t.token}", t.sourceLocation)

  private def unexpected(t: TokenData): Nothing =
    throw StatusCode
      .SYNTAX_ERROR
      .newException(s"Unexpected token ${t.token} '${t.str}'", t.sourceLocation)

  private def unexpected(expr: Expression): Nothing =
    throw StatusCode
      .SYNTAX_ERROR
      .newException(s"Unexpected expression: ${expr}", expr.sourceLocation)

  def identifier(): QualifiedName =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.IDENTIFIER =>
        consume(FlowToken.IDENTIFIER)
        UnquotedIdentifier(t.str, t.nodeLocation)
      case FlowToken.UNDERSCORE =>
        consume(FlowToken.UNDERSCORE)
        ContextInputRef(DataType.UnknownType, t.nodeLocation)
      case FlowToken.STAR =>
        consume(FlowToken.STAR)
        Wildcard(t.nodeLocation)
      case FlowToken.INTEGER_LITERAL =>
        consume(FlowToken.INTEGER_LITERAL)
        DigitIdentifier(t.str, t.nodeLocation)
      case _ =>
        reserved()

  def identifierSingle(): Identifier =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.IDENTIFIER =>
        consume(FlowToken.IDENTIFIER)
        UnquotedIdentifier(t.str, t.nodeLocation)
      case _ =>
        reserved()

  def reserved(): Identifier =
    val t = scanner.nextToken()
    t.token match
      case FlowToken.FROM | FlowToken.SELECT | FlowToken.WHERE | FlowToken.GROUP | FlowToken.BY |
          FlowToken.HAVING | FlowToken.JOIN | FlowToken.ORDER | FlowToken.LIMIT | FlowToken.AS |
          FlowToken.MODEL | FlowToken.TYPE | FlowToken.DEF | FlowToken.END | FlowToken.IN |
          FlowToken.LIKE =>
        UnquotedIdentifier(t.str, t.nodeLocation)
      case _ =>
        unexpected(t)

  /**
    * PackageDef := 'package' qualifiedId (statement)*
    */
  def packageDef(): PackageDef =
    val t = scanner.nextToken()
    val packageName: QualifiedName =
      t.token match
        case FlowToken.PACKAGE =>
          val packageName = qualifiedId()
          packageName
        case _ =>
          EmptyName

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
      case FlowToken.DEF =>
        val d = funDef()
        TopLevelFunctionDef(d, t.nodeLocation)
      case FlowToken.SHOW =>
        Query(queryBlock(show()), t.nodeLocation)
      case _ =>
        unexpected(t)

  def show(): Show =
    val t    = consume(FlowToken.SHOW)
    val name = identifier()
    try
      Show(ShowType.valueOf(name.leafName), t.nodeLocation)
    catch
      case e: IllegalArgumentException =>
        throw StatusCode
          .SYNTAX_ERROR
          .newException(s"Unknown argument for show: ${name.leafName}", t.sourceLocation)

  def modelDef(): ModelDef =
    val t    = consume(FlowToken.MODEL)
    val name = identifierSingle()
    val params =
      scanner.lookAhead().token match
        case FlowToken.L_PAREN =>
          consume(FlowToken.L_PAREN)
          val args = defArgs()
          consume(FlowToken.R_PAREN)
          args
        case _ =>
          Nil
    val tpe: Option[NameExpr] =
      scanner.lookAhead().token match
        case FlowToken.COLON =>
          // model type
          consume(FlowToken.COLON)
          Some(identifier())
        case _ =>
          None
    consume(FlowToken.EQ)
    val q = query()
    consume(FlowToken.END)
    ModelDef(
      TableName(name.fullName),
      params,
      // resolve the model type from the query if no type is given
      tpe.map(x => UnresolvedRelationType(x.fullName, Name.typeName(x.leafName))),
      q,
      t.nodeLocation
    )

  end modelDef

  def importStatement(): Import =
    val i         = consume(FlowToken.IMPORT)
    val d: Import = importRef()
    scanner.lookAhead().token match
      case FlowToken.FROM =>
        consume(FlowToken.FROM)
        val fromSource = consume(FlowToken.STRING_LITERAL)
        d.copy(fromSource = Some(StringLiteral(fromSource.str, fromSource.nodeLocation)))
      case _ =>
        d

  def importRef(): Import =
    val qid: NameExpr = qualifiedId()
    val t             = scanner.lookAhead()
    t.token match
      case FlowToken.DOT =>
        consume(FlowToken.DOT)
        val w = consume(FlowToken.STAR)
        Import(
          DotRef(qid, Wildcard(w.nodeLocation), DataType.UnknownType, qid.nodeLocation),
          None,
          None,
          qid.nodeLocation
        )
      case FlowToken.AS =>
        // alias
        consume(FlowToken.AS)
        val alias = identifier()
        Import(qid, Some(alias), None, qid.nodeLocation)
      case _ =>
        Import(qid, None, None, qid.nodeLocation)

  def typeDef(): TypeDef =
    val t       = consume(FlowToken.TYPE)
    val name    = Name.typeName(identifier().leafName)
    val tp      = typeParams()
    val scopes  = context()
    val parents = typeExtends()
    consume(FlowToken.COLON)
    val elems = typeElems()
    consume(FlowToken.END)
    if parents.size > 1 then
      throw StatusCode
        .SYNTAX_ERROR
        .newException(
          s"extending multiple types is not supported: ${name} extends ${parents.map(_.fullName).mkString(", ")}",
          t.sourceLocation
        )
    TypeDef(name, tp, scopes, parents.headOption, elems, t.nodeLocation)

  def typeParams(): List[TypeParameter] =
    scanner.lookAhead().token match
      case FlowToken.L_BRACKET =>
        consume(FlowToken.L_BRACKET)
        val params = List.newBuilder[TypeParameter]
        def nextParam: Unit =
          val t = scanner.lookAhead()
          t.token match
            case FlowToken.COMMA =>
              consume(FlowToken.COMMA)
              nextParam
            case FlowToken.R_BRACKET =>
            // ok
            case FlowToken.INTEGER_LITERAL =>
              // e.g., decimal[15, 2]
              val i = consume(FlowToken.INTEGER_LITERAL)
              params += IntConstant(i.str.toInt)
              nextParam
            case _ =>
              val name = identifier()
              val tpe =
                scanner.lookAhead().token match
                  case FlowToken.OF =>
                    consume(FlowToken.OF)
                    Some(identifier())
                  case _ =>
                    None
              params += UnresolvedTypeParameter(name.fullName, tpe)
              nextParam
        nextParam
        consume(FlowToken.R_BRACKET)
        params.result()
      case _ =>
        Nil

  def typeExtends(): List[NameExpr] =
    scanner.lookAhead().token match
      case FlowToken.EXTENDS =>
        consume(FlowToken.EXTENDS)
        val parents = List.newBuilder[NameExpr]
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
        val tp      = typeParams()
        val defaultValue =
          scanner.lookAhead().token match
            case FlowToken.EQ =>
              consume(FlowToken.EQ)
              Some(expression())
            case _ =>
              None
        FieldDef(Name.termName(name.leafName), valType, tp, defaultValue, t.nodeLocation)
      case _ =>
        unexpected(t)

  def funDef(): FunctionDef =
    val t    = consume(FlowToken.DEF)
    val name = funName()
    val args: List[DefArg] =
      scanner.lookAhead().token match
        case FlowToken.L_PAREN =>
          consume(FlowToken.L_PAREN)
          val args = defArgs()
          consume(FlowToken.R_PAREN)
          args
        case _ =>
          Nil

    val defScope: List[DefContext] = context()

    val retType: Option[DataType] =
      scanner.lookAhead().token match
        case FlowToken.COLON =>
          consume(FlowToken.COLON)
          val id = identifier()
          val tp = typeParams()
          Some(DataType.parse(id.fullName, tp))
        case _ =>
          None

    val body: Option[Expression] =
      scanner.lookAhead().token match
        case FlowToken.EQ =>
          consume(FlowToken.EQ)
          Some(expression())
        case _ =>
          None
    FunctionDef(Name.termName(name.leafName), args, defScope, retType, body, t.nodeLocation)

  end funDef

  def funName(): NameExpr =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.IDENTIFIER =>
        identifier()
      case FlowToken.PLUS | FlowToken.MINUS | FlowToken.STAR | FlowToken.DIV | FlowToken.MOD |
          FlowToken.AMP | FlowToken.PIPE | FlowToken.EQ | FlowToken.NEQ | FlowToken.LT | FlowToken
            .LTEQ | FlowToken.GT | FlowToken.GTEQ =>
        // symbols
        consume(t.token)
        UnquotedIdentifier(t.str, t.nodeLocation)
      case _ =>
        reserved()

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
    val isVarArg =
      scanner.lookAhead().token match
        case FlowToken.STAR =>
          consume(FlowToken.STAR)
          true
        case _ =>
          false

    val defaultValue =
      scanner.lookAhead().token match
        case FlowToken.EQ =>
          consume(FlowToken.EQ)
          Some(expression())
        case _ =>
          None
    var dt =
      if DataType.isPrimitiveTypeName(tpe.fullName) then
        DataType.getPrimitiveType(tpe.fullName)
      else
        UnresolvedType(tpe.fullName)
    dt =
      if isVarArg then
        VarArgType(dt)
      else
        dt
    // TODO check the name is a leaf name
    DefArg(Name.termName(name.leafName), dt, defaultValue, name.nodeLocation)

  end defArg

  def context(): List[DefContext] =
    scanner.lookAhead().token match
      case FlowToken.IN =>
        consume(FlowToken.IN)
        val scopes = List.newBuilder[DefContext]
        def nextScope: Unit =
          val t = scanner.lookAhead()
          t.token match
            case FlowToken.COMMA =>
              consume(FlowToken.COMMA)
              nextScope
            case FlowToken.COLON | FlowToken.EQ | FlowToken.EXTENDS =>
            // ok
            case _ =>
              val nameOrType = identifier()
              scopes += DefContext(None, nameOrType, t.nodeLocation)
              nextScope
        nextScope
        scopes.result()
      case _ =>
        Nil

  def orderExpr(input: Relation): Sort =
    val t = consume(FlowToken.ORDER)
    consume(FlowToken.BY)
    val items = sortItems()
    Sort(input, items, t.nodeLocation)

  def sortItems(): List[SortItem] =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.IDENTIFIER =>
        val expr = expression()
        val order =
          scanner.lookAhead().token match
            case FlowToken.ASC =>
              consume(FlowToken.ASC)
              Some(SortOrdering.Ascending)
            case FlowToken.DESC =>
              consume(FlowToken.DESC)
              Some(SortOrdering.Descending)
            case _ =>
              None
        // TODO: Support NullOrdering
        SortItem(expr, order, None, expr.nodeLocation) :: sortItems()
      case FlowToken.COMMA =>
        consume(FlowToken.COMMA)
        sortItems()
      case _ =>
        Nil

  /**
    * query := 'from' fromRelation queryBlock*
    */
  def query(): Relation =
    val t = consume(FlowToken.FROM)
    var r = fromRelation()

    def readRest(): Unit =
      scanner.lookAhead().token match
        case FlowToken.COMMA =>
          val ct    = consume(FlowToken.COMMA)
          val rNext = fromRelation()
          r = Join(JoinType.ImplicitJoin, r, rNext, NoJoinCriteria, ct.nodeLocation)
          readRest()
        case _ =>

    readRest()

    r = queryBlock(r)
    val q = Query(r, t.nodeLocation)
    q

  /**
    * fromRelation := relationPrimary ('as' identifier)?
    * @return
    */
  def fromRelation(): Relation =
    val primary = relationPrimary()
    val t       = scanner.lookAhead()
    var rel: Relation =
      t.token match
        case FlowToken.AS =>
          consume(FlowToken.AS)
          val alias = identifier()
          AliasedRelation(primary, alias, None, t.nodeLocation)
        case FlowToken.L_PAREN =>
          consume(FlowToken.L_PAREN)
          val relationArgs = functionArgs()
          consume(FlowToken.R_PAREN)
          primary match
            case r: TableRef =>
              TableFunctionCall(r.name, relationArgs, t.nodeLocation)
            case other =>
              unexpected(t)
        case _ =>
          primary
    rel

  def queryBlock(input: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.LEFT | FlowToken.RIGHT | FlowToken.INNER | FlowToken.FULL | FlowToken.CROSS |
          FlowToken.JOIN =>
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
      case FlowToken.LIMIT =>
        val limit = limitExpr(input)
        queryBlock(limit)
      case FlowToken.ORDER =>
        val order = orderExpr(input)
        queryBlock(order)
      case FlowToken.TEST =>
        val test = testExpr(input)
        queryBlock(test)
      case _ =>
        input

    end match

  end queryBlock

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
        val joinType =
          t.token match
            case FlowToken.LEFT =>
              JoinType.LeftOuterJoin
            case FlowToken.RIGHT =>
              JoinType.RightOuterJoin
            case FlowToken.INNER =>
              JoinType.InnerJoin
            case FlowToken.FULL =>
              JoinType.FullOuterJoin
            case _ =>
              unexpected(t)
        consume(t.token)
        consume(FlowToken.JOIN)
        val right  = relationPrimary()
        val joinOn = joinCriteria()
        Join(joinType, input, right, joinOn, t.nodeLocation)
      case _ =>
        unexpected(t)

    end match

  end join

  def joinCriteria(): JoinCriteria =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.ON =>
        consume(FlowToken.ON)
        val cond = booleanExpression()
        cond match
          case i: Identifier =>
            val joinKeys = List.newBuilder[NameExpr]
            joinKeys += i
            def nextKey: Unit =
              val la = scanner.lookAhead()
              la.token match
                case FlowToken.COMMA =>
                  consume(FlowToken.COMMA)
                  val k = identifier()
                  joinKeys += k
                  nextKey
                case other =>
                // stop the search
            nextKey
            JoinUsing(joinKeys.result(), t.nodeLocation)
          case _ =>
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
          items += selectItem()
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
        val item = selectItem()
        val key  = UnresolvedGroupingKey(item.nameExpr, item.expr, t.nodeLocation)
        key :: groupByItemList()
      case FlowToken.COMMA =>
        consume(FlowToken.COMMA)
        groupByItemList()
      case t if t.tokenType == TokenType.Keyword =>
        Nil
      case FlowToken.EOF =>
        Nil
      case _ =>
        // expression only
        val e   = expression()
        val key = UnresolvedGroupingKey(EmptyName, e, e.nodeLocation)
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
      case FlowToken.R_PAREN =>
        // sub-query end
        Nil
      case t if t.tokenType == TokenType.Keyword =>
        Nil
      case _ =>
        selectItem() :: selectItems()
    end match

  end selectItems

  def selectItem(): SingleColumn =
    def selectItemWithAlias(item: Expression): SingleColumn =
      val t = scanner.lookAhead()
      t.token match
        case FlowToken.AS =>
          consume(FlowToken.AS)
          val alias = identifier()
          SingleColumn(alias, item, item.nodeLocation)
        case _ =>
          SingleColumn(EmptyName, item, item.nodeLocation)

    val t = scanner.lookAhead()
    t.token match
      case FlowToken.IDENTIFIER =>
        val exprOrColumName = expression()
        exprOrColumName match
          case Eq(columnName: Identifier, expr: Expression, nodeLocation) =>
            SingleColumn(columnName, expr, t.nodeLocation)
          case i: Identifier =>
            // Propagate the column name for a single column reference
            SingleColumn(i, exprOrColumName, t.nodeLocation)
          case _ =>
            selectItemWithAlias(exprOrColumName)
      case _ =>
        val expr = expression()
        selectItemWithAlias(expr)

  def limitExpr(input: Relation): Limit =
    val t = consume(FlowToken.LIMIT)
    val n = consume(FlowToken.INTEGER_LITERAL)
    Limit(input, LongLiteral(n.str.toLong, t.nodeLocation), t.nodeLocation)

  def testExpr(input: Relation): Relation =
    val t = consume(FlowToken.TEST)
    consume(FlowToken.COLON)
    val items = List.newBuilder[Expression]
    def nextItem: Unit =
      val t = scanner.lookAhead()
      t.token match
        case FlowToken.END | FlowToken.EOF                           =>
        case t if FlowToken.queryBlockKeywords.contains(t.tokenType) =>
        case _ =>
          val e = expression()
          items += e
          nextItem
    nextItem
    TestRelation(input, items.result(), t.nodeLocation)

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
      case FlowToken.STRING_INTERPOLATION_PREFIX if t.str == "sql" =>
        val rawSQL = interpolatedString()
        RawSQL(rawSQL, t.nodeLocation)
      case _ =>
        unexpected(t)

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
    SingleColumn(EmptyName, expression(), t.nodeLocation)

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
      case FlowToken.IN =>
        consume(FlowToken.IN)
        val valueList = inExprList()
        In(expression, valueList, t.nodeLocation)
      case FlowToken.LIKE =>
        consume(FlowToken.LIKE)
        val right = valueExpression()
        Like(expression, right, t.nodeLocation)
      case FlowToken.NOT =>
        consume(FlowToken.NOT)
        val t2 = scanner.lookAhead()
        t2.token match
          case FlowToken.LIKE =>
            consume(FlowToken.LIKE)
            val right = valueExpression()
            NotLike(expression, right, t.nodeLocation)
          case FlowToken.IN =>
            consume(FlowToken.IN)
            val valueList = inExprList()
            NotIn(expression, valueList, t.nodeLocation)
          case other =>
            unexpected(t2)
      case FlowToken.SHOULD =>
        consume(FlowToken.SHOULD)
        val not =
          scanner.lookAhead().token match
            case FlowToken.NOT =>
              consume(FlowToken.NOT)
              true
            case _ =>
              false
        val testType =
          scanner.lookAhead().token match
            case FlowToken.BE =>
              consume(FlowToken.BE)
              if not then
                TestType.ShouldNotBe
              else
                TestType.ShouldBe
            case FlowToken.CONTAIN =>
              consume(FlowToken.CONTAIN)
              if not then
                TestType.ShouldNotContain
              else
                TestType.ShouldContain
            case _ =>
              unexpected(t)
        val right = booleanExpression()
        ShouldExpr(testType, left = expression, right, t.nodeLocation)
      case _ =>
        expression

    end match

  end valueExpressionRest

  def primaryExpression(): Expression =
    val t = scanner.lookAhead()
    val expr: Expression =
      t.token match
        case FlowToken.THIS =>
          consume(FlowToken.THIS)
          This(DataType.UnknownType, t.nodeLocation)
        case FlowToken.UNDERSCORE =>
          consume(FlowToken.UNDERSCORE)
          ContextInputRef(DataType.UnknownType, t.nodeLocation)
        case FlowToken.NULL | FlowToken.INTEGER_LITERAL | FlowToken.DOUBLE_LITERAL |
            FlowToken.FLOAT_LITERAL | FlowToken.DECIMAL_LITERAL | FlowToken.EXP_LITERAL | FlowToken
              .STRING_LITERAL =>
          literal()
        case FlowToken.IF =>
          consume(FlowToken.IF)
          val cond = booleanExpression()
          consume(FlowToken.THEN)
          val thenExpr = expression()
          consume(FlowToken.ELSE)
          val elseExpr = expression()
          IfExpr(cond, thenExpr, elseExpr, t.nodeLocation)
        case FlowToken.STRING_INTERPOLATION_PREFIX =>
          interpolatedString()
        case FlowToken.FROM =>
          val q: Relation = query()
          SubQueryExpression(q, t.nodeLocation)
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

  end primaryExpression

  def inExprList(): List[Expression] =
    def rest: List[Expression] =
      val t = scanner.lookAhead()
      t.token match
        case FlowToken.R_PAREN =>
          consume(FlowToken.R_PAREN)
          Nil
        case FlowToken.COMMA =>
          consume(FlowToken.COMMA)
          rest
        case _ =>
          val e = valueExpression()
          e :: rest

    consume(FlowToken.L_PAREN)
    rest

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
    val prefixNode = ResolvedIdentifier(prefix.str, NoType, prefix.nodeLocation)
    val parts      = List.newBuilder[Expression]

    def nextPart(): Unit =
      val t = scanner.lookAhead()
      t.token match
        case FlowToken.STRING_PART =>
          val part = consume(FlowToken.STRING_PART)
          parts += StringPart(part.str, part.nodeLocation)
          nextPart()
        case FlowToken.L_BRACE =>
          consume(FlowToken.L_BRACE)
          val expr = expression()
          consume(FlowToken.R_BRACE)
          parts += expr
          nextPart()
        case _ =>

    while scanner.lookAhead().token == FlowToken.STRING_PART do
      nextPart()
    if scanner.lookAhead().token == FlowToken.STRING_LITERAL then
      val part = consume(FlowToken.STRING_PART)
      parts += StringPart(part.str, part.nodeLocation)

    InterpolatedString(prefixNode, parts.result(), DataType.UnknownType, prefix.nodeLocation)

  end interpolatedString

  def nameExpression(): NameExpr =
    primaryExpression() match
      case n: NameExpr =>
        n
      case other =>
        unexpected(other)

  def primaryExpressionRest(expr: Expression): Expression =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.DOT =>
        consume(FlowToken.DOT)
        val next = identifier()
        scanner.lookAhead().token match
          case FlowToken.L_PAREN =>
            val sel  = DotRef(expr, next, DataType.UnknownType, next.nodeLocation)
            val p    = consume(FlowToken.L_PAREN)
            val args = functionArgs()
            consume(FlowToken.R_PAREN)
            val f = FunctionApply(sel, args, p.nodeLocation)
            primaryExpressionRest(f)
          case _ =>
            primaryExpressionRest(DotRef(expr, next, DataType.UnknownType, t.nodeLocation))
      case FlowToken.L_PAREN =>
        expr match
          case n: NameExpr =>
            consume(FlowToken.L_PAREN)
            val args = functionArgs()
            consume(FlowToken.R_PAREN)
            // Global function call
            val f = FunctionApply(n, args, t.nodeLocation)
            primaryExpressionRest(f)
          case _ =>
            unexpected(expr)
      case _ =>
        expr

  end primaryExpressionRest

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
    val t = scanner.lookAhead()
    scanner.lookAhead().token match
      case FlowToken.IDENTIFIER =>
        val nameOrArg = expression()
        nameOrArg match
          case i: Identifier =>
            scanner.lookAhead().token match
              case FlowToken.EQ =>
                consume(FlowToken.EQ)
                val expr = expression()
                FunctionArg(Some(Name.termName(i.leafName)), expr, t.nodeLocation)
              case _ =>
                FunctionArg(None, nameOrArg, t.nodeLocation)
          case Eq(i: Identifier, v: Expression, nodeLocation) =>
            FunctionArg(Some(Name.termName(i.leafName)), v, t.nodeLocation)
          case expr: Expression =>
            FunctionArg(None, nameOrArg, t.nodeLocation)
      case _ =>
        val nameOrArg = expression()
        FunctionArg(None, nameOrArg, t.nodeLocation)

  /**
    * qualifiedId := identifier ('.' identifier)*
    */
  def qualifiedId(): QualifiedName = dotRef(identifier())

  /**
    * dotRef := ('.' identifier)*
    * @param expr
    * @return
    */
  def dotRef(expr: QualifiedName): QualifiedName =
    val token = scanner.lookAhead()
    token.token match
      case FlowToken.DOT =>
        val dt = consume(FlowToken.DOT)
        scanner.lookAhead().token match
          case FlowToken.STAR =>
            val t = consume(FlowToken.STAR)
            DotRef(expr, Wildcard(t.nodeLocation), DataType.UnknownType, dt.nodeLocation)
          case _ =>
            val id = identifier()
            dotRef(DotRef(expr, id, DataType.UnknownType, token.nodeLocation))
      case _ =>
        expr

end FlowParser
