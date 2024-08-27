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

import wvlet.lang.StatusCode
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.parser.WvletToken.*
import wvlet.lang.compiler.{CompilationUnit, Name, SourceFile}
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.expr.NameExpr.EmptyName
import wvlet.lang.model.plan.*
import wvlet.log.LogSupport

/**
  * {{{
  *   [Wvlet Language Grammar]
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
  *   reserved   : 'from' | 'select' | 'agg' | 'where' | 'group' | 'by' | 'having' | 'join'
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
  *             | 'select' selectItems
  *             | 'agg' selectItems
  *             | 'pivot' 'on' pivotItem (',' pivotItem)*
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
  *   selectItems: selectItem (',' selectItem)* ','?
  *   selectItem : (identifier '=')? expression
  *              | expression ('as' identifier)?
  *
  *   test: 'test' COLON testExpr*
  *   testExpr: booleanExpression
  *
  *   showCommand: 'show' identifier
  *
  *   sortItem: expression ('asc' | 'desc')?
  *
  *   pivotKey: identifier ('in' '(' (valueExpression (',' valueExpression)*) ')')?
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
class WvletParser(unit: CompilationUnit) extends LogSupport:

  given src: SourceFile                  = unit.sourceFile
  given compilationUnit: CompilationUnit = unit

  private val scanner = WvletScanner(unit.sourceFile, ScannerConfig(skipComments = true))

  def parse(): LogicalPlan =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.PACKAGE =>
        packageDef()
      case _ =>
        val stmts = statements()
        PackageDef(EmptyName, stmts, unit.sourceFile, t.nodeLocation)

  // private def sourceLocation: SourceLocation = SourceLocation(unit.sourceFile, nodeLocation())

  def consume(expected: WvletToken): TokenData =
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
      case WvletToken.IDENTIFIER =>
        consume(WvletToken.IDENTIFIER)
        UnquotedIdentifier(t.str, t.nodeLocation)
      case WvletToken.UNDERSCORE =>
        consume(WvletToken.UNDERSCORE)
        ContextInputRef(DataType.UnknownType, t.nodeLocation)
      case WvletToken.STAR =>
        consume(WvletToken.STAR)
        Wildcard(t.nodeLocation)
      case WvletToken.INTEGER_LITERAL =>
        consume(WvletToken.INTEGER_LITERAL)
        DigitIdentifier(t.str, t.nodeLocation)
      case _ =>
        reserved()

  def identifierSingle(): Identifier =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.IDENTIFIER =>
        consume(WvletToken.IDENTIFIER)
        UnquotedIdentifier(t.str, t.nodeLocation)
      case _ =>
        reserved()

  def reserved(): Identifier =
    val t = scanner.nextToken()
    t.token match
      case WvletToken.FROM | WvletToken.SELECT | WvletToken.AGG | WvletToken.WHERE | WvletToken
            .GROUP | WvletToken.BY | WvletToken.HAVING | WvletToken.JOIN | WvletToken.ORDER |
          WvletToken.LIMIT | WvletToken.AS | WvletToken.MODEL | WvletToken.TYPE | WvletToken.DEF |
          WvletToken.END | WvletToken.IN | WvletToken.LIKE =>
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
        case WvletToken.PACKAGE =>
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
      case WvletToken.EOF =>
        List.empty
      case _ =>
        val stmt: LogicalPlan = statement()
        stmt :: statements()

  def statement(): LogicalPlan =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.IMPORT =>
        importStatement()
      case WvletToken.FROM =>
        query()
      case WvletToken.SELECT =>
        Query(select(), t.nodeLocation)
      case WvletToken.TYPE =>
        typeDef()
      case WvletToken.MODEL =>
        modelDef()
      case WvletToken.DEF =>
        val d = funDef()
        TopLevelFunctionDef(d, t.nodeLocation)
      case WvletToken.SHOW =>
        Query(queryBlock(show()), t.nodeLocation)
      case _ =>
        unexpected(t)

  def show(): Show =
    val t    = consume(WvletToken.SHOW)
    val name = identifier()
    try
      Show(ShowType.valueOf(name.leafName), t.nodeLocation)
    catch
      case e: IllegalArgumentException =>
        throw StatusCode
          .SYNTAX_ERROR
          .newException(s"Unknown argument for show: ${name.leafName}", t.sourceLocation)

  def modelDef(): ModelDef =
    val t    = consume(WvletToken.MODEL)
    val name = identifierSingle()
    val params =
      scanner.lookAhead().token match
        case WvletToken.L_PAREN =>
          consume(WvletToken.L_PAREN)
          val args = defArgs()
          consume(WvletToken.R_PAREN)
          args
        case _ =>
          Nil
    val tpe: Option[NameExpr] =
      scanner.lookAhead().token match
        case WvletToken.COLON =>
          // model type
          consume(WvletToken.COLON)
          Some(identifier())
        case _ =>
          None
    consume(WvletToken.EQ)
    val q = query()
    consume(WvletToken.END)
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
    val i         = consume(WvletToken.IMPORT)
    val d: Import = importRef()
    scanner.lookAhead().token match
      case WvletToken.FROM =>
        consume(WvletToken.FROM)
        val fromSource = consume(WvletToken.STRING_LITERAL)
        d.copy(fromSource = Some(StringLiteral(fromSource.str, fromSource.nodeLocation)))
      case _ =>
        d

  def importRef(): Import =
    val qid: NameExpr = qualifiedId()
    val t             = scanner.lookAhead()
    t.token match
      case WvletToken.DOT =>
        consume(WvletToken.DOT)
        val w = consume(WvletToken.STAR)
        Import(
          DotRef(qid, Wildcard(w.nodeLocation), DataType.UnknownType, qid.nodeLocation),
          None,
          None,
          qid.nodeLocation
        )
      case WvletToken.AS =>
        // alias
        consume(WvletToken.AS)
        val alias = identifier()
        Import(qid, Some(alias), None, qid.nodeLocation)
      case _ =>
        Import(qid, None, None, qid.nodeLocation)

  def typeDef(): TypeDef =
    val t       = consume(WvletToken.TYPE)
    val name    = Name.typeName(identifier().leafName)
    val tp      = typeParams()
    val scopes  = context()
    val parents = typeExtends()
    consume(WvletToken.COLON)
    val elems = typeElems()
    consume(WvletToken.END)
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
      case WvletToken.L_BRACKET =>
        consume(WvletToken.L_BRACKET)
        val params = List.newBuilder[TypeParameter]
        def nextParam: Unit =
          val t = scanner.lookAhead()
          t.token match
            case WvletToken.COMMA =>
              consume(WvletToken.COMMA)
              nextParam
            case WvletToken.R_BRACKET =>
            // ok
            case WvletToken.INTEGER_LITERAL =>
              // e.g., decimal[15, 2]
              val i = consume(WvletToken.INTEGER_LITERAL)
              params += IntConstant(i.str.toInt)
              nextParam
            case _ =>
              val name = identifier()
              val tpe =
                scanner.lookAhead().token match
                  case WvletToken.OF =>
                    consume(WvletToken.OF)
                    Some(identifier())
                  case _ =>
                    None
              params += UnresolvedTypeParameter(name.fullName, tpe)
              nextParam
        nextParam
        consume(WvletToken.R_BRACKET)
        params.result()
      case _ =>
        Nil

  def typeExtends(): List[NameExpr] =
    scanner.lookAhead().token match
      case WvletToken.EXTENDS =>
        consume(WvletToken.EXTENDS)
        val parents = List.newBuilder[NameExpr]
        def nextParent: Unit =
          val t = scanner.lookAhead()
          t.token match
            case WvletToken.COMMA =>
              consume(WvletToken.COMMA)
              nextParent
            case WvletToken.COLON =>
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
      case WvletToken.EOF | WvletToken.END =>
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
      case WvletToken.DEF =>
        funDef()
      case WvletToken.IDENTIFIER =>
        val name = identifier()
        consume(WvletToken.COLON)
        val valType = identifier()
        val tp      = typeParams()
        val defaultValue =
          scanner.lookAhead().token match
            case WvletToken.EQ =>
              consume(WvletToken.EQ)
              Some(expression())
            case _ =>
              None
        FieldDef(Name.termName(name.leafName), valType, tp, defaultValue, t.nodeLocation)
      case _ =>
        unexpected(t)

  def funDef(): FunctionDef =
    val t    = consume(WvletToken.DEF)
    val name = funName()
    val args: List[DefArg] =
      scanner.lookAhead().token match
        case WvletToken.L_PAREN =>
          consume(WvletToken.L_PAREN)
          val args = defArgs()
          consume(WvletToken.R_PAREN)
          args
        case _ =>
          Nil

    val defScope: List[DefContext] = context()

    val retType: Option[DataType] =
      scanner.lookAhead().token match
        case WvletToken.COLON =>
          consume(WvletToken.COLON)
          val id = identifier()
          val tp = typeParams()
          Some(DataType.parse(id.fullName, tp))
        case _ =>
          None

    val body: Option[Expression] =
      scanner.lookAhead().token match
        case WvletToken.EQ =>
          consume(WvletToken.EQ)
          Some(expression())
        case _ =>
          None
    FunctionDef(Name.termName(name.leafName), args, defScope, retType, body, t.nodeLocation)

  end funDef

  def funName(): NameExpr =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.IDENTIFIER =>
        identifier()
      case WvletToken.PLUS | WvletToken.MINUS | WvletToken.STAR | WvletToken.DIV | WvletToken.MOD |
          WvletToken.AMP | WvletToken.PIPE | WvletToken.EQ | WvletToken.NEQ | WvletToken.LT |
          WvletToken.LTEQ | WvletToken.GT | WvletToken.GTEQ =>
        // symbols
        consume(t.token)
        UnquotedIdentifier(t.str, t.nodeLocation)
      case _ =>
        reserved()

  def defArgs(): List[DefArg] =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.R_PAREN | WvletToken.COLON | WvletToken.EQ =>
        List.empty
      case WvletToken.COMMA =>
        consume(WvletToken.COMMA)
        defArgs()
      case _ =>
        val e = defArg()
        e :: defArgs()

  def defArg(): DefArg =
    val name = identifier()
    consume(WvletToken.COLON)
    val tpe = identifier()
    val isVarArg =
      scanner.lookAhead().token match
        case WvletToken.STAR =>
          consume(WvletToken.STAR)
          true
        case _ =>
          false

    val defaultValue =
      scanner.lookAhead().token match
        case WvletToken.EQ =>
          consume(WvletToken.EQ)
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
      case WvletToken.IN =>
        consume(WvletToken.IN)
        val scopes = List.newBuilder[DefContext]
        def nextScope: Unit =
          val t = scanner.lookAhead()
          t.token match
            case WvletToken.COMMA =>
              consume(WvletToken.COMMA)
              nextScope
            case WvletToken.COLON | WvletToken.EQ | WvletToken.EXTENDS =>
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
    val t = consume(WvletToken.ORDER)
    consume(WvletToken.BY)
    val items = sortItems()
    Sort(input, items, t.nodeLocation)

  def sortItems(): List[SortItem] =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.IDENTIFIER =>
        val expr = expression()
        val order =
          scanner.lookAhead().token match
            case WvletToken.ASC =>
              consume(WvletToken.ASC)
              Some(SortOrdering.Ascending)
            case WvletToken.DESC =>
              consume(WvletToken.DESC)
              Some(SortOrdering.Descending)
            case _ =>
              None
        // TODO: Support NullOrdering
        SortItem(expr, order, None, expr.nodeLocation) :: sortItems()
      case WvletToken.COMMA =>
        consume(WvletToken.COMMA)
        sortItems()
      case _ =>
        Nil

  /**
    * query := 'from' fromRelation queryBlock*
    */
  def query(): Relation =
    val t = consume(WvletToken.FROM)
    var r = fromRelation()

    def readRest(): Unit =
      scanner.lookAhead().token match
        case WvletToken.COMMA =>
          val ct    = consume(WvletToken.COMMA)
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
        case WvletToken.AS =>
          consume(WvletToken.AS)
          val alias = identifier()
          AliasedRelation(primary, alias, None, t.nodeLocation)
        case WvletToken.L_PAREN =>
          consume(WvletToken.L_PAREN)
          val relationArgs = functionArgs()
          consume(WvletToken.R_PAREN)
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
      case WvletToken.LEFT | WvletToken.RIGHT | WvletToken.INNER | WvletToken.FULL | WvletToken
            .CROSS | WvletToken.JOIN =>
        val joinRel = join(input)
        queryBlock(joinRel)
      case WvletToken.WHERE =>
        consume(WvletToken.WHERE)
        val cond   = booleanExpression()
        val filter = Filter(input, cond, t.nodeLocation)
        queryBlock(filter)
      case WvletToken.TRANSFORM =>
        val transform = transformExpr(input)
        queryBlock(transform)
      case WvletToken.GROUP =>
        val groupBy = groupByExpr(input)
        queryBlock(groupBy)
      case WvletToken.AGG =>
        val agg = aggExpr(input)
        queryBlock(agg)
      case WvletToken.PIVOT =>
        val pivot = pivotExpr(input)
        queryBlock(pivot)
      case WvletToken.SELECT =>
        val select = selectExpr(input)
        queryBlock(select)
      case WvletToken.LIMIT =>
        val limit = limitExpr(input)
        queryBlock(limit)
      case WvletToken.ORDER =>
        val order = orderExpr(input)
        queryBlock(order)
      case WvletToken.TEST =>
        val test = testExpr(input)
        queryBlock(test)
      case _ =>
        input

    end match

  end queryBlock

  def join(input: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.CROSS =>
        consume(WvletToken.CROSS)
        consume(WvletToken.JOIN)
        val right = relationPrimary()
        Join(JoinType.CrossJoin, input, right, NoJoinCriteria, t.nodeLocation)
      case WvletToken.JOIN =>
        consume(WvletToken.JOIN)
        val right  = relationPrimary()
        val joinOn = joinCriteria()
        Join(JoinType.InnerJoin, input, right, joinOn, t.nodeLocation)
      case WvletToken.LEFT | WvletToken.RIGHT | WvletToken.INNER | WvletToken.FULL =>
        val joinType =
          t.token match
            case WvletToken.LEFT =>
              JoinType.LeftOuterJoin
            case WvletToken.RIGHT =>
              JoinType.RightOuterJoin
            case WvletToken.INNER =>
              JoinType.InnerJoin
            case WvletToken.FULL =>
              JoinType.FullOuterJoin
            case _ =>
              unexpected(t)
        consume(t.token)
        consume(WvletToken.JOIN)
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
      case WvletToken.ON =>
        consume(WvletToken.ON)
        val cond = booleanExpression()
        cond match
          case i: Identifier =>
            val joinKeys = List.newBuilder[NameExpr]
            joinKeys += i
            def nextKey: Unit =
              val la = scanner.lookAhead()
              la.token match
                case WvletToken.COMMA =>
                  consume(WvletToken.COMMA)
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
    val t     = consume(WvletToken.TRANSFORM)
    val items = List.newBuilder[SingleColumn]
    def nextItem: Unit =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          nextItem
        case t if t.tokenType == TokenType.Keyword =>
        // finish
        case _ =>
          items += selectItem()
          nextItem
    nextItem
    Transform(input, items.result, t.nodeLocation)

  def groupByExpr(input: Relation): GroupBy =
    val t = consume(WvletToken.GROUP)
    consume(WvletToken.BY)
    val items = groupByItemList()
    GroupBy(input, items, t.nodeLocation)

  def aggExpr(input: Relation): Agg =
    def findGroupingKeys(r: Relation): List[GroupingKey] =
      r match
        case g: GroupBy =>
          g.groupingKeys
        case f: FilteringRelation =>
          findGroupingKeys(f.child)
        case _ =>
          Nil
    end findGroupingKeys

    val t = consume(WvletToken.AGG)
    val groupingKeys = findGroupingKeys(input)
      .zipWithIndex
      .map { case (g, i) =>
        val key = UnquotedIdentifier(s"_${i + 1}", g.nodeLocation)
        SingleColumn(key, key, g.nodeLocation)
      }
    // report _1, _2, agg_expr...
    val items = groupingKeys ++ selectItems()
    Agg(input, items, t.nodeLocation)

  def pivotExpr(input: Relation): Pivot =
    def pivotValues: List[Expression] =
      val values = List.newBuilder[Expression]
      def nextValue: Unit =
        val t = scanner.lookAhead()
        t.token match
          case WvletToken.COMMA =>
            consume(WvletToken.COMMA)
            nextValue
          case WvletToken.R_PAREN =>
          // ok
          case _ =>
            val e = expression()
            values += e
            nextValue
      end nextValue

      nextValue
      values.result()
    end pivotValues

    def pivotKeys: List[PivotKey] =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.IDENTIFIER =>
          val pivotKey = identifierSingle()
          scanner.lookAhead().token match
            case WvletToken.IN =>
              consume(WvletToken.IN)
              consume(WvletToken.L_PAREN)
              val values = pivotValues
              consume(WvletToken.R_PAREN)
              PivotKey(pivotKey, values, t.nodeLocation) :: pivotKeys
            case _ =>
              PivotKey(pivotKey, Nil, t.nodeLocation) :: pivotKeys
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          pivotKeys
        case _ =>
          Nil
    end pivotKeys

    val t = consume(WvletToken.PIVOT)
    consume(WvletToken.ON)
    val keys = pivotKeys
    Pivot(input, keys, t.nodeLocation)

  end pivotExpr

  def groupByItemList(): List[GroupingKey] =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.IDENTIFIER =>
        val item = selectItem()
        val key  = UnresolvedGroupingKey(item.nameExpr, item.expr, t.nodeLocation)
        key :: groupByItemList()
      case WvletToken.COMMA =>
        consume(WvletToken.COMMA)
        groupByItemList()
      case t if t.tokenType == TokenType.Keyword =>
        Nil
      case WvletToken.EOF =>
        Nil
      case _ =>
        // expression only
        val e   = expression()
        val key = UnresolvedGroupingKey(EmptyName, e, e.nodeLocation)
        key :: groupByItemList()

  def selectExpr(input: Relation): Project =
    val t     = consume(WvletToken.SELECT)
    val items = selectItems()
    Project(input, items, t.nodeLocation)

  def selectItems(): List[Attribute] =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.COMMA =>
        consume(WvletToken.COMMA)
        selectItems()
      case WvletToken.EOF | WvletToken.END =>
        Nil
      case WvletToken.R_PAREN =>
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
        case WvletToken.AS =>
          consume(WvletToken.AS)
          val alias = identifier()
          SingleColumn(alias, item, item.nodeLocation)
        case _ =>
          SingleColumn(EmptyName, item, item.nodeLocation)

    val t = scanner.lookAhead()
    t.token match
      case WvletToken.IDENTIFIER =>
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
    val t = consume(WvletToken.LIMIT)
    val n = consume(WvletToken.INTEGER_LITERAL)
    Limit(input, LongLiteral(n.str.toLong, t.nodeLocation), t.nodeLocation)

  def testExpr(input: Relation): Relation =
    val t = consume(WvletToken.TEST)
    consume(WvletToken.COLON)
    val items = List.newBuilder[Expression]
    def nextItem: Unit =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.END | WvletToken.EOF                          =>
        case t if WvletToken.queryBlockKeywords.contains(t.tokenType) =>
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
      case WvletToken.IDENTIFIER =>
        TableRef(qualifiedId(), t.nodeLocation)
      case WvletToken.L_PAREN =>
        consume(WvletToken.L_PAREN)
        val q = query()
        consume(WvletToken.R_PAREN)
        ParenthesizedRelation(q, t.nodeLocation)
      case WvletToken.STRING_LITERAL =>
        consume(WvletToken.STRING_LITERAL)
        FileScan(t.str, t.nodeLocation)
      case WvletToken.STRING_INTERPOLATION_PREFIX if t.str == "sql" =>
        val rawSQL = interpolatedString()
        RawSQL(rawSQL, t.nodeLocation)
      case _ =>
        unexpected(t)

  def select(): Relation =
    val t     = consume(WvletToken.SELECT)
    val attrs = attributeList()
    Project(EmptyRelation(t.nodeLocation), attrs, t.nodeLocation)

  def attributeList(): List[Attribute] =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.EOF =>
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
      case WvletToken.EXCLAMATION | WvletToken.NOT =>
        consume(t.token)
        val e = booleanExpression()
        Not(e, t.nodeLocation)
      case _ =>
        val expr = valueExpression()
        booleanExpressionRest(expr)

  def booleanExpressionRest(expression: Expression): Expression =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.AND =>
        consume(WvletToken.AND)
        val right = booleanExpression()
        And(expression, right, t.nodeLocation)
      case WvletToken.OR =>
        consume(WvletToken.OR)
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
      case WvletToken.PLUS =>
        consume(WvletToken.PLUS)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Add, expression, right, t.nodeLocation)
      case WvletToken.MINUS =>
        consume(WvletToken.MINUS)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Subtract, expression, right, t.nodeLocation)
      case WvletToken.STAR =>
        consume(WvletToken.STAR)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Multiply, expression, right, t.nodeLocation)
      case WvletToken.DIV =>
        consume(WvletToken.DIV)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Divide, expression, right, t.nodeLocation)
      case WvletToken.MOD =>
        consume(WvletToken.MOD)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Modulus, expression, right, t.nodeLocation)
      case WvletToken.EQ =>
        consume(WvletToken.EQ)
        scanner.lookAhead().token match
          case WvletToken.EQ =>
            consume(WvletToken.EQ)
          case _ =>
        val right = valueExpression()
        Eq(expression, right, t.nodeLocation)
      case WvletToken.NEQ =>
        consume(WvletToken.NEQ)
        val right = valueExpression()
        NotEq(expression, right, t.nodeLocation)
      case WvletToken.IS =>
        consume(WvletToken.IS)
        scanner.lookAhead().token match
          case WvletToken.NOT =>
            consume(WvletToken.NOT)
            val right = valueExpression()
            NotEq(expression, right, t.nodeLocation)
          case _ =>
            val right = valueExpression()
            Eq(expression, right, t.nodeLocation)
      case WvletToken.LT =>
        consume(WvletToken.LT)
        val right = valueExpression()
        LessThan(expression, right, t.nodeLocation)
      case WvletToken.GT =>
        consume(WvletToken.GT)
        val right = valueExpression()
        GreaterThan(expression, right, t.nodeLocation)
      case WvletToken.LTEQ =>
        consume(WvletToken.LTEQ)
        val right = valueExpression()
        LessThanOrEq(expression, right, t.nodeLocation)
      case WvletToken.GTEQ =>
        consume(WvletToken.GTEQ)
        val right = valueExpression()
        GreaterThanOrEq(expression, right, t.nodeLocation)
      case WvletToken.IN =>
        consume(WvletToken.IN)
        val valueList = inExprList()
        In(expression, valueList, t.nodeLocation)
      case WvletToken.LIKE =>
        consume(WvletToken.LIKE)
        val right = valueExpression()
        Like(expression, right, t.nodeLocation)
      case WvletToken.NOT =>
        consume(WvletToken.NOT)
        val t2 = scanner.lookAhead()
        t2.token match
          case WvletToken.LIKE =>
            consume(WvletToken.LIKE)
            val right = valueExpression()
            NotLike(expression, right, t.nodeLocation)
          case WvletToken.IN =>
            consume(WvletToken.IN)
            val valueList = inExprList()
            NotIn(expression, valueList, t.nodeLocation)
          case other =>
            unexpected(t2)
      case WvletToken.SHOULD =>
        consume(WvletToken.SHOULD)
        val not =
          scanner.lookAhead().token match
            case WvletToken.NOT =>
              consume(WvletToken.NOT)
              true
            case _ =>
              false
        val testType =
          scanner.lookAhead().token match
            case WvletToken.BE =>
              consume(WvletToken.BE)
              if not then
                TestType.ShouldNotBe
              else
                TestType.ShouldBe
            case WvletToken.CONTAIN =>
              consume(WvletToken.CONTAIN)
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
        case WvletToken.THIS =>
          consume(WvletToken.THIS)
          This(DataType.UnknownType, t.nodeLocation)
        case WvletToken.UNDERSCORE =>
          consume(WvletToken.UNDERSCORE)
          ContextInputRef(DataType.UnknownType, t.nodeLocation)
        case WvletToken.NULL | WvletToken.INTEGER_LITERAL | WvletToken.DOUBLE_LITERAL | WvletToken
              .FLOAT_LITERAL | WvletToken.DECIMAL_LITERAL | WvletToken.EXP_LITERAL | WvletToken
              .STRING_LITERAL =>
          literal()
        case WvletToken.IF =>
          consume(WvletToken.IF)
          val cond = booleanExpression()
          consume(WvletToken.THEN)
          val thenExpr = expression()
          consume(WvletToken.ELSE)
          val elseExpr = expression()
          IfExpr(cond, thenExpr, elseExpr, t.nodeLocation)
        case WvletToken.STRING_INTERPOLATION_PREFIX =>
          interpolatedString()
        case WvletToken.FROM =>
          val q: Relation = query()
          SubQueryExpression(q, t.nodeLocation)
        case WvletToken.L_PAREN =>
          consume(WvletToken.L_PAREN)
          val t2 = scanner.lookAhead()
          t2.token match
            case WvletToken.FROM =>
              val q = query()
              consume(WvletToken.R_PAREN)
              SubQueryExpression(q, t2.nodeLocation)
            case _ =>
              val e = expression()
              consume(WvletToken.R_PAREN)
              ParenthesizedExpression(e, t.nodeLocation)
        case WvletToken.L_BRACKET =>
          array()
        case WvletToken.IDENTIFIER | WvletToken.STAR | WvletToken.END =>
          identifier()
        case _ =>
          unexpected(t)
    primaryExpressionRest(expr)

  end primaryExpression

  def inExprList(): List[Expression] =
    def rest: List[Expression] =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.R_PAREN =>
          consume(WvletToken.R_PAREN)
          Nil
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          rest
        case _ =>
          val e = valueExpression()
          e :: rest

    consume(WvletToken.L_PAREN)
    rest

  def array(): ArrayConstructor =
    val t        = consume(WvletToken.L_BRACKET)
    val elements = List.newBuilder[Expression]
    def nextElement: Unit =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          nextElement
        case WvletToken.R_BRACKET =>
        // ok
        case _ =>
          elements += expression()
          nextElement
    nextElement
    consume(WvletToken.R_BRACKET)
    ArrayConstructor(elements.result(), t.nodeLocation)

  def literal(): Literal =
    val t = scanner.nextToken()
    t.token match
      case WvletToken.NULL =>
        NullLiteral(t.nodeLocation)
      case WvletToken.INTEGER_LITERAL =>
        LongLiteral(t.str.toLong, t.nodeLocation)
      case WvletToken.DOUBLE_LITERAL =>
        DoubleLiteral(t.str.toDouble, t.nodeLocation)
      case WvletToken.FLOAT_LITERAL =>
        DoubleLiteral(t.str.toFloat, t.nodeLocation)
      case WvletToken.DECIMAL_LITERAL =>
        DecimalLiteral(t.str, t.nodeLocation)
      case WvletToken.EXP_LITERAL =>
        DecimalLiteral(t.str, t.nodeLocation)
      case WvletToken.STRING_LITERAL =>
        StringLiteral(t.str, t.nodeLocation)
      case _ =>
        unexpected(t)

  def interpolatedString(): InterpolatedString =
    val prefix     = consume(WvletToken.STRING_INTERPOLATION_PREFIX)
    val prefixNode = ResolvedIdentifier(prefix.str, NoType, prefix.nodeLocation)
    val parts      = List.newBuilder[Expression]

    def nextPart(): Unit =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.STRING_PART =>
          val part = consume(WvletToken.STRING_PART)
          parts += StringPart(part.str, part.nodeLocation)
          nextPart()
        case WvletToken.L_BRACE =>
          consume(WvletToken.L_BRACE)
          val expr = expression()
          consume(WvletToken.R_BRACE)
          parts += expr
          nextPart()
        case _ =>

    while scanner.lookAhead().token == WvletToken.STRING_PART do
      nextPart()
    if scanner.lookAhead().token == WvletToken.STRING_LITERAL then
      val part = consume(WvletToken.STRING_PART)
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
      case WvletToken.DOT =>
        consume(WvletToken.DOT)
        val next = identifier()
        scanner.lookAhead().token match
          case WvletToken.L_PAREN =>
            val sel  = DotRef(expr, next, DataType.UnknownType, next.nodeLocation)
            val p    = consume(WvletToken.L_PAREN)
            val args = functionArgs()
            consume(WvletToken.R_PAREN)
            val f = FunctionApply(sel, args, p.nodeLocation)
            primaryExpressionRest(f)
          case _ =>
            primaryExpressionRest(DotRef(expr, next, DataType.UnknownType, t.nodeLocation))
      case WvletToken.L_PAREN =>
        expr match
          case n: NameExpr =>
            consume(WvletToken.L_PAREN)
            val args = functionArgs()
            consume(WvletToken.R_PAREN)
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
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          nextArg
        case WvletToken.R_PAREN =>
        // ok
        case _ =>
          args += functionArg()
          nextArg

    nextArg
    args.result()

  def functionArg(): FunctionArg =
    val t = scanner.lookAhead()
    scanner.lookAhead().token match
      case WvletToken.IDENTIFIER =>
        val nameOrArg = expression()
        nameOrArg match
          case i: Identifier =>
            scanner.lookAhead().token match
              case WvletToken.EQ =>
                consume(WvletToken.EQ)
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
      case WvletToken.DOT =>
        val dt = consume(WvletToken.DOT)
        scanner.lookAhead().token match
          case WvletToken.STAR =>
            val t = consume(WvletToken.STAR)
            DotRef(expr, Wildcard(t.nodeLocation), DataType.UnknownType, dt.nodeLocation)
          case _ =>
            val id = identifier()
            dotRef(DotRef(expr, id, DataType.UnknownType, token.nodeLocation))
      case _ =>
        expr

end WvletParser
