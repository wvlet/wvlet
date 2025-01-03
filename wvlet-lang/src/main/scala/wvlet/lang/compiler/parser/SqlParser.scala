package wvlet.lang.compiler.parser

import wvlet.airframe.SourceCode
import wvlet.lang.api.{Span, StatusCode}
import wvlet.lang.compiler.parser.SqlToken.{EOF, ROW, STAR, STRING_LITERAL}
import wvlet.lang.compiler.{CompilationUnit, Name, SourceFile}
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
import wvlet.lang.model.expr.NameExpr.EmptyName
import wvlet.lang.model.plan.*
import wvlet.lang.model.plan.{Lateral, ShowType, TableRef, Unnest, Values}
import wvlet.log.LogSupport

class SqlParser(unit: CompilationUnit, isContextUnit: Boolean = false) extends LogSupport:

  given src: SourceFile                  = unit.sourceFile
  given compilationUnit: CompilationUnit = unit

  private val scanner = SqlScanner(
    unit.sourceFile,
    ScannerConfig(
      skipComments = true,
      // enable debug only for the context unit
      debugScanner = isContextUnit
    )
  )

  private var lastToken: TokenData[SqlToken] = null

  def parse(): LogicalPlan =
    val t     = scanner.lookAhead()
    val stmts = statementList()
    PackageDef(EmptyName, stmts, unit.sourceFile, spanFrom(t))

  /**
    * Consume the expected token if it exists and return true, otherwise return false
    * @param expected
    * @return
    */
  def consumeIfExist(expected: SqlToken): Boolean =
    val t = scanner.lookAhead()
    if t.token == expected then
      consumeToken()
      true
    else
      false

  def consume(expected: SqlToken)(using code: SourceCode): TokenData[SqlToken] =
    val t = scanner.nextToken()
    if t.token == expected then
      lastToken = t
      t
    else
      throw StatusCode
        .SYNTAX_ERROR
        .newException(
          s"Expected ${expected}, but found ${t.token} (context: ${code.fileName}:${code.line})",
          t.sourceLocation(using compilationUnit)
        )

  def consumeToken(): TokenData[SqlToken] =
    val t = scanner.nextToken()
    lastToken = t
    t

  /**
    * Compute a span from the given token to the last read token
    *
    * @param startToken
    * @return
    */
  private def spanFrom(startToken: TokenData[SqlToken]): Span = startToken
    .span
    .extendTo(lastToken.span)

  private def spanFrom(startSpan: Span): Span = startSpan.extendTo(lastToken.span)

  private def unexpected(t: TokenData[SqlToken])(using code: SourceCode): Nothing =
    throw StatusCode
      .SYNTAX_ERROR
      .newException(
        s"Unexpected token: <${t.token}> '${t.str}' (context: SqlParser.scala:${code.line})",
        t.sourceLocation(using compilationUnit)
      )

  private def unexpected(expr: Expression)(using code: SourceCode): Nothing =
    throw StatusCode
      .SYNTAX_ERROR
      .newException(
        s"Unexpected expression: ${expr} (context: SqlParser.scala:${code.line})",
        expr.sourceLocationOfCompilationUnit(using compilationUnit)
      )

  def statementList(): List[LogicalPlan] =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.EOF =>
        Nil
      case SqlToken.SEMICOLON =>
        consume(SqlToken.SEMICOLON)
        statementList()
      case _ =>
        val stmt = statement()
        stmt :: statementList()

  def statement(): LogicalPlan =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.ALTER | SqlToken.SET | SqlToken.RESET =>
        alterStatement()
      case SqlToken.EXPLAIN =>
        explain()
//      case SqlToken.DESCRIBE =>
//       describe()
      case t if t.isUpdateStart =>
        update()
      case t if t.isQueryStart =>
        query()
      case SqlToken.SHOW =>
        show()
      case SqlToken.USE =>
        use()
      case _ =>
        unexpected(t)

    end match

  end statement

  def alterStatement(): LogicalPlan =
    val t = scanner.lookAhead()
    val alterType: AlterType =
      t.token match
        case SqlToken.ALTER =>
          consume(SqlToken.ALTER)
          scanner.lookAhead().token match
            case SqlToken.SYSTEM =>
              consume(SqlToken.SYSTEM)
              AlterType.SYSTEM
            case SqlToken.SESSION =>
              consume(SqlToken.SESSION)
              AlterType.SESSION
            case _ =>
              AlterType.DEFAULT
        case _ =>
          AlterType.DEFAULT

    val t2 = scanner.lookAhead()
    t2.token match
      case SqlToken.SET =>
        consume(SqlToken.SET)
        val id = identifier()
        consume(SqlToken.EQ)
        val value = expression()
        AlterVariable(alterType, false, id, Some(value), spanFrom(t))
      case SqlToken.RESET =>
        consume(SqlToken.RESET)
        scanner.lookAhead().token match
          case SqlToken.ALL =>
            consume(SqlToken.ALL)
            AlterVariable(alterType, true, EmptyName, None, spanFrom(t))
          case _ =>
            val id = identifier()
            AlterVariable(alterType, false, id, None, spanFrom(t))
      case other =>
        unexpected(t2)

  end alterStatement

  def explain(): ExplainPlan =
    val t = consume(SqlToken.EXPLAIN)

    // Calcite require PLAN keyword after EXPLAIN
    if scanner.lookAhead().token == SqlToken.PLAN then
      consume(SqlToken.PLAN)

    // TODO Read EXPLAIN parameters

    // Calcite requires FOR keyword before the query
    if scanner.lookAhead().token == SqlToken.FOR then
      consume(SqlToken.FOR)

    val body = queryOrUpdate()
    ExplainPlan(body, spanFrom(t))

  end explain

//  def describe(): LogicalPlan =
//    val t = consume(SqlToken.DESCRIBE)
//    scanner.lookAhead().token match
//      case SqlToken.DATABASE =>
//        consume(SqlToken.DATABASE)
//        val name = qualifiedName()
//        DescribeStmt(DescribeTarget.DATABASE, name, spanFrom(t))
//      case SqlToken.CATALOG =>
//        consume(SqlToken.CATALOG)
//        val name = qualifiedName()
//        DescribeStmt(DescribeTarget.CATALOG, name, spanFrom(t))
//      case SqlToken.SCHEMA =>
//        consume(SqlToken.SCHEMA)
//        val name = qualifiedName()
//        DescribeStmt(DescribeTarget.SCHEMA, name, spanFrom(t))
//      case SqlToken.TABLE =>
//        consume(SqlToken.TABLE)
//        val name = qualifiedName()
//        DescribeStmt(DescribeTarget.TABLE, name, spanFrom(t))
//      case tk if tk.isIdentifier =>
//        val name = qualifiedName()
//        DescribeStmt(DescribeTarget.TABLE, name, spanFrom(t))
//      case SqlToken.STATEMENT =>
//        consume(SqlToken.STATEMENT)
//        val q = query()
//        Describe(q, spanFrom(t))
//      case tk if tk.isQueryStart =>
//        val q = query()
//        Describe(q, spanFrom(t))

  def queryOrUpdate(): LogicalPlan =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.WITH | SqlToken.SELECT =>
        query()
      case SqlToken.VALUE | SqlToken.VALUES =>
        values()
      case SqlToken.INSERT | SqlToken.UPSERT =>
        insert()
      case SqlToken.UPDATE =>
        update()
      case SqlToken.MERGE =>
        merge()
      case SqlToken.DELETE =>
        delete()
      case _ =>
        unexpected(t)
  end queryOrUpdate

  def insert(): InsertOps =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.INSERT =>
        consume(SqlToken.INSERT)
        val target = qualifiedName()
        val columns =
          scanner.lookAhead().token match
            case SqlToken.L_PAREN =>
              consume(SqlToken.L_PAREN)
              val cols = identifierList()
              consume(SqlToken.R_PAREN)
              cols
            case _ =>
              Nil
        consume(SqlToken.VALUES)
        val q = query()
        Insert(target, columns, q, spanFrom(t))
      case SqlToken.UPSERT =>
        consume(SqlToken.UPSERT)
        val target = qualifiedName()
        val columns =
          scanner.lookAhead().token match
            case SqlToken.L_PAREN =>
              consume(SqlToken.L_PAREN)
              val cols = identifierList()
              consume(SqlToken.R_PAREN)
              cols
            case _ =>
              Nil
        consume(SqlToken.VALUES)
        val q = query()
        Upsert(target, columns, q, spanFrom(t))
      case _ =>
        unexpected(t)
    end match
  end insert

  def update(): UpdateRows =
    val t      = consume(SqlToken.UPDATE)
    val target = qualifiedName()
    consume(SqlToken.SET)

    val lst = assignments()
    val cond =
      scanner.lookAhead().token match
        case SqlToken.WHERE =>
          consume(SqlToken.WHERE)
          Some(expression())
        case _ =>
          None
    UpdateRows(target, lst, cond, spanFrom(t))

  end update

  def assignments(): List[UpdateAssignment] =
    val t = scanner.lookAhead()
    t.token match
      case id if id.isIdentifier =>
        val target = identifier()
        consume(SqlToken.EQ)
        val value = expression()
        scanner.lookAhead().token match
          case SqlToken.COMMA =>
            consume(SqlToken.COMMA)
            UpdateAssignment(target, value, spanFrom(t)) :: assignments()
          case _ =>
            List(UpdateAssignment(target, value, spanFrom(t)))
      case _ =>
        Nil

  def merge(): Merge =
    val t = consume(SqlToken.MERGE)
    consume(SqlToken.INTO)
    val target = qualifiedName()
    val alias =
      scanner.lookAhead().token match
        case id if id.isIdentifier =>
          Some(identifier())
        case _ =>
          None
    consume(SqlToken.USING)
    val using = tablePrimary()
    consume(SqlToken.ON)
    val on = expression()
    val whenMatched =
      scanner.lookAhead().token match
        case SqlToken.WHEN =>
          consume(SqlToken.WHEN)
          consume(SqlToken.MATCHED)
          consume(SqlToken.THEN)
          consume(SqlToken.UPDATE)
          consume(SqlToken.SET)
          val lst = assignments()
          Some(lst)
        case _ =>
          None
    val whenNotMatchedInsert =
      scanner.lookAhead().token match
        case SqlToken.WHEN =>
          consume(SqlToken.WHEN)
          consume(SqlToken.NOT)
          consume(SqlToken.MATCHED)
          consume(SqlToken.THEN)
          consume(SqlToken.INSERT)
          val insertValues = values()
          Some(insertValues)
        case _ =>
          None
    Merge(target, alias, using, on, whenMatched, whenNotMatchedInsert, spanFrom(t))
  end merge

  def delete(): DeleteOps =
    val t = consume(SqlToken.DELETE)
    consume(SqlToken.FROM)
    val target = tablePrimary()

    val filteredRelation =
      scanner.lookAhead().token match
        case SqlToken.WHERE =>
          consume(SqlToken.WHERE)
          val cond = expression()
          Filter(target, cond, spanFrom(t))
        case _ =>
          target

    def deleteExpr(x: Relation): DeleteOps =
      x match
        case f: FilteringRelation =>
          deleteExpr(f.child)
        case TableRef(qname: QualifiedName, _) =>
          Delete(filteredRelation, qname, spanFrom(t))
        case f: FileScan =>
          DeleteFromFile(filteredRelation, f.path, spanFrom(t))
        case other =>
          throw StatusCode
            .SYNTAX_ERROR
            .newException(
              s"delete statement can't have ${other.modelName} operator",
              t.sourceLocation(using unit)
            )
    deleteExpr(filteredRelation)

  end delete

  def query(): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.VALUE | SqlToken.VALUES =>
        values()
      case q if q.isQueryStart =>
        select()
      case SqlToken.L_PAREN =>
        consume(SqlToken.L_PAREN)
        val subQuery = query()
        consume(SqlToken.R_PAREN)
        subQuery
      case _ =>
        unexpected(t)

  def selectItems(): List[Attribute] =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.COMMA =>
        consume(SqlToken.COMMA)
        selectItems()
      case token if token.isQueryDelimiter =>
        Nil
      case t if t.tokenType == TokenType.Keyword && !SqlToken.literalStartKeywords.contains(t) =>
        Nil
      case _ =>
        selectItem() :: selectItems()
    end match

  end selectItems

  def selectItem(): SingleColumn =
    def selectItemWithAlias(item: Expression): SingleColumn =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.AS =>
          consume(SqlToken.AS)
          val alias = identifier()
          SingleColumn(alias, item, spanFrom(t))
        case id if id.isIdentifier =>
          val alias = identifier()
          // Propagate the column name for a single column reference
          SingleColumn(alias, item, spanFrom(t))
        case _ =>
          item match
            case i: Identifier =>
              // Propagate the column name for a single column reference
              SingleColumn(i, i, spanFrom(t))
            case _ =>
              SingleColumn(EmptyName, item, spanFrom(t))

    val t = scanner.lookAhead()
    t.token match
      case id if id.isIdentifier =>
        val exprOrColumName = expression()
        exprOrColumName match
          case Eq(columnName: Identifier, expr: Expression, span) =>
            SingleColumn(columnName, expr, spanFrom(t))
          case _ =>
            selectItemWithAlias(exprOrColumName)
      case _ =>
        val expr = expression()
        selectItemWithAlias(expr)

  end selectItem

  def select(): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.SELECT =>
        consume(SqlToken.SELECT)
        val isDistinct = consumeIfExist(SqlToken.DISTINCT)
        val items      = selectItems()
        var r          = fromClause()
        r = whereClause(r)
        val g          = groupBy(r)
        val hasGroupBy = r ne g
        r = g

        r =
          if hasGroupBy then
            Agg(r, items, spanFrom(t))
          else
            Project(r, items, spanFrom(t))
        r = orderBy(r)
        r = limit(r)
        r = offset(r)
        r
      case other =>
        unexpected(t)

  end select

  def fromClause(): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.FROM =>
        consume(SqlToken.FROM)
        tablePrimary()
      case _ =>
        unexpected(t)

  def whereClause(input: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.WHERE =>
        consume(SqlToken.WHERE)
        val cond = booleanExpression()
        Filter(input, cond, spanFrom(t))
      case _ =>
        input

  def groupBy(input: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.GROUP =>
        consume(SqlToken.GROUP)
        consume(SqlToken.BY)
        val items = groupByItemList()
        val g     = GroupBy(input, items, spanFrom(t))
        having(g)
      case _ =>
        input

  def groupByItemList(): List[GroupingKey] =
    val t = scanner.lookAhead()
    t.token match
      case id if id.isIdentifier =>
        val item = selectItem()
        val key  = UnresolvedGroupingKey(item.nameExpr, item.expr, spanFrom(t))
        key :: groupByItemList()
      case SqlToken.COMMA =>
        consume(SqlToken.COMMA)
        groupByItemList()
      case t if t.tokenType == TokenType.Keyword =>
        Nil
      case SqlToken.EOF =>
        Nil
      case _ =>
        // expression only
        val e   = expression()
        val key = UnresolvedGroupingKey(EmptyName, e, e.span)
        key :: groupByItemList()

  def having(input: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.HAVING =>
        consume(SqlToken.HAVING)
        val cond = booleanExpression()
        Filter(input, cond, spanFrom(t))
      case _ =>
        input

  def orderBy(input: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.ORDER =>
        consume(SqlToken.ORDER)
        consume(SqlToken.BY)
        val items = sortItems()
        Sort(input, items, spanFrom(t))
      case _ =>
        input

  def limit(input: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.LIMIT =>
        consume(SqlToken.LIMIT)
        val limit = consume(SqlToken.INTEGER_LITERAL)
        Limit(input, LongLiteral(limit.str.toLong, limit.span), spanFrom(t))
      case _ =>
        input

  def offset(input: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.OFFSET =>
        consume(SqlToken.OFFSET)
        val offset = consume(SqlToken.INTEGER_LITERAL)
        Offset(input, LongLiteral(offset.str.toLong, offset.span), spanFrom(t))
      case _ =>
        input

  def values(): Values =
    def valueList(): List[Expression] =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.L_PAREN =>
          consume(SqlToken.L_PAREN)
          val values = expressionList()
          consume(SqlToken.R_PAREN)
          values
        case _ =>
          unexpected(t)

    val t = scanner.lookAhead()
    t.token match
      case SqlToken.VALUE | SqlToken.VALUES =>
        consume(t.token)
        val values = expressionList()
        Values(values, spanFrom(t))
      case _ =>
        unexpected(t)

  def show(): Show =
    def inExpr(): QualifiedName =
      scanner.lookAhead().token match
        case SqlToken.IN =>
          consume(SqlToken.IN)
          qualifiedName()
        case _ =>
          EmptyName

    val t    = consume(SqlToken.SHOW)
    val name = identifier()
    try
      val tpe = ShowType.valueOf(name.leafName.toLowerCase)
      tpe match
        case ShowType.databases | ShowType.tables | ShowType.schemas =>
          val in = inExpr()
          Show(tpe, in, spanFrom(t))
        case ShowType.catalogs =>
          Show(ShowType.catalogs, EmptyName, spanFrom(t))
        case _ =>
          unexpected(name)
    catch
      case e: IllegalArgumentException =>
        throw StatusCode
          .SYNTAX_ERROR
          .newException(s"Unknown SHOW type: ${name}", name.sourceLocationOfCompilationUnit)

  def use(): UseSchema =
    val t      = consume(SqlToken.USE)
    val schema = qualifiedName()
    UseSchema(schema, spanFrom(t))

  def expressionList(): List[Expression] =
    def next(): List[Expression] =
      val e = expression()
      scanner.lookAhead().token match
        case SqlToken.COMMA =>
          consume(SqlToken.COMMA)
          e :: next()
        case _ =>
          List(e)

    next()

  def expression(): Expression = booleanExpression()

  def booleanExpression(): Expression =
    def booleanExpressionRest(expr: Expression): Expression =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.AND =>
          consume(SqlToken.AND)
          val right = booleanExpression()
          And(expr, right, spanFrom(t))
        case SqlToken.OR =>
          consume(SqlToken.OR)
          val right = booleanExpression()
          Or(expr, right, spanFrom(t))
        case _ =>
          expr

    val t = scanner.lookAhead()
    t.token match
      case SqlToken.EXCLAMATION | SqlToken.NOT =>
        consume(t.token)
        val e = booleanExpression()
        Not(e, spanFrom(t))
      case _ =>
        val expr = valueExpression()
        booleanExpressionRest(expr)

  def valueExpression(): Expression =
    def valueExpressionRest(expr: Expression): Expression =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.PLUS =>
          consume(SqlToken.PLUS)
          val right = valueExpression()
          ArithmeticBinaryExpr(BinaryExprType.Add, expr, right, spanFrom(t))
        case SqlToken.MINUS =>
          consume(SqlToken.MINUS)
          val right = valueExpression()
          ArithmeticBinaryExpr(BinaryExprType.Subtract, expr, right, spanFrom(t))
        case SqlToken.STAR =>
          consume(SqlToken.STAR)
          val right = valueExpression()
          ArithmeticBinaryExpr(BinaryExprType.Multiply, expr, right, spanFrom(t))
        case SqlToken.DIV =>
          consume(SqlToken.DIV)
          val right = valueExpression()
          ArithmeticBinaryExpr(BinaryExprType.Divide, expr, right, spanFrom(t))
        case SqlToken.MOD =>
          consume(SqlToken.MOD)
          val right = valueExpression()
          ArithmeticBinaryExpr(BinaryExprType.Modulus, expr, right, spanFrom(t))
        case SqlToken.EQ =>
          consume(SqlToken.EQ)
          val right = valueExpression()
          Eq(expr, right, spanFrom(t))
        case SqlToken.NEQ =>
          consume(SqlToken.NEQ)
          val right = valueExpression()
          NotEq(expr, right, spanFrom(t))
        case SqlToken.IS =>
          consume(SqlToken.IS)
          scanner.lookAhead().token match
            case SqlToken.NOT =>
              consume(SqlToken.NOT)
              val right = valueExpression()
              NotEq(expr, right, spanFrom(t))
            case _ =>
              val right = valueExpression()
              Eq(expr, right, spanFrom(t))
        case SqlToken.LT =>
          consume(SqlToken.LT)
          val right = valueExpression()
          LessThan(expr, right, spanFrom(t))
        case SqlToken.GT =>
          consume(SqlToken.GT)
          val right = valueExpression()
          GreaterThan(expr, right, spanFrom(t))
        case SqlToken.LTEQ =>
          consume(SqlToken.LTEQ)
          val right = valueExpression()
          LessThanOrEq(expr, right, spanFrom(t))
        case SqlToken.GTEQ =>
          consume(SqlToken.GTEQ)
          val right = valueExpression()
          GreaterThanOrEq(expr, right, spanFrom(t))
        case SqlToken.IN =>
          consume(SqlToken.IN)
          val values = inExprList()
          In(expr, values, spanFrom(t))
        case SqlToken.LIKE =>
          consume(SqlToken.LIKE)
          val right = valueExpression()
          Like(expr, right, spanFrom(t))
        case SqlToken.NOT =>
          consume(SqlToken.NOT)
          val t2 = scanner.lookAhead()
          t2.token match
            case SqlToken.LIKE =>
              consume(SqlToken.LIKE)
              val right = valueExpression()
              NotLike(expr, right, spanFrom(t))
            case SqlToken.IN =>
              consume(SqlToken.IN)
              val values = inExprList()
              NotIn(expr, values, spanFrom(t))
            case _ =>
              unexpected(t2)
        case _ =>
          expr
      end match
    end valueExpressionRest

    val expr = primaryExpression()
    valueExpressionRest(expr)

  end valueExpression

  def inExprList(): List[Expression] =
    def rest(): List[Expression] =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.R_PAREN =>
          consume(SqlToken.R_PAREN)
          Nil
        case SqlToken.COMMA =>
          consume(SqlToken.COMMA)
          rest()
        case _ =>
          val e = valueExpression()
          e :: rest()

    consume(SqlToken.L_PAREN)
    rest()

  def functionArgs(): List[FunctionArg] =
    val args = List.newBuilder[FunctionArg]

    def nextArg: Unit =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.COMMA =>
          consume(SqlToken.COMMA)
          nextArg
        case SqlToken.R_PAREN =>
        // ok
        case _ =>
          args += functionArg()
          nextArg

    nextArg
    args.result()

  def functionArg(): FunctionArg =
    val t = scanner.lookAhead()
    scanner.lookAhead().token match
      case id if id.isIdentifier =>
        val nameOrArg = expression()
        nameOrArg match
          case i: Identifier =>
            scanner.lookAhead().token match
              case SqlToken.EQ =>
                consume(SqlToken.EQ)
                val expr = expression()
                FunctionArg(Some(Name.termName(i.leafName)), expr, spanFrom(t))
              case _ =>
                FunctionArg(None, nameOrArg, spanFrom(t))
          case Eq(i: Identifier, v: Expression, span) =>
            FunctionArg(Some(Name.termName(i.leafName)), v, spanFrom(t))
          case expr: Expression =>
            FunctionArg(None, nameOrArg, spanFrom(t))
      case _ =>
        val nameOrArg = expression()
        FunctionArg(None, nameOrArg, spanFrom(t))

  def primaryExpression(): Expression =
    def primaryExpressionRest(expr: Expression): Expression =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.DOT =>
          consume(SqlToken.DOT)
          val next = identifier()
          scanner.lookAhead().token match
            case SqlToken.L_PAREN =>
              val sel  = DotRef(expr, next, DataType.UnknownType, spanFrom(t))
              val p    = consume(SqlToken.L_PAREN)
              val args = functionArgs()
              consume(SqlToken.R_PAREN)
              val w = window()
              val f = FunctionApply(sel, args, w, spanFrom(t))
              primaryExpressionRest(f)
            case _ =>
              primaryExpressionRest(DotRef(expr, next, DataType.UnknownType, spanFrom(t)))
        case SqlToken.L_PAREN =>
          expr match
            case n: NameExpr =>
              consume(SqlToken.L_PAREN)
              val args = functionArgs()
              consume(SqlToken.R_PAREN)
              // Global function call
              val w = window()
              val f = FunctionApply(n, args, w, spanFrom(t))
              primaryExpressionRest(f)
            case _ =>
              unexpected(expr)
        case SqlToken.L_BRACKET =>
          consume(SqlToken.L_BRACKET)
          val index = expression()
          consume(SqlToken.R_BRACKET)
          primaryExpressionRest(ArrayAccess(expr, index, spanFrom(t)))
        case SqlToken.R_ARROW if expr.isIdentifier =>
          consume(SqlToken.R_ARROW)
          val body = identifier()
          primaryExpressionRest(
            LambdaExpr(args = List(expr.asInstanceOf[Identifier]), body, spanFrom(t))
          )
        case SqlToken.OVER =>
          window() match
            case Some(w) =>
              WindowApply(expr, w, spanFrom(t))
            case _ =>
              expr
        case _ =>
          expr
      end match
    end primaryExpressionRest

    val t = scanner.lookAhead()
    val expr =
      t.token match
        case SqlToken.NULL | SqlToken.INTEGER_LITERAL | SqlToken.DOUBLE_LITERAL |
            SqlToken.FLOAT_LITERAL | SqlToken.DECIMAL_LITERAL | SqlToken.EXP_LITERAL | SqlToken
              .STRING_LITERAL =>
          literal()
        case SqlToken.CASE =>
          val cases                          = List.newBuilder[WhenClause]
          var elseClause: Option[Expression] = None
          def nextCase: Unit =
            val t = scanner.lookAhead()
            t.token match
              case SqlToken.WHEN =>
                consume(SqlToken.WHEN)
                val cond = booleanExpression()
                consume(SqlToken.THEN)
                val thenExpr = expression()
                cases += WhenClause(cond, thenExpr, spanFrom(t))
                nextCase
              case SqlToken.ELSE =>
                consume(SqlToken.ELSE)
                val elseExpr = expression()
                elseClause = Some(elseExpr)
              case _ =>
          // done
          end nextCase

          consume(SqlToken.CASE)
          val target =
            scanner.lookAhead().token match
              case SqlToken.WHEN =>
                None
              case other =>
                Some(expression())
          nextCase
          consume(SqlToken.END)
          CaseExpr(target, cases.result(), elseClause, spanFrom(t))
        case q if q.isQueryStart =>
          val subQuery = query()
          SubQueryExpression(subQuery, spanFrom(t))
        case SqlToken.L_PAREN =>
          consume(SqlToken.L_PAREN)
          val t2 = scanner.lookAhead()
          t2.token match
            case q if q.isQueryStart =>
              val subQuery = query()
              consume(SqlToken.R_PAREN)
              SubQueryExpression(subQuery, spanFrom(t))
            case id if id.isIdentifier =>
              val exprs = List.newBuilder[Expression]

              // true if the expression is a list of identifiers
              def nextIdentifier: Boolean =
                scanner.lookAhead().token match
                  case SqlToken.COMMA =>
                    consume(SqlToken.COMMA)
                    nextIdentifier
                  case SqlToken.R_PAREN =>
                    // ok
                    true
                  case _ =>
                    val expr = expression()
                    exprs += expr
                    expr match
                      case i: Identifier =>
                        nextIdentifier
                      case _ =>
                        false

              val isIdentifierList = nextIdentifier
              consume(SqlToken.R_PAREN)
              val args = exprs.result()
              val t3   = scanner.lookAhead()
              t3.token match
                case SqlToken.R_ARROW if isIdentifierList =>
                  // Lambda
                  consume(SqlToken.R_ARROW)
                  val body = identifier()
                  LambdaExpr(args.map(_.asInstanceOf[Identifier]), body, spanFrom(t))
                case _ if args.size == 1 =>
                  ParenthesizedExpression(args.head, spanFrom(t))
                case _ =>
                  unexpected(t3)
            case _ =>
              val e = expression()
              consume(SqlToken.R_PAREN)
              ParenthesizedExpression(e, spanFrom(t))
          end match
        case SqlToken.ARRAY | SqlToken.L_BRACKET =>
          array()
        case SqlToken.MAP =>
          map()
        case id if id.isIdentifier =>
          identifier()
        case SqlToken.STAR =>
          identifier()
        case _ =>
          unexpected(t)
    primaryExpressionRest(expr)

  end primaryExpression

  def array(): ArrayConstructor =
    consumeIfExist(SqlToken.ARRAY)
    val t        = consume(SqlToken.L_BRACKET)
    val elements = List.newBuilder[Expression]

    def nextElement: Unit =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.COMMA =>
          consume(SqlToken.COMMA)
          nextElement
        case SqlToken.R_BRACKET =>
        // ok
        case _ =>
          elements += expression()
          nextElement

    nextElement
    consume(SqlToken.R_BRACKET)
    ArrayConstructor(elements.result(), spanFrom(t))

  def map(): MapValue =
    val entries = List.newBuilder[MapEntry]

    def nextEntry: Unit =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.COMMA =>
          consume(SqlToken.COMMA)
          nextEntry
        case SqlToken.R_BRACE =>
        // ok
        case _ =>
          val key = expression()
          consume(SqlToken.COLON)
          val value = expression()
          entries += MapEntry(key, value, spanFrom(t))
          nextEntry

    val t = consume(SqlToken.MAP)
    consume(SqlToken.L_BRACE)
    nextEntry
    consume(SqlToken.R_BRACE)
    MapValue(entries.result(), spanFrom(t))

  def literal(): Literal =
    def removeUnderscore(s: String): String = s.replaceAll("_", "")

    val t = consumeToken()
    t.token match
      case SqlToken.NULL =>
        NullLiteral(spanFrom(t))
      case SqlToken.INTEGER_LITERAL =>
        LongLiteral(removeUnderscore(t.str).toLong, spanFrom(t))
      case SqlToken.DOUBLE_LITERAL =>
        DoubleLiteral(t.str.toDouble, spanFrom(t))
      case SqlToken.FLOAT_LITERAL =>
        DoubleLiteral(t.str.toFloat, spanFrom(t))
      case SqlToken.DECIMAL_LITERAL =>
        DecimalLiteral(removeUnderscore(t.str), spanFrom(t))
      case SqlToken.EXP_LITERAL =>
        DecimalLiteral(t.str, spanFrom(t))
      case SqlToken.STRING_LITERAL =>
        StringLiteral(t.str, spanFrom(t))
      case _ =>
        unexpected(t)

  def sortItems(): List[SortItem] =
    def sortOrder(): Option[SortOrdering] =
      scanner.lookAhead().token match
        case SqlToken.ASC =>
          consume(SqlToken.ASC)
          Some(SortOrdering.Ascending)
        case SqlToken.DESC =>
          consume(SqlToken.DESC)
          Some(SortOrdering.Descending)
        case _ =>
          None

    val t = scanner.lookAhead()
    t.token match
      case id if id.isIdentifier =>
        val expr  = expression()
        val order = sortOrder()
        // TODO: Support NullOrdering
        SortItem(expr, order, None, spanFrom(expr.span)) :: sortItems()
      case SqlToken.INTEGER_LITERAL =>
        val expr  = literal()
        val order = sortOrder()
        SortItem(expr, order, None, spanFrom(expr.span)) :: sortItems()
      case SqlToken.COMMA =>
        consume(SqlToken.COMMA)
        sortItems()
      case _ =>
        Nil

  def window(): Option[Window] =

    def partitionKeys(): List[Expression] =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.R_PAREN | SqlToken.ORDER | SqlToken.RANGE | SqlToken.ROWS =>
          Nil
        case SqlToken.COMMA =>
          consume(SqlToken.COMMA)
          partitionKeys()
        case _ =>
          val e = expression()
          e :: partitionKeys()
      end match
    end partitionKeys

    def partitionBy(): Seq[Expression] =
      scanner.lookAhead().token match
        case SqlToken.PARTITION =>
          consume(SqlToken.PARTITION)
          consume(SqlToken.BY)
          partitionKeys()
        case _ =>
          Nil

    def orderBy(): Seq[SortItem] =
      scanner.lookAhead().token match
        case SqlToken.ORDER =>
          consume(SqlToken.ORDER)
          consume(SqlToken.BY)
          sortItems()
        case _ =>
          Nil

    def windowFrame(): Option[WindowFrame] =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.ROWS =>
          consume(SqlToken.ROWS)
          consume(SqlToken.L_BRACKET)
          val frameStart: FrameBound =
            val t = scanner.lookAhead()
            t.token match
              case SqlToken.COLON =>
                FrameBound.UnboundedPreceding
              case SqlToken.INTEGER_LITERAL =>
                val n = consume(SqlToken.INTEGER_LITERAL).str.toInt
                if n == 0 then
                  FrameBound.CurrentRow
                else
                  FrameBound.Preceding(-n)
              case _ =>
                unexpected(t)

          consume(SqlToken.COLON)

          val frameEnd: FrameBound =
            val t = scanner.lookAhead()
            t.token match
              case SqlToken.R_BRACKET =>
                FrameBound.UnboundedFollowing
              case SqlToken.INTEGER_LITERAL =>
                val n = consume(SqlToken.INTEGER_LITERAL).str.toInt
                if n == 0 then
                  FrameBound.CurrentRow
                else
                  FrameBound.Following(n)
              case _ =>
                unexpected(t)
          consume(SqlToken.R_BRACKET)
          Some(WindowFrame(FrameType.RowsFrame, frameStart, frameEnd, spanFrom(t)))
        case _ =>
          // TODO Support SqlToken.RANGE
          None
      end match
    end windowFrame

    scanner.lookAhead().token match
      case SqlToken.OVER =>
        val t = consume(SqlToken.OVER)
        consume(SqlToken.L_PAREN)
        val partition = partitionBy()
        val order     = orderBy()
        val frame     = windowFrame()
        consume(SqlToken.R_PAREN)
        Some(Window(partition, order, frame, spanFrom(t)))
      case _ =>
        None
  end window

  def tablePrimary(): Relation =
    val t = scanner.lookAhead()
    t.token match
      case id if id.isIdentifier =>
        val name = qualifiedName()
        TableRef(name, spanFrom(t))
      case SqlToken.LATERAL =>
        consume(SqlToken.LATERAL)
        // TODO Support LATERAL TABLE ...
        consume(SqlToken.L_PAREN)
        val subQuery = query()
        consume(SqlToken.R_PAREN)
        Lateral(subQuery, spanFrom(t))
      case SqlToken.L_PAREN =>
        consume(SqlToken.L_PAREN)
        val subQuery = query()
        consume(SqlToken.R_PAREN)
        subQuery
      case SqlToken.UNNEST =>
        consume(SqlToken.UNNEST)
        consume(SqlToken.L_PAREN)
        val expr = expressionList()
        consume(SqlToken.R_PAREN)
        scanner.lookAhead().token match
          case SqlToken.WITH =>
            consume(SqlToken.WITH)
            consume(SqlToken.ORDINALITY)
            Unnest(expr, withOrdinality = true, spanFrom(t))
          case _ =>
            Unnest(expr, withOrdinality = false, spanFrom(t))
      case _ =>
        unexpected(t)
    end match

  end tablePrimary

  def identifierList(): List[QualifiedName] =
    val t = scanner.lookAhead()

    def next(): List[QualifiedName] =
      val id = identifier()
      scanner.lookAhead().token match
        case SqlToken.COMMA =>
          id :: next()
        case _ =>
          List(id)

    next()

  def identifier(): Identifier =
    val t = scanner.lookAhead()
    t.token match
      case id if id.isIdentifier =>
        consume(id)
        UnquotedIdentifier(t.str, spanFrom(t))
      case SqlToken.STAR =>
        consume(SqlToken.STAR)
        Wildcard(spanFrom(t))
      case SqlToken.INTEGER_LITERAL =>
        consume(SqlToken.INTEGER_LITERAL)
        DigitIdentifier(t.str, spanFrom(t))
      case _ =>
        reserved()

  def qualifiedName(): QualifiedName = dotRef(identifier())

  def dotRef(expr: QualifiedName): QualifiedName =
    val token = scanner.lookAhead()
    token.token match
      case SqlToken.DOT =>
        val dt = consume(SqlToken.DOT)
        scanner.lookAhead().token match
          case SqlToken.STAR =>
            consume(SqlToken.STAR)
            DotRef(expr, Wildcard(spanFrom(token)), DataType.UnknownType, spanFrom(token))
          case _ =>
            val next = identifier()
            dotRef(DotRef(expr, next, DataType.UnknownType, spanFrom(token)))
      case _ =>
        expr

  def reserved(): Identifier =
    val t = consumeToken()
    t.token match
      case token if token.isReservedKeyword =>
        UnquotedIdentifier(t.str, spanFrom(t))
      case _ =>
        unexpected(t)

end SqlParser
