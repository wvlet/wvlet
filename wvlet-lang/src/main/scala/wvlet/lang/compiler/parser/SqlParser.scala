package wvlet.lang.compiler.parser

import wvlet.airframe.SourceCode
import wvlet.lang.api.{Span, StatusCode}
import wvlet.lang.compiler.parser.SqlToken.{EOF, ROW, STRING_LITERAL}
import wvlet.lang.compiler.{CompilationUnit, Name}
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.{
  DigitIdentifier,
  DotRef,
  DoubleQuotedIdentifier,
  Expression,
  NameExpr,
  QualifiedName,
  UnquotedIdentifier,
  Wildcard
}
import wvlet.lang.model.plan.{
  AlterType,
  AlterVariable,
  Delete,
  DescribeStmt,
  DescribeTarget,
  ExplainPlan,
  Insert,
  Lateral,
  LogicalPlan,
  Merge,
  PackageDef,
  Relation,
  Show,
  TableRef,
  Unnest,
  Update,
  UpdateAssignment,
  Upsert
}
import wvlet.log.LogSupport

class SqlParser(unit: CompilationUnit, isContextUnit: Boolean) extends LogSupport:

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
    PackageDef(NameExpr.EmptyName, stmts, unit.sourceFile, spanFrom(t))

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
          t.sourceLocation
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
        t.sourceLocation
      )

  private def unexpected(expr: Expression)(using code: SourceCode): Nothing =
    throw StatusCode
      .SYNTAX_ERROR
      .newException(
        s"Unexpected expression: ${expr} (context: SqlParser.scala:${code.line})",
        expr.sourceLocationOfCompilationUnit
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
      case SqlToken.DESCRIBE =>
        describe()
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
        scanner.lookAhead() match
          case SqlToken.ALL =>
            consume(SqlToken.ALL)
            AlterVariable(alterType, true, NameExpr.EmptyName, None, spanFrom(t))
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

  def describe(): LogicalPlan =
    val t = consume(SqlToken.DESCRIBE)
    scanner.lookAhead().token match
      case SqlToken.DATABASE =>
        consume(SqlToken.DATABASE)
        val name = qualifiedName()
        DescribeStmt(DescribeTarget.DATABASE, name, spanFrom(t))
      case SqlToken.CATALOG =>
        consume(SqlToken.CATALOG)
        val name = qualifiedName()
        DescribeStmt(DescribeTarget.CATALOG, name, spanFrom(t))
      case SqlToken.SCHEMA =>
        consume(SqlToken.SCHEMA)
        val name = qualifiedName()
        DescribeStmt(DescribeTarget.SCHEMA, name, spanFrom(t))
      case SqlToken.TABLE =>
        consume(SqlToken.TABLE)
        val name = qualifiedName()
        DescribeStmt(DescribeTarget.TABLE, name, spanFrom(t))
      case tk if tk.isIdentifier =>
        val name = qualifiedName()
        DescribeStmt(DescribeTarget.TABLE, name, spanFrom(t))
      case SqlToken.STATEMENT =>
        consume(SqlToken.STATEMENT)
        val q = query()
        DescribeStmt(DescribeTarget.STATEMENT, q, spanFrom(t))
      case tk if tk.isQueryStart =>
        val q = query()
        DescribeStmt(DescribeTarget.STATEMENT, q, spanFrom(t))

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

  def insert(): Insert =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.INSERT =>
        consume(SqlToken.INSERT)
        val target = tablePrimary()
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
        val query = query()
        Insert(target, columns, query, spanFrom(t))
      case SqlToken.UPSERT =>
        consume(SqlToken.UPSERT)
        val target = tablePrimary()
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
        val query = query()
        Upsert(target, columns, query, spanFrom(t))
      case _ =>
        unexpected(t)
    end match
  end insert

  def update(): Update =
    val t      = consume(SqlToken.UPDATE)
    val target = tablePrimary()
    consume(SqlToken.SET)

    val lst = assignments()
    val cond =
      scanner.lookAhead().token match
        case SqlToken.WHERE =>
          consume(SqlToken.WHERE)
          Some(expression())
        case _ =>
          None
    Update(target, lst, cond, spanFrom(t))

  end update

  def assignments(): List[UpdateAssignment] =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.IDENTIFIER =>
        val id = identifier()
        consume(SqlToken.EQ)
        val value = expression()
        scanner.lookAhead().token match
          case SqlToken.COMMA =>
            consume(SqlToken.COMMA)
            UpdateAssignment(id, value, spanFrom(t)) :: assign
          case _ =>
            UpdateAssignment(id, value, spanFrom(t))
      case _ =>
        Nil

  def merge(): Merge =
    val t = consume(SqlToken.MERGE)
    consume(SqlToken.INTO)
    val target = tablePrimary()
    val alias =
      scanner.lookAhead().token match
        case id if id.isIdentifier =>
          Some(identifier())
        case SqlToken.STRING_LITERAL =>
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
          val values = values()
          Some(values)
        case _ =>
          None
    Merge(target, alias, using, on, whenMatched, whenNotMatchedInsert, spanFrom(t))
  end merge

  def delete(): Delete =
    val t = consume(SqlToken.DELETE)
    consume(SqlToken.FROM)
    val target = tablePrimary()
    val cond =
      scanner.lookAhead().token match
        case SqlToken.WHERE =>
          consume(SqlToken.WHERE)
          Some(expression())
        case _ =>
          None
    Delete(target, target, spanFrom(t))

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
        val expr = expression()
        consume(SqlToken.R_PAREN)
        Unnest(expr, spanFrom(t))
      // TODO Support ordinality
      case _ =>
        unexpected(t)

  end tablePrimary

  def identifierList(): List[QualifiedName] =
    val t = scanner.lookAhead()

    def next: List[QualifiedName] =
      val id = identifier()
      scanner.lookAhead().token match
        case SqlToken.COMMA =>
          id :: next()
        case _ =>
          List(id)

    next

  def identifier(): QualifiedName =
    val t = scanner.lookAhead()
    t.token match
      case id if id.isIdentifier =>
        consume(id)
        UnquotedIdentifier(t.str, spanFrom(t))
      case SqlToken.STRING_LITERAL =>
        consume(SqlToken.STRING_LITERAL)
        DoubleQuotedIdentifier(t.str, spanFrom(t))
      case SqlToken.STAR =>
        consume(SqlToken.STAR)
        Wildcard(spanFrom(t))
      case SqlToken.INTEGER_LITERAL =>
        consume(SqlToken.INTEGER_LITERAL)
        DigitIdentifier(t.str, sponFrom(t))
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
            dotRef(DotRef(expr, id, DataType.UnknownType, spanFrom(token)))
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
