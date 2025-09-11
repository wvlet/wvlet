package wvlet.lang.compiler.parser

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import wvlet.airframe.SourceCode
import wvlet.lang.api.{Span, StatusCode}
import wvlet.lang.compiler.parser.SqlToken.{EOF, ROW, STAR}
import wvlet.lang.compiler.{CompilationUnit, Name, SourceFile, TermName}
import wvlet.lang.model.{DataType, RelationType}
import wvlet.lang.model.DataType.{
  EmptyRelationType,
  IntConstant,
  IntType,
  NamedType,
  TimestampField,
  TimestampType,
  TypeParameter,
  UnresolvedTypeParameter
}
import wvlet.lang.model.expr.*
import wvlet.lang.model.expr.JsonObjectModifier.ABSENT_ON_NULL
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
  private var questionMarkParamIndex: Int    = 0

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

  private def unexpected(t: TokenData[SqlToken])(using code: SourceCode): Nothing = unexpected(
    t,
    ""
  )

  private def unexpected(t: TokenData[SqlToken], message: String)(using code: SourceCode): Nothing =
    val errorMessage =
      if message == null || message.isEmpty then
        s"Unexpected token: <${t.token}> '${t.str}' (context: SqlParser.scala:${code.line})"
      else
        s"${message} (context: SqlParser.scala:${code.line})"
    throw StatusCode
      .SYNTAX_ERROR
      .newException(errorMessage, t.sourceLocation(using compilationUnit))

  private def unexpected(expr: Expression)(using code: SourceCode): Nothing = unexpected(expr, "")

  private def unexpected(expr: Expression, message: String)(using code: SourceCode): Nothing =
    val errorMessage =
      if message == null || message.isEmpty then
        s"Unexpected expression: ${expr} (context: SqlParser.scala:${code.line})"
      else
        s"${message} (context: SqlParser.scala:${code.line})"
    throw StatusCode
      .SYNTAX_ERROR
      .newException(errorMessage, expr.sourceLocationOfCompilationUnit(using compilationUnit))

  def statementList(): List[LogicalPlan] =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.EOF =>
        Nil
      case _ =>
        val stmt = statement()
        scanner.lookAhead().token match
          case SqlToken.SEMICOLON =>
            consume(SqlToken.SEMICOLON)
            stmt :: statementList()
          case _ =>
            List(stmt)

  def statement(): LogicalPlan =
    // Reset parameter counter for each statement
    questionMarkParamIndex = 0
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.ALTER | SqlToken.SET | SqlToken.RESET =>
        alterStatement()
      case SqlToken.EXPLAIN =>
        explain()
      case SqlToken.DESCRIBE =>
        describe()
      case u if u.isUpdateStart =>
        update()
      case SqlToken.WITH =>
        // Handle WITH ... SELECT or WITH ... INSERT
        withClause() match
          case q: Relation =>
            Query(q, spanFrom(t))
          case u: Update =>
            u
          case other =>
            // This should not happen with current withClause() implementation
            unexpected(t, s"Unexpected logical plan from withClause: ${other.nodeName}")
      case q if q.isQueryStart || q == SqlToken.L_PAREN =>
        // A query can start with SELECT, VALUES, or a parenthesis
        Query(query(), spanFrom(t))
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
            case SqlToken.TABLE =>
              // Handle ALTER TABLE operations
              return alterTable(spanFrom(t))
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

  def alterTable(span: Span): LogicalPlan =
    consume(SqlToken.TABLE)

    // Check for IF EXISTS
    val ifExists  = parseIfExists()
    val tableName = qualifiedName()

    val nextToken = scanner.lookAhead()
    nextToken.token match
      case SqlToken.RENAME =>
        alterTableRename(tableName, ifExists, span)
      case SqlToken.ADD =>
        alterTableAdd(tableName, ifExists, span)
      case SqlToken.DROP =>
        alterTableDrop(tableName, ifExists, span)
      case SqlToken.ALTER =>
        alterTableAlterColumn(tableName, ifExists, span)
      case SqlToken.SET =>
        alterTableSet(tableName, ifExists, span)
      case SqlToken.EXECUTE =>
        alterTableExecute(tableName, ifExists, span)
      case _ =>
        unexpected(nextToken)

  end alterTable

  def alterTableRename(tableName: NameExpr, tableIfExists: Boolean, span: Span): LogicalPlan =
    consume(SqlToken.RENAME)
    scanner.lookAhead().token match
      case SqlToken.TO =>
        // ALTER TABLE table RENAME TO new_name
        val toToken = consume(SqlToken.TO)
        val newName = qualifiedName()
        AlterTable(tableName, tableIfExists, RenameTableOp(newName, spanFrom(toToken)), span)
      case SqlToken.COLUMN =>
        // ALTER TABLE table RENAME COLUMN [IF EXISTS] old_name TO new_name
        val columnToken   = consume(SqlToken.COLUMN)
        val ifExists      = parseIfExists()
        val oldColumnName = identifier()
        consume(SqlToken.TO)
        val newColumnName = identifier()
        AlterTable(
          tableName,
          tableIfExists,
          RenameColumnOp(oldColumnName, newColumnName, ifExists, spanFrom(columnToken)),
          span
        )
      case _ =>
        unexpected(scanner.lookAhead())

  def alterTableAdd(tableName: NameExpr, tableIfExists: Boolean, span: Span): LogicalPlan =
    consume(SqlToken.ADD)
    scanner.lookAhead().token match
      case SqlToken.COLUMN =>
        val columnToken = consume(SqlToken.COLUMN)
        val ifNotExists = parseIfNotExists()
        val columnName  = identifier()
        val dataType    = typeName()

        // Parse optional column attributes
        var notNull                                  = false
        var comment: Option[String]                  = None
        var defaultValue: Option[Expression]         = None
        var properties: List[(NameExpr, Expression)] = Nil
        var position: Option[String]                 = None // FIRST, LAST, or AFTER column_name

        // Parse column attributes in a loop
        var continue = true
        while continue do
          scanner.lookAhead().token match
            case SqlToken.NOT =>
              consume(SqlToken.NOT)
              consume(SqlToken.NULL)
              notNull = true
            case SqlToken.COMMENT =>
              consume(SqlToken.COMMENT)
              val commentLiteral = literal()
              comment =
                commentLiteral match
                  case s: StringLiteral =>
                    Some(s.unquotedValue)
                  case _ =>
                    unexpected(commentLiteral, "COMMENT must be followed by a string literal")
            case SqlToken.DEFAULT =>
              consume(SqlToken.DEFAULT)
              defaultValue = Some(expression())
            case SqlToken.WITH =>
              consume(SqlToken.WITH)
              consume(SqlToken.L_PAREN)
              properties = parsePropertyList()
              consume(SqlToken.R_PAREN)
            case SqlToken.FIRST =>
              consume(SqlToken.FIRST)
              position = Some("FIRST")
            case SqlToken.LAST =>
              consume(SqlToken.LAST)
              position = Some("LAST")
            case SqlToken.AFTER =>
              consume(SqlToken.AFTER)
              val afterColumn = identifier()
              position = Some(s"AFTER ${afterColumn.fullName}")
            case _ =>
              continue = false
        end while

        val columnDef = ColumnDef(
          columnName,
          dataType,
          span,
          notNull = notNull,
          comment = comment,
          defaultValue = defaultValue,
          properties = properties,
          position = position
        )
        AlterTable(
          tableName,
          tableIfExists,
          AddColumnOp(columnDef, ifNotExists, spanFrom(columnToken)),
          span
        )
      case SqlToken.PRIMARY =>
        consume(SqlToken.PRIMARY)
        consume(SqlToken.KEY)
        consume(SqlToken.L_PAREN)
        val columns = parseIdentifierList()
        consume(SqlToken.R_PAREN)
        // For now, we'll just return a dummy DDL as this is not in our DDL case classes
        // This would need a new case class like AddPrimaryKey
        throw StatusCode.NOT_IMPLEMENTED.newException("ADD PRIMARY KEY is not yet supported")
      case _ =>
        unexpected(scanner.lookAhead())

    end match

  end alterTableAdd

  def alterTableDrop(tableName: NameExpr, tableIfExists: Boolean, span: Span): LogicalPlan =
    consume(SqlToken.DROP)
    scanner.lookAhead().token match
      case SqlToken.COLUMN =>
        val columnToken = consume(SqlToken.COLUMN)
        val ifExists    = parseIfExists()
        val columnName  = identifier()
        AlterTable(
          tableName,
          tableIfExists,
          DropColumnOp(columnName, ifExists, spanFrom(columnToken)),
          span
        )
      case _ =>
        unexpected(scanner.lookAhead())

  def alterTableAlterColumn(tableName: NameExpr, tableIfExists: Boolean, span: Span): LogicalPlan =
    val alterToken = consume(SqlToken.ALTER)

    // Check if COLUMN keyword is present (standard SQL) or if it's DuckDB syntax
    val columnName =
      if scanner.lookAhead().token == SqlToken.COLUMN then
        consume(SqlToken.COLUMN)
        identifier()
      else
        // DuckDB syntax: ALTER TABLE table ALTER column_name TYPE ...
        identifier()

    scanner.lookAhead().token match
      case SqlToken.SET =>
        consume(SqlToken.SET)
        scanner.lookAhead().token match
          case SqlToken.DATA =>
            consume(SqlToken.DATA)
            consume(SqlToken.TYPE)
            val newType = typeName()
            val using =
              if scanner.lookAhead().token == SqlToken.USING then
                consume(SqlToken.USING)
                Some(expression())
              else
                None
            AlterTable(
              tableName,
              tableIfExists,
              AlterColumnSetDataTypeOp(columnName, newType, using, spanFrom(alterToken)),
              span
            )
          case SqlToken.DEFAULT =>
            consume(SqlToken.DEFAULT)
            val defaultValue = expression()
            AlterTable(
              tableName,
              tableIfExists,
              AlterColumnSetDefaultOp(columnName, defaultValue, spanFrom(alterToken)),
              span
            )
          case SqlToken.NOT =>
            consume(SqlToken.NOT)
            consume(SqlToken.NULL)
            AlterTable(
              tableName,
              tableIfExists,
              AlterColumnSetNotNullOp(columnName, spanFrom(alterToken)),
              span
            )
          case _ =>
            unexpected(scanner.lookAhead())
        end match
      case SqlToken.DROP =>
        consume(SqlToken.DROP)
        scanner.lookAhead().token match
          case SqlToken.NOT =>
            consume(SqlToken.NOT)
            consume(SqlToken.NULL)
            AlterTable(
              tableName,
              tableIfExists,
              AlterColumnDropNotNullOp(columnName, spanFrom(alterToken)),
              span
            )
          case SqlToken.DEFAULT =>
            consume(SqlToken.DEFAULT)
            AlterTable(
              tableName,
              tableIfExists,
              AlterColumnDropDefaultOp(columnName, spanFrom(alterToken)),
              span
            )
          case _ =>
            unexpected(scanner.lookAhead())
      case SqlToken.TYPE =>
        // DuckDB syntax: ALTER TABLE table ALTER column TYPE new_type [USING expression]
        consume(SqlToken.TYPE)
        val newType = typeName()
        val using =
          if scanner.lookAhead().token == SqlToken.USING then
            consume(SqlToken.USING)
            Some(expression())
          else
            None
        AlterTable(
          tableName,
          tableIfExists,
          AlterColumnSetDataTypeOp(columnName, newType, using, spanFrom(alterToken)),
          span
        )
      case _ =>
        unexpected(scanner.lookAhead())

    end match

  end alterTableAlterColumn

  def alterTableSet(tableName: NameExpr, tableIfExists: Boolean, span: Span): LogicalPlan =
    val setToken = consume(SqlToken.SET)
    scanner.lookAhead().token match
      case SqlToken.AUTHORIZATION =>
        consume(SqlToken.AUTHORIZATION)
        consume(SqlToken.L_PAREN)

        val (principal, principalType) =
          scanner.lookAhead().token match
            case SqlToken.USER =>
              consume(SqlToken.USER)
              val user = identifier()
              (user, Some("USER"))
            case SqlToken.ROLE =>
              consume(SqlToken.ROLE)
              val role = identifier()
              (role, Some("ROLE"))
            case _ =>
              val principal = identifier()
              (principal, None)

        consume(SqlToken.R_PAREN)
        AlterTable(
          tableName,
          tableIfExists,
          SetAuthorizationOp(principal, principalType, spanFrom(setToken)),
          span
        )
      case SqlToken.PROPERTIES =>
        consume(SqlToken.PROPERTIES)
        val properties = parsePropertyList()
        AlterTable(tableName, tableIfExists, SetPropertiesOp(properties, spanFrom(setToken)), span)
      case _ =>
        unexpected(scanner.lookAhead())

    end match

  end alterTableSet

  def alterTableExecute(tableName: NameExpr, tableIfExists: Boolean, span: Span): LogicalPlan =
    val executeToken = consume(SqlToken.EXECUTE)
    val command      = identifier()

    val parameters =
      if scanner.lookAhead().token == SqlToken.L_PAREN then
        consume(SqlToken.L_PAREN)
        val params = parseParameterList()
        consume(SqlToken.R_PAREN)
        params
      else
        Nil

    val where =
      if scanner.lookAhead().token == SqlToken.WHERE then
        consume(SqlToken.WHERE)
        Some(expression())
      else
        None

    AlterTable(
      tableName,
      tableIfExists,
      ExecuteOp(command, parameters, where, spanFrom(executeToken)),
      span
    )

  private def parseEscapeClause(): Option[Expression] =
    if scanner.lookAhead().token == SqlToken.ESCAPE then
      consume(SqlToken.ESCAPE)
      Some(valueExpression())
    else
      None

  private def parseCommaSeparatedList[T](parseElement: () => T): List[T] =
    val elements = List.newBuilder[T]
    elements += parseElement()
    while scanner.lookAhead().token == SqlToken.COMMA do
      consume(SqlToken.COMMA)
      elements += parseElement()
    elements.result()

  def parsePropertyList(): List[(NameExpr, Expression)] = parseCommaSeparatedList(() =>
    parseProperty()
  )

  def parseProperty(): (NameExpr, Expression) =
    val name = identifier()
    consume(SqlToken.EQ)
    val value = expression()
    (name, value)

  def parseParameterList(): List[(NameExpr, Expression)] = parseCommaSeparatedList(() =>
    parseParameter()
  )

  def parseParameter(): (NameExpr, Expression) =
    val name = identifier()
    consume(SqlToken.EQ) // Using = for parameter assignment
    val value = expression()
    (name, value)

  def parseIdentifierList(): List[NameExpr] = parseCommaSeparatedList(() => identifier())

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
    val t                  = consume(SqlToken.DESCRIBE)
    val lookaheadTokenData = scanner.lookAhead()
    lookaheadTokenData.token match
      case token @ (SqlToken.INPUT | SqlToken.OUTPUT) =>
        consume(token)
        val name     = identifier()
        val tokenStr = token.str.toUpperCase
        name match
          case _: Wildcard =>
            unexpected(name, s"Statement name for DESCRIBE ${tokenStr} cannot be a wildcard (*).")
          case _: DigitIdentifier =>
            unexpected(
              name,
              s"Statement name for DESCRIBE ${tokenStr} cannot be a numeric literal."
            )
          case _ =>
            if token == SqlToken.INPUT then
              DescribeInput(name, spanFrom(t))
            else
              DescribeOutput(name, spanFrom(t))
      case _ =>
        unexpected(
          lookaheadTokenData,
          s"Unsupported DESCRIBE target: ${lookaheadTokenData
              .token}. Only DESCRIBE INPUT and DESCRIBE OUTPUT are currently supported."
        )

  end describe

  def queryOrUpdate(): LogicalPlan =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.WITH =>
        // Handle WITH ... INSERT (Hive syntax) or WITH ... SELECT
        withClause()
      case SqlToken.SELECT =>
        query()
      case SqlToken.VALUES =>
        values()
      case SqlToken.CREATE | SqlToken.UPDATE =>
        update()
      case SqlToken.MERGE =>
        merge()
      case SqlToken.DELETE =>
        delete()
      case _ =>
        unexpected(t)
  end queryOrUpdate

  private def parseWithCTEs(startToken: TokenData[SqlToken]): (Boolean, List[AliasedRelation]) =
    consume(SqlToken.WITH)
    val isRecursive = consumeIfExist(SqlToken.RECURSIVE)

    def withQuery(): List[AliasedRelation] =
      // Use tail-recursive loop to avoid StackOverflowError with many CTEs
      @annotation.tailrec
      def loop(acc: List[AliasedRelation]): List[AliasedRelation] =
        val t     = scanner.lookAhead()
        val alias = identifier()
        val typeDefs =
          scanner.lookAhead().token match
            case SqlToken.L_PAREN =>
              consume(SqlToken.L_PAREN)
              val types = namedTypes()
              consume(SqlToken.R_PAREN)
              Some(types)
            case _ =>
              None

        consume(SqlToken.AS)
        consume(SqlToken.L_PAREN)
        val body = query()
        consume(SqlToken.R_PAREN)
        // Fix span calculation - use the CTE's own start token, not the WITH token
        val r = AliasedRelation(body, alias, typeDefs, spanFrom(t))

        scanner.lookAhead().token match
          case SqlToken.COMMA =>
            consume(SqlToken.COMMA)
            loop(r :: acc)
          case _ =>
            (r :: acc).reverse

      loop(Nil)
    end withQuery

    (isRecursive, withQuery())

  end parseWithCTEs

  def withClause(): LogicalPlan =
    val t                        = scanner.lookAhead()
    val (isRecursive, withStmts) = parseWithCTEs(t)
    // Capture the span of just the WITH clause and CTEs
    val withClauseSpan = spanFrom(t)

    // Check what follows the WITH clause
    scanner.lookAhead().token match
      case SqlToken.INSERT =>
        // Handle WITH ... INSERT (Hive syntax)
        val insertStmt = insert()
        // Wrap the INSERT's query child with the WITH clause
        insertStmt match
          case InsertInto(target, columns, child, partitionOptions, _) =>
            // WithQuery span should be from WITH to end of CTEs only
            val withBody = WithQuery(isRecursive, withStmts, child, withClauseSpan)
            // INSERT span should include entire WITH ... INSERT
            InsertInto(target, columns, withBody, partitionOptions, spanFrom(t))
          case InsertOverwrite(target, child, partitionOptions, _) =>
            // WithQuery span should be from WITH to end of CTEs only
            val withBody = WithQuery(isRecursive, withStmts, child, withClauseSpan)
            // INSERT span should include entire WITH ... INSERT
            InsertOverwrite(target, withBody, partitionOptions, spanFrom(t))
          case Insert(target, columns, query, _) =>
            // WithQuery span should be from WITH to end of CTEs only
            val withBody = WithQuery(isRecursive, withStmts, query, withClauseSpan)
            // INSERT span should include entire WITH ... INSERT
            Insert(target, columns, withBody, spanFrom(t))
          case Upsert(target, columns, query, _) =>
            // WithQuery span should be from WITH to end of CTEs only
            val withBody = WithQuery(isRecursive, withStmts, query, withClauseSpan)
            // UPSERT span should include entire WITH ... INSERT
            Upsert(target, columns, withBody, spanFrom(t))
          case other =>
            // This should not happen with current insert() implementation
            unexpected(t, s"Unexpected update type from insert(): ${other.nodeName}")
      case _ =>
        // Standard WITH ... SELECT
        val body = query()
        WithQuery(isRecursive, withStmts, body, spanFrom(t))
    end match
  end withClause

  def insert(): Update =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.INSERT =>
        consume(SqlToken.INSERT)
        scanner.lookAhead().token match
          case SqlToken.INTO =>
            consume(SqlToken.INTO)
            // Handle optional TABLE keyword after INSERT INTO (Hive syntax)
            consumeIfExist(SqlToken.TABLE)
            val target = qualifiedName()
            val (columns, q) =
              if scanner.lookAhead().token == SqlToken.L_PAREN then
                consume(SqlToken.L_PAREN)
                scanner.lookAhead().token match
                  case SqlToken.SELECT | SqlToken.WITH | SqlToken.VALUES =>
                    // This is a parenthesized query
                    val subQuery = query()
                    consume(SqlToken.R_PAREN)
                    (Nil, subQuery)
                  case _ =>
                    // This is a column list
                    val cols = identifierList()
                    consume(SqlToken.R_PAREN)
                    (cols, query())
              else
                // No column list or parenthesized query
                (Nil, query())
            val (childRelation, allOptions) = extractAllPartitionOptions(q)
            InsertInto(target, columns, childRelation, allOptions, spanFrom(t))
          case SqlToken.OVERWRITE =>
            consume(SqlToken.OVERWRITE)
            consume(SqlToken.TABLE)
            val target = qualifiedName()
            // Note: OVERWRITE TABLE doesn't support column lists in standard Hive syntax
            val q                           = query()
            val (childRelation, allOptions) = extractAllPartitionOptions(q)
            InsertOverwrite(target, childRelation, allOptions, spanFrom(t))
          case _ =>
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
        end match
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

  def update(): LogicalPlan =

    def consumeEntityType(): SqlToken =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.TABLE =>
          consume(SqlToken.TABLE)
          SqlToken.TABLE
        case SqlToken.SCHEMA =>
          consume(SqlToken.SCHEMA)
          SqlToken.SCHEMA
        case SqlToken.VIEW =>
          consume(SqlToken.VIEW)
          SqlToken.VIEW
        case _ =>
          SqlToken.TABLE // default to table

    def tableElems(): List[ColumnDef] =
      val t = scanner.lookAhead()
      t.token match
        case id if id.isIdentifier =>
          val col = identifier()
          val tpe = typeName()
          val cd  = ColumnDef(col, tpe, spanFrom(col.span))
          scanner.lookAhead().token match
            case SqlToken.COMMA =>
              consume(SqlToken.COMMA)
              cd :: tableElems()
            case _ =>
              List(cd)
        case _ =>
          Nil

    def parseCreateStatement(): LogicalPlan =
      val t = consume(SqlToken.CREATE)
      val replace =
        scanner.lookAhead().token match
          case SqlToken.OR =>
            consume(SqlToken.OR)
            consume(SqlToken.REPLACE)
            true
          case _ =>
            false

      val entityType = consumeEntityType()
      entityType match
        case SqlToken.SCHEMA =>
          val ifNotExists = parseIfNotExists()
          val schemaName  = qualifiedName()
          CreateSchema(schemaName, ifNotExists, None, spanFrom(t))
        case SqlToken.VIEW =>
          val viewName = qualifiedName()
          consume(SqlToken.AS)
          val queryPlan = this.query() // Use 'this' to access the outer query method
          CreateView(viewName, replace, queryPlan, spanFrom(t))
        case SqlToken.TABLE | _ =>
          val createMode =
            if replace then
              CreateMode.Replace
            else if parseIfNotExists() then
              CreateMode.IfNotExists
            else
              CreateMode.NoOverwrite
          val tbl = qualifiedName()

          // Parse optional column definitions: (col1 type1, col2 type2, ...)
          val columnDefs =
            if scanner.lookAhead().token == SqlToken.L_PAREN then
              consume(SqlToken.L_PAREN)
              val elems = tableElems()
              consume(SqlToken.R_PAREN)
              elems
            else
              Nil

          // Parse optional WITH properties: WITH (prop1 = val1, prop2 = val2, ...)
          val properties =
            if scanner.lookAhead().token == SqlToken.WITH then
              consume(SqlToken.WITH)
              consume(SqlToken.L_PAREN)
              val props = parsePropertyList()
              consume(SqlToken.R_PAREN)
              props
            else
              Nil

          // Parse optional AS SELECT clause
          scanner.lookAhead().token match
            case SqlToken.AS =>
              consume(SqlToken.AS)
              val q                           = query()
              val (childRelation, allOptions) = extractAllPartitionOptions(q)
              CreateTableAs(tbl, createMode, childRelation, properties, allOptions, spanFrom(t))
            case _ =>
              // No AS SELECT clause - create table with columns and/or properties
              CreateTable(
                tbl,
                createMode == CreateMode.IfNotExists,
                columnDefs,
                properties,
                spanFrom(t)
              )
      end match
    end parseCreateStatement

    def parseDropStatement(): LogicalPlan =
      val t          = consume(SqlToken.DROP)
      val entityType = consumeEntityType()
      val ifExists   = parseIfExists()
      val name       = qualifiedName()

      entityType match
        case SqlToken.SCHEMA =>
          DropSchema(name, ifExists, spanFrom(t))
        case SqlToken.VIEW =>
          DropView(name, ifExists, spanFrom(t))
        case SqlToken.TABLE | _ =>
          DropTable(name, ifExists, spanFrom(t))

    val t = scanner.lookAhead()
    t.token match
      case SqlToken.CREATE =>
        parseCreateStatement()
      case SqlToken.INSERT =>
        insert()
      case SqlToken.DROP =>
        parseDropStatement()
      case SqlToken.DELETE =>
        delete()
      case SqlToken.PREPARE =>
        prepareStatement()
      case SqlToken.EXECUTE =>
        executeStatement()
      case SqlToken.DEALLOCATE =>
        deallocateStatement()
      case SqlToken.UPDATE =>
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
      case _ =>
        unexpected(t)
    end match

  end update

  def parseIfNotExists(): Boolean =
    scanner.lookAhead().token match
      case SqlToken.IF =>
        consume(SqlToken.IF)
        consume(SqlToken.NOT)
        consume(SqlToken.EXISTS)
        true
      case _ =>
        false

  def parseIfExists(): Boolean =
    scanner.lookAhead().token match
      case SqlToken.IF =>
        consume(SqlToken.IF)
        consume(SqlToken.EXISTS)
        true
      case _ =>
        false

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

  def prepareStatement(): PrepareStatement =
    val t    = consume(SqlToken.PREPARE)
    val name = identifier()
    // Support both "FROM" (Trino) and "AS" (DuckDB) syntax
    val fromOrAs = scanner.lookAhead().token
    if fromOrAs == SqlToken.FROM || fromOrAs == SqlToken.AS then
      consumeToken() // consume FROM or AS
    else
      unexpected(scanner.lookAhead(), "Expected FROM or AS after PREPARE statement name")
    val statement = queryOrUpdate()
    PrepareStatement(name, statement, spanFrom(t))

  def executeStatement(): ExecuteStatement =
    val t    = consume(SqlToken.EXECUTE)
    val name = identifier()

    // Parse parameters - support both DuckDB style EXECUTE stmt(args) and Trino style EXECUTE stmt USING args
    val parameters: List[Expression] =
      scanner.lookAhead().token match
        case SqlToken.L_PAREN =>
          // DuckDB style: EXECUTE stmt(arg1, arg2) or EXECUTE stmt()
          consume(SqlToken.L_PAREN)
          val params =
            if scanner.lookAhead().token == SqlToken.R_PAREN then
              Nil // Empty parentheses: EXECUTE stmt()
            else
              expressionList()
          consume(SqlToken.R_PAREN)
          params
        case SqlToken.USING =>
          // Trino style: EXECUTE stmt USING arg1, arg2
          consume(SqlToken.USING)
          expressionList()
        case _ =>
          // No parameters
          Nil

    ExecuteStatement(name, parameters, spanFrom(t))

  def deallocateStatement(): DeallocateStatement =
    val t = consume(SqlToken.DEALLOCATE)
    // Optional PREPARE keyword (common in many dialects)
    consumeIfExist(SqlToken.PREPARE)
    val name = identifier()
    DeallocateStatement(name, spanFrom(t))

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
    val using = relationPrimary()
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

  def delete(): Delete =
    val t = consume(SqlToken.DELETE)
    consume(SqlToken.FROM)
    val target = relationPrimary()

    val filteredRelation =
      scanner.lookAhead().token match
        case SqlToken.WHERE =>
          consume(SqlToken.WHERE)
          val cond = expression()
          Filter(target, cond, spanFrom(t))
        case _ =>
          target

    def deleteExpr(x: Relation): Delete =
      x match
        case f: FilteringRelation =>
          deleteExpr(f.child)
        case r: TableRef =>
          Delete(filteredRelation, r.name, spanFrom(t))
        case f: FileScan =>
          Delete(filteredRelation, f.path, spanFrom(t))
        case other =>
          throw StatusCode
            .SYNTAX_ERROR
            .newException(
              s"delete statement can't have ${other.nodeName} operator",
              t.sourceLocation(using unit)
            )
    deleteExpr(filteredRelation)

  end delete

  def query(): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.VALUES =>
        values()
      case SqlToken.WITH =>
        // Handle WITH queries (CTEs)
        val (isRecursive, withStmts) = parseWithCTEs(t)
        val body                     = query()
        WithQuery(isRecursive, withStmts, body, spanFrom(t))
      case q if q.isQueryStart =>
        select()
      case SqlToken.L_PAREN =>
        consume(SqlToken.L_PAREN)
        val subQuery = query()
        consume(SqlToken.R_PAREN)
        queryRest(subQuery)
      case _ =>
        unexpected(t)

    end match

  end query

  def selectItems(): List[Attribute] =
    val t = scanner.lookAhead()
    t.token match
      case token if token.isQueryDelimiter =>
        Nil
      case t
          if t.tokenType == TokenType.Keyword && !SqlToken.literalStartKeywords.contains(t) &&
            !t.isNonReservedKeyword =>
        Nil
      case _ =>
        val item = selectItem()
        scanner.lookAhead().token match
          case SqlToken.COMMA =>
            consume(SqlToken.COMMA)
            item :: selectItems()
          case _ =>
            List(item)
    end match

  end selectItems

  def selectItem(): SingleColumn =
    def selectItemWithAlias(item: Expression): SingleColumn =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.AS =>
          consume(SqlToken.AS)
          // Check if this is UDTF column aliases: function(...) AS (col1, col2, ...)
          val nextToken = scanner.lookAhead()
          if nextToken.token == SqlToken.L_PAREN && item.isInstanceOf[FunctionApply] then
            consume(SqlToken.L_PAREN)
            val aliases = List.newBuilder[Identifier]
            aliases += identifier()
            while scanner.lookAhead().token == SqlToken.COMMA do
              consume(SqlToken.COMMA)
              aliases += identifier()
            consume(SqlToken.R_PAREN)
            // Update the FunctionApply with column aliases
            val f        = item.asInstanceOf[FunctionApply]
            val udtfCall = f.copy(columnAliases = Some(aliases.result()))
            SingleColumn(EmptyName, udtfCall, spanFrom(t))
          else
            // Regular AS alias
            val alias = identifier()
            SingleColumn(alias, item, spanFrom(t))
        case id if id.isIdentifier =>
          val alias = identifier()
          // Propagate the column name for a single column reference
          SingleColumn(alias, item, spanFrom(t))
        case SqlToken.DOUBLE_QUOTE_STRING =>
          val alias = identifier()
          SingleColumn(alias, item, spanFrom(t))
        case _ =>
          item match
            case i: Identifier =>
              // Propagate the column name for a single column reference
              SingleColumn(i, i, spanFrom(t))
            case _ =>
              SingleColumn(EmptyName, item, spanFrom(t))
      end match
    end selectItemWithAlias

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
        val g = groupBy(r)
        r = having(g)

        val p = Project(r, items, spanFrom(t))
        if isDistinct then
          r = Distinct(p, spanFrom(t))
        else
          r = p

//        g match
//            case g: GroupBy =>
//              val keyAttrs = g
//                .groupingKeys
//                .map { k =>
//                  SingleColumn(NameExpr.EmptyName, k.name, k.span)
//                }
//              Agg(r, keyAttrs ++ items, spanFrom(t))
//            case _ =>
//              Project(r, items, spanFrom(t))
        r = orderBy(r)
        r = limit(r)
        r = offset(r)
        r = hivePartitionClauses(r)
        r = queryRest(r)
        r
      case other =>
        unexpected(t)
    end match

  end select

  private def toSamplingSize(expr: Expression): SamplingSize =
    expr match
      case ArithmeticBinaryExpr(BinaryExprType.Modulus, percentageExpr, _, _) =>
        // Handle "10 % " as percentage
        percentageExpr match
          case LongLiteral(value, _, _) =>
            SamplingSize.Percentage(value.toDouble)
          case DoubleLiteral(value, _, _) =>
            SamplingSize.Percentage(value)
          case DecimalLiteral(value, _, _) =>
            SamplingSize.Percentage(value.toDouble)
          case _ =>
            SamplingSize.PercentageExpr(expr)
      case LongLiteral(value, _, _) =>
        // For TABLESAMPLE BERNOULLI/SYSTEM, integer literals are percentages
        SamplingSize.Percentage(value.toDouble)
      case DoubleLiteral(value, _, _) =>
        SamplingSize.Percentage(value)
      case DecimalLiteral(value, _, _) =>
        SamplingSize.Percentage(value.toDouble)
      case _ =>
        // For any other expression (parenthesized, arithmetic, etc.)
        SamplingSize.PercentageExpr(expr)

  private def handleTableSample(r: Relation): Relation =
    scanner.lookAhead().token match
      case SqlToken.TABLESAMPLE =>
        consume(SqlToken.TABLESAMPLE)
        val methodName = identifier() // e.g., BERNOULLI, SYSTEM
        consume(SqlToken.L_PAREN)
        val sizeExpr = expression() // percentage value or expression

        // Handle different expression types for sampling size
        val samplingSize = toSamplingSize(sizeExpr)

        consume(SqlToken.R_PAREN)

        // Convert method name to SamplingMethod
        val method =
          try
            SamplingMethod.valueOf(methodName.leafName.toLowerCase)
          catch
            case _: IllegalArgumentException =>
              unexpected(methodName)

        Sample(r, Some(method), samplingSize, spanFrom(r.span))
      case SqlToken.USING =>
        // Handle DuckDB USING SAMPLE syntax
        consume(SqlToken.USING)
        consume(SqlToken.SAMPLE)

        val sizeExpr = expression()

        // Check for optional keywords after the size expression
        val (samplingSize, samplingMethod) =
          scanner.lookAhead().token match
            case SqlToken.ROWS =>
              consume(SqlToken.ROWS)
              val rows =
                sizeExpr match
                  case LongLiteral(value, _, _) =>
                    value
                  case _ =>
                    unexpected(sizeExpr)
              (SamplingSize.Rows(rows), None)
            case SqlToken.PERCENT =>
              consume(SqlToken.PERCENT)
              val samplingSize = toSamplingSize(sizeExpr)
              (samplingSize, None)
            case SqlToken.L_PAREN =>
              // Handle reservoir(10%) syntax
              val methodName =
                sizeExpr match
                  case i: Identifier =>
                    i.leafName.toLowerCase
                  case _ =>
                    unexpected(sizeExpr)

              consume(SqlToken.L_PAREN)
              val expr         = expression()
              val samplingSize = toSamplingSize(expr)
              consume(SqlToken.R_PAREN)

              val method =
                try
                  SamplingMethod.valueOf(methodName)
                catch
                  case _: IllegalArgumentException =>
                    unexpected(sizeExpr)

              (samplingSize, Some(method))
            case _ =>
              // Default case: determine if it's rows or percentage based on the expression
              sizeExpr match
                case ArithmeticBinaryExpr(BinaryExprType.Modulus, percentageExpr, _, _) =>
                  val samplingSize = toSamplingSize(sizeExpr)
                  (samplingSize, None)
                case LongLiteral(value, _, _) =>
                  // Could be rows or percentage - default to rows for USING SAMPLE
                  (SamplingSize.Rows(value), None)
                case DoubleLiteral(value, _, _) =>
                  (SamplingSize.Percentage(value), None)
                case DecimalLiteral(value, _, _) =>
                  (SamplingSize.Percentage(value.toDouble), None)
                case _ =>
                  // For any other expression (parenthesized, arithmetic, etc.)
                  (SamplingSize.PercentageExpr(sizeExpr), None)

        Sample(r, samplingMethod, samplingSize, spanFrom(r.span))
      case _ =>
        r

  private def parseLateralViewRest(child: Relation, lateralToken: TokenData[SqlToken]): Relation =
    consume(SqlToken.VIEW)

    // Check for optional OUTER keyword
    val isOuter = consumeIfExist(SqlToken.OUTER)

    // Parse the generator function (e.g., explode(word2freq))
    val funcName = identifier()
    consume(SqlToken.L_PAREN)
    val args = expressionList()
    consume(SqlToken.R_PAREN)

    // Create the function expression
    val funcExpr = FunctionApply(
      funcName,
      args.map(expr => FunctionArg(None, expr, false, Nil, expr.span)),
      None,
      None,
      None,
      spanFrom(funcName.span)
    )

    // Parse table alias (e.g., t2)
    val tableAlias = identifier()

    // Parse AS keyword and column aliases (e.g., AS word, freq)
    val columnAliases =
      if consumeIfExist(SqlToken.AS) then
        parseIdentifierList()
      else
        Seq.empty

    LateralView(
      child = child,
      exprs = Seq(funcExpr),
      tableAlias = tableAlias,
      columnAliases = columnAliases,
      isOuter = isOuter,
      span = spanFrom(lateralToken)
    )

  end parseLateralViewRest

  private def relationRest(r: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.TABLESAMPLE =>
        // Handle TABLESAMPLE and continue with other relation operations
        val sampledR = handleTableSample(r)
        relationRest(sampledR)
      case SqlToken.LATERAL =>
        // Save token position for span tracking
        val lateralToken = consume(SqlToken.LATERAL)
        val nextToken    = scanner.lookAhead()
        if nextToken.token == SqlToken.VIEW then
          // Handle LATERAL VIEW
          val lateralView = parseLateralViewRest(r, lateralToken)
          relationRest(lateralView)
        else
          // Support for standalone LATERAL (subquery)
          consume(SqlToken.L_PAREN)
          val subQuery = query()
          consume(SqlToken.R_PAREN)
          relationRest(Lateral(subQuery, spanFrom(lateralToken)))
      case SqlToken.COMMA =>
        consume(SqlToken.COMMA)
        // Note: Parsing the rest as a new relation is important to build a left-deep plan
        val next = relation()
        relationRest(
          Join(JoinType.ImplicitJoin, r, next, NoJoinCriteria, asof = false, spanFrom(next.span))
        )
      case SqlToken.LEFT | SqlToken.RIGHT | SqlToken.INNER | SqlToken.FULL | SqlToken.CROSS |
          SqlToken.ASOF | SqlToken.JOIN =>
        relationRest(join(r))
      case SqlToken.UNION =>
        relationRest(union(r))
      case SqlToken.EXCEPT | SqlToken.INTERSECT =>
        relationRest(intersectOrExcept(r))
      case SqlToken.CLUSTER | SqlToken.DISTRIBUTE | SqlToken.SORT =>
        val hinted = hivePartitionClauses(r)
        relationRest(hinted)
      case _ =>
        r
    end match
  end relationRest

  def fromClause(): Relation =

    val t = scanner.lookAhead()
    t.token match
      case SqlToken.FROM =>
        consume(SqlToken.FROM)
        var r = relation()
        r = relationRest(r)
        r
      case d if d.isQueryDelimiter =>
        EmptyRelation(spanFrom(t))
      case SqlToken.WHERE | SqlToken.GROUP | SqlToken.HAVING | SqlToken.ORDER | SqlToken.LIMIT |
          SqlToken.OFFSET | SqlToken.UNION | SqlToken.INTERSECT | SqlToken.EXCEPT =>
        // SELECT without FROM clause, followed by WHERE/GROUP BY/HAVING/ORDER BY/LIMIT/etc.
        EmptyRelation(spanFrom(t))
      case _ =>
        unexpected(t)

  end fromClause

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
        GroupBy(input, items, spanFrom(t))
      case _ =>
        input

  def groupByItemList(): List[GroupingKey] =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.GROUPING =>
        consume(SqlToken.GROUPING)
        consume(SqlToken.SETS)
        consume(SqlToken.L_PAREN)
        val sets = parseGroupingSets()
        consume(SqlToken.R_PAREN)
        GroupingSets(sets, spanFrom(t)) :: groupByItemList()
      case SqlToken.CUBE =>
        consume(SqlToken.CUBE)
        consume(SqlToken.L_PAREN)
        val keys =
          if scanner.lookAhead().token != SqlToken.R_PAREN then
            parseGroupingKeyList()
          else
            Nil
        consume(SqlToken.R_PAREN)
        Cube(keys, spanFrom(t)) :: groupByItemList()
      case SqlToken.ROLLUP =>
        consume(SqlToken.ROLLUP)
        consume(SqlToken.L_PAREN)
        val keys =
          if scanner.lookAhead().token != SqlToken.R_PAREN then
            parseGroupingKeyList()
          else
            Nil
        consume(SqlToken.R_PAREN)
        Rollup(keys, spanFrom(t)) :: groupByItemList()
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
      case e if e.isQueryDelimiter =>
        Nil
      case _ =>
        // expression only
        val e   = expression()
        val key = UnresolvedGroupingKey(NameExpr.EmptyName, e, e.span)
        key :: groupByItemList()

    end match

  end groupByItemList

  def parseGroupingSets(): List[List[GroupingKey]] =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.L_PAREN =>
        consume(SqlToken.L_PAREN)
        val keys =
          if scanner.lookAhead().token == SqlToken.R_PAREN then
            // Empty grouping set ()
            List.empty[GroupingKey]
          else
            parseGroupingKeyList()
        consume(SqlToken.R_PAREN)
        val remaining =
          if scanner.lookAhead().token == SqlToken.COMMA then
            consume(SqlToken.COMMA)
            parseGroupingSets()
          else
            Nil
        keys :: remaining
      case _ =>
        Nil

  def parseGroupingKeyList(): List[GroupingKey] =
    val t = scanner.lookAhead()
    val key =
      t.token match
        case id if id.isIdentifier =>
          val item = selectItem()
          UnresolvedGroupingKey(item.nameExpr, item.expr, spanFrom(t))
        case _ =>
          // expression only
          val e = expression()
          UnresolvedGroupingKey(NameExpr.EmptyName, e, e.span)

    val remaining =
      if scanner.lookAhead().token == SqlToken.COMMA then
        consume(SqlToken.COMMA)
        parseGroupingKeyList()
      else
        Nil
    key :: remaining

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
        Limit(input, LongLiteral(limit.str.toLong, limit.str, limit.span), spanFrom(t))
      case _ =>
        input

  def offset(input: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.OFFSET =>
        consume(SqlToken.OFFSET)
        val offset = consume(SqlToken.INTEGER_LITERAL)
        Offset(input, LongLiteral(offset.str.toLong, offset.str, offset.span), spanFrom(t))
      case _ =>
        input

  def values(): Values =
    def value(): Expression =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.L_PAREN =>
          consume(SqlToken.L_PAREN)
          val values = expressionList()
          consume(SqlToken.R_PAREN)
          ArrayConstructor(values, spanFrom(t))
        case _ =>
          expression()

    def valueList(): List[Expression] =
      @tailrec
      def loop(acc: List[Expression]): List[Expression] =
        val v = value()
        scanner.lookAhead().token match
          case SqlToken.COMMA =>
            consume(SqlToken.COMMA)
            loop(v :: acc)
          case _ =>
            (v :: acc).reverse
      loop(Nil)

    val t = scanner.lookAhead()
    t.token match
      case SqlToken.VALUES =>
        consume(t.token)
        val values = valueList()
        Values(values, EmptyRelationType, spanFrom(t))
      case _ =>
        unexpected(t)

  end values

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

    // Handle multi-word SHOW commands
    name.leafName.toLowerCase match
      case "create" =>
        // SHOW CREATE VIEW viewname
        val next = identifier()
        if next.leafName.toLowerCase == "view" then
          val viewName = qualifiedName()
          Show(ShowType.createView, viewName, spanFrom(t))
        else
          unexpected(next, s"Expected VIEW after CREATE, but found: ${next.leafName}")
      case "functions" =>
        Show(ShowType.functions, EmptyName, spanFrom(t))
      case _ =>
        // Handle existing single-word SHOW commands
        try
          val tpe = ShowType.valueOf(name.leafName.toLowerCase)
          tpe match
            case ShowType.databases | ShowType.tables | ShowType.schemas =>
              val in = inExpr()
              Show(tpe, in, spanFrom(t))
            case ShowType.catalogs =>
              Show(ShowType.catalogs, EmptyName, spanFrom(t))
            case ShowType.columns =>
              // Handle "SHOW COLUMNS FROM table" syntax
              consume(SqlToken.FROM)
              val tableName = qualifiedName()
              Show(ShowType.columns, tableName, spanFrom(t))
            case _ =>
              unexpected(name)
        catch
          case e: IllegalArgumentException =>
            unexpected(name, s"Unknown SHOW type: ${name.leafName}")
    end match

  end show

  def use(): UseSchema =
    val t      = consume(SqlToken.USE)
    val schema = qualifiedName()
    UseSchema(schema, spanFrom(t))

  def expressionList(): List[Expression] =
    @tailrec
    def next(acc: List[Expression]): List[Expression] =
      val e = expression()
      scanner.lookAhead().token match
        case SqlToken.COMMA =>
          consume(SqlToken.COMMA)
          next(e :: acc)
        case _ =>
          (e :: acc).reverse

    next(Nil)

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
        val e = valueExpression()
        booleanExpressionRest(Not(e, spanFrom(t)))
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
        case token
            if token == SqlToken.DIV_INT || (token.isIdentifier && t.str.toUpperCase == "DIV") =>
          // Support DIV keyword (Hive/MySQL) and // operator (DuckDB) for integer division
          consumeToken()
          val right = valueExpression()
          ArithmeticBinaryExpr(BinaryExprType.DivideInt, expr, right, spanFrom(t))
        case SqlToken.EQ =>
          consume(SqlToken.EQ)
          val right = valueExpression()
          Eq(expr, right, spanFrom(t))
        case SqlToken.NEQ | SqlToken.NEQ2 =>
          consumeToken()
          val right = valueExpression()
          NotEq(expr, right, spanFrom(t))
        case SqlToken.IS =>
          consume(SqlToken.IS)
          scanner.lookAhead().token match
            case SqlToken.NOT =>
              consume(SqlToken.NOT)
              scanner.lookAhead().token match
                case SqlToken.NULL =>
                  consume(SqlToken.NULL)
                  IsNotNull(expr, spanFrom(t))
                case _ =>
                  val right = valueExpression()
                  NotEq(expr, right, spanFrom(t))
            case SqlToken.NULL =>
              consume(SqlToken.NULL)
              IsNull(expr, spanFrom(t))
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
          val right  = valueExpression()
          val escape = parseEscapeClause()
          Like(expr, right, escape, spanFrom(t))
        case SqlToken.RLIKE =>
          consume(SqlToken.RLIKE)
          val right = valueExpression()
          RLike(expr, right, spanFrom(t))
        case SqlToken.NOT =>
          consume(SqlToken.NOT)
          val t2 = scanner.lookAhead()
          t2.token match
            case SqlToken.LIKE =>
              consume(SqlToken.LIKE)
              val right  = valueExpression()
              val escape = parseEscapeClause()
              NotLike(expr, right, escape, spanFrom(t))
            case SqlToken.RLIKE =>
              consume(SqlToken.RLIKE)
              val right = valueExpression()
              NotRLike(expr, right, spanFrom(t))
            case SqlToken.IN =>
              consume(SqlToken.IN)
              val values = inExprList()
              NotIn(expr, values, spanFrom(t))
            case _ =>
              unexpected(t2)
        case SqlToken.BETWEEN =>
          consume(SqlToken.BETWEEN)
          val start = valueExpression()
          consume(SqlToken.AND)
          val end = valueExpression()
          Between(expr, start, end, spanFrom(t))
        case _ =>
          expr
      end match
    end valueExpressionRest

    val t = scanner.lookAhead()
    val expr =
      t.token match
        case SqlToken.PLUS =>
          consume(SqlToken.PLUS)
          val v = valueExpression()
          ArithmeticUnaryExpr(Sign.Positive, v, spanFrom(t))
        case SqlToken.MINUS =>
          consume(SqlToken.MINUS)
          val v = valueExpression()
          ArithmeticUnaryExpr(Sign.Negative, v, spanFrom(t))
        case _ =>
          primaryExpression()
    valueExpressionRest(expr)

  end valueExpression

  def inExprList(): List[Expression] =
    @tailrec
    def rest(acc: List[Expression]): List[Expression] =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.R_PAREN =>
          consume(SqlToken.R_PAREN)
          acc.reverse
        case SqlToken.COMMA =>
          consume(SqlToken.COMMA)
          rest(acc)
        case _ =>
          val e = valueExpression()
          rest(e :: acc)

    consume(SqlToken.L_PAREN)
    rest(Nil)

  def functionArgs(): List[FunctionArg] =
    @tailrec
    def parseArgs(acc: List[FunctionArg]): List[FunctionArg] =
      val arg = functionArg()
      scanner.lookAhead().token match
        case SqlToken.COMMA =>
          consume(SqlToken.COMMA)
          parseArgs(arg :: acc)
        case _ =>
          (arg :: acc).reverse

    val t = scanner.lookAhead()
    t.token match
      case SqlToken.R_PAREN =>
        // ok
        Nil
      case _ =>
        parseArgs(Nil)

  def jsonParams(): List[JsonParam] =
    @tailrec
    def parseParams(acc: List[JsonParam]): List[JsonParam] =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.R_PAREN | SqlToken.NULL | SqlToken.ABSENT | SqlToken.WITH | SqlToken
              .WITHOUT =>
          // closing paren or object modifier start keywords
          acc.reverse
        case _ =>
          // Simple alternating key-value syntax json_object(k1, v1, k2, v2, ...)
          val key = expression()
          val t2  = scanner.lookAhead()
          t2.token match
            case SqlToken.COMMA =>
              consume(SqlToken.COMMA)
              val value = expression()
              consumeIfExist(SqlToken.COMMA)
              parseParams(JsonParam(key, value, spanFrom(key.span)) :: acc)
            case other =>
              unexpected(t2)

    val t = scanner.lookAhead()
    t.token match
      case SqlToken.R_PAREN | SqlToken.NULL | SqlToken.ABSENT | SqlToken.WITH | SqlToken.WITHOUT =>
        // closing paren or object modifier start keywords
        Nil
      case _ =>
        // Check if this is KEY...VALUE syntax
        scanner.lookAhead().token match
          case SqlToken.KEY =>
            // KEY...VALUE syntax - parse KEY expression VALUE expression pairs
            parseJsonKeyValuePairs()
          case _ =>
            parseParams(Nil)

  end jsonParams

  private def parseJsonKeyValuePairs(): List[JsonParam] =
    @tailrec
    def parsePairs(acc: List[JsonParam]): List[JsonParam] =
      // Consume KEY
      consume(SqlToken.KEY)
      val keyExpr = expression()

      // Expect VALUE
      scanner.lookAhead().token match
        case SqlToken.VALUE =>
          consume(SqlToken.VALUE)
          val valueExpr = expression()
          val jsonParam = JsonParam(keyExpr, valueExpr, spanFrom(keyExpr.span))

          // Check for more KEY...VALUE pairs or modifiers
          scanner.lookAhead().token match
            case SqlToken.COMMA =>
              consume(SqlToken.COMMA)
              scanner.lookAhead().token match
                case SqlToken.KEY =>
                  // Another KEY...VALUE pair
                  parsePairs(jsonParam :: acc)
                case _ =>
                  // After a comma, we must have another KEY...VALUE pair
                  unexpected(
                    scanner.lookAhead(),
                    "Expected KEY after a comma in JSON_OBJECT arguments"
                  )
            case _ =>
              (jsonParam :: acc).reverse
        case _ =>
          unexpected(scanner.lookAhead(), "Expected VALUE after KEY")
    end parsePairs

    parsePairs(Nil)

  end parseJsonKeyValuePairs

  def parseJsonObjectModifiers(): List[JsonObjectModifier] =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.R_PAREN =>
        Nil
      case SqlToken.NULL =>
        consume(SqlToken.NULL)
        consume(SqlToken.ON)
        consume(SqlToken.NULL)
        JsonObjectModifier.NULL_ON_NULL :: parseJsonObjectModifiers()
      case SqlToken.ABSENT =>
        consume(SqlToken.ABSENT)
        consume(SqlToken.ON)
        consume(SqlToken.NULL)
        JsonObjectModifier.ABSENT_ON_NULL :: parseJsonObjectModifiers()
      case SqlToken.WITH =>
        consume(SqlToken.WITH)
        consume(SqlToken.UNIQUE)
        scanner.lookAhead().token match
          case SqlToken.KEYS | SqlToken.KEY =>
            consumeToken()
            JsonObjectModifier.WITH_UNIQUE_KEYS :: parseJsonObjectModifiers()
          case _ =>
            unexpected(scanner.lookAhead(), "Expected UNIQUE KEYS or UNIQUE KEY")
      case SqlToken.WITHOUT =>
        consume(SqlToken.WITHOUT)
        consume(SqlToken.UNIQUE)
        scanner.lookAhead().token match
          case SqlToken.KEYS | SqlToken.KEY =>
            consumeToken()
            JsonObjectModifier.WITHOUT_UNIQUE_KEYS :: parseJsonObjectModifiers()
          case _ =>
            unexpected(scanner.lookAhead(), "Expected UNIQUE KEYS or UNIQUE KEY")
      case _ =>
        Nil

    end match

  end parseJsonObjectModifiers

  def functionArg(): FunctionArg =
    val t = scanner.lookAhead()

    // Parse the main argument part (DISTINCT, named arg, or expression)
    val (name, expr, isDistinct) =
      scanner.lookAhead().token match
        case SqlToken.DISTINCT =>
          consume(SqlToken.DISTINCT)
          val e = expression()
          (None, e, true)
        case _ =>
          val nameOrArg = expression()
          nameOrArg match
            // Check for named argument `arg = value`
            case i: Identifier if scanner.lookAhead().token == SqlToken.EQ =>
              consume(SqlToken.EQ)
              val e = expression()
              (Some(Name.termName(i.leafName)), e, false)
            // This case handles `arg = value` when it's parsed as Eq expression.
            case Eq(i: Identifier, v: Expression, _) =>
              (Some(Name.termName(i.leafName)), v, false)
            // Positional argument
            case _ =>
              (None, nameOrArg, false)

    // Check for ORDER BY clause within the function argument
    val orderByList =
      scanner.lookAhead().token match
        case SqlToken.ORDER =>
          consume(SqlToken.ORDER)
          consume(SqlToken.BY)
          sortItems()
        case _ =>
          Nil

    FunctionArg(name, expr, isDistinct, orderByList, spanFrom(t))

  end functionArg

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
              val f = FunctionApply(sel, args, w, None, None, spanFrom(t))
              primaryExpressionRest(f)
            case _ =>
              primaryExpressionRest(DotRef(expr, next, DataType.UnknownType, spanFrom(t)))
        case SqlToken.L_PAREN =>
          def functionApply(functionName: Expression) =
            consume(SqlToken.L_PAREN)
            // Check if this is JSON_OBJECT to use specialized parsing
            val isJsonObject =
              functionName match
                case id: Identifier =>
                  id.unquotedValue.equalsIgnoreCase("JSON_OBJECT")
                case other =>
                  false

            if isJsonObject then
              val params              = jsonParams()
              val jsonObjectModifiers = parseJsonObjectModifiers()
              val jsonObj = JsonObjectConstructor(params, jsonObjectModifiers, spanFrom(t))
              consume(SqlToken.R_PAREN)
              primaryExpressionRest(jsonObj)
            else
              val args = functionArgs()
              consume(SqlToken.R_PAREN)
              // Global function call
              val w = window()
              // Check for FILTER clause after function arguments
              val filter =
                if scanner.lookAhead().token == SqlToken.FILTER then
                  consume(SqlToken.FILTER)
                  consume(SqlToken.L_PAREN)
                  consume(SqlToken.WHERE)
                  val filterExpr = expression()
                  consume(SqlToken.R_PAREN)
                  Some(filterExpr)
                else
                  None
              val f = FunctionApply(functionName, args, w, filter, None, spanFrom(t))
              primaryExpressionRest(f)
          end functionApply

          expr match
            case n: NameExpr =>
              functionApply(n)
            case _ =>
              unexpected(expr)
        case SqlToken.L_BRACKET =>
          consume(SqlToken.L_BRACKET)
          val index = expression()
          consume(SqlToken.R_BRACKET)
          primaryExpressionRest(ArrayAccess(expr, index, spanFrom(t)))
        case SqlToken.R_ARROW if expr.isIdentifier =>
          consume(SqlToken.R_ARROW)
          // In Trino, lambda bodies are full expressions (e.g., x IS NOT NULL, x + y)
          val body = expression()
          primaryExpressionRest(
            LambdaExpr(args = List(expr.asInstanceOf[Identifier]), body, spanFrom(t))
          )
        case SqlToken.OVER =>
          window() match
            case Some(w) =>
              WindowApply(expr, w, spanFrom(t))
            case _ =>
              expr
        case SqlToken.DOUBLE_COLON =>
          consume(SqlToken.DOUBLE_COLON)
          val tpe = typeName()
          primaryExpressionRest(Cast(expr, tpe, tryCast = false, spanFrom(t)))
        case SqlToken.AT =>
          consume(SqlToken.AT)
          consume(SqlToken.TIME)
          consume(SqlToken.ZONE)
          val timezone = valueExpression()
          primaryExpressionRest(AtTimeZone(expr, timezone, spanFrom(t)))
        case _ =>
          expr
      end match
    end primaryExpressionRest

    def parseFunctionCallOrLiteral(
        createLiteral: (TokenData[SqlToken], Literal) => GenericLiteral
    ): Expression =
      val keywordToken = consumeToken() // consume the DATE/TIME/TIMESTAMP token
      val nextToken    = scanner.lookAhead().token
      if nextToken == SqlToken.L_PAREN then
        // Treat as a function call, e.g., date(...)
        val identifier = UnquotedIdentifier(keywordToken.str, spanFrom(keywordToken))
        primaryExpressionRest(identifier)
      else if keywordToken.token.isNonReservedKeyword &&
        nextToken != SqlToken.SINGLE_QUOTE_STRING && nextToken != SqlToken.TRIPLE_QUOTE_STRING
      then
        // Non-reserved keyword used as identifier (e.g., column name)
        val identifier = UnquotedIdentifier(keywordToken.str, spanFrom(keywordToken))
        primaryExpressionRest(identifier)
      else
        // Treat as a literal prefix, e.g., DATE '...'
        val lit = literal()
        createLiteral(keywordToken, lit)

    val t = scanner.lookAhead()
    val expr =
      t.token match
        case SqlToken.NULL | SqlToken.TRUE | SqlToken.FALSE | SqlToken.INTEGER_LITERAL | SqlToken
              .DOUBLE_LITERAL | SqlToken.FLOAT_LITERAL | SqlToken.DECIMAL_LITERAL | SqlToken
              .EXP_LITERAL | SqlToken.SINGLE_QUOTE_STRING | SqlToken.TRIPLE_QUOTE_STRING =>
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
        case SqlToken.EXISTS =>
          consume(SqlToken.EXISTS)
          consume(SqlToken.L_PAREN)
          val subQuery = query()
          consume(SqlToken.R_PAREN)
          Exists(SubQueryExpression(subQuery, subQuery.span), spanFrom(t))
        case q if q.isQueryStart =>
          val subQuery = query()
          SubQueryExpression(subQuery, spanFrom(t))
        case SqlToken.CAST | SqlToken.TRY_CAST =>
          val isTryCast: Boolean = t.token == SqlToken.TRY_CAST
          consumeToken()
          consume(SqlToken.L_PAREN)
          val e = expression()
          consume(SqlToken.AS)
          val dt = typeName()
          consume(SqlToken.R_PAREN)
          Cast(e, dt, isTryCast, spanFrom(t))
        case SqlToken.TRIM =>
          consumeToken()
          consume(SqlToken.L_PAREN)

          // Parse optional LEADING/TRAILING/BOTH
          val trimType =
            scanner.lookAhead().token match
              case SqlToken.LEADING =>
                consumeToken()
                Some("LEADING")
              case SqlToken.TRAILING =>
                consumeToken()
                Some("TRAILING")
              case SqlToken.BOTH =>
                consumeToken()
                Some("BOTH")
              case _ =>
                None

          // Check for the pattern: [chars] FROM string
          // or just: string
          // Special case: if we have trimType and next is FROM, there's no chars to trim
          val (trimChars, trimFrom) =
            if trimType.isDefined && scanner.lookAhead().token == SqlToken.FROM then
              consume(SqlToken.FROM)
              val fromExpr = expression()
              (None, fromExpr)
            else
              val firstExpr = expression()
              scanner.lookAhead().token match
                case SqlToken.FROM =>
                  consume(SqlToken.FROM)
                  val fromExpr = expression()
                  (Some(firstExpr), fromExpr)
                case _ =>
                  // No FROM clause, firstExpr is the string to trim
                  (None, firstExpr)

          consume(SqlToken.R_PAREN)

          // Create a FunctionApply with the appropriate arguments
          val funcName = UnquotedIdentifier("trim", spanFrom(t))
          val args =
            (trimType, trimChars) match
              case (Some(tType), Some(chars)) =>
                // TRIM(LEADING/TRAILING/BOTH chars FROM string)
                List(
                  FunctionArg(None, UnquotedIdentifier(tType, chars.span), false, Nil, chars.span),
                  FunctionArg(None, chars, false, Nil, chars.span),
                  FunctionArg(Some(TermName("from")), trimFrom, false, Nil, trimFrom.span)
                )
              case (Some(tType), None) =>
                // TRIM(LEADING/TRAILING/BOTH FROM string)
                List(
                  FunctionArg(
                    None,
                    UnquotedIdentifier(tType, trimFrom.span),
                    false,
                    Nil,
                    trimFrom.span
                  ),
                  FunctionArg(Some(TermName("from")), trimFrom, false, Nil, trimFrom.span)
                )
              case (None, Some(chars)) =>
                // TRIM(chars FROM string)
                List(
                  FunctionArg(None, chars, false, Nil, chars.span),
                  FunctionArg(Some(TermName("from")), trimFrom, false, Nil, trimFrom.span)
                )
              case (None, None) =>
                // TRIM(string)
                List(FunctionArg(None, trimFrom, false, Nil, trimFrom.span))

          FunctionApply(funcName, args, None, None, None, spanFrom(t))
        case SqlToken.L_PAREN =>
          consume(SqlToken.L_PAREN)
          val t2 = scanner.lookAhead()
          t2.token match
            case q if q.isQueryStart =>
              val subQuery = query()
              consume(SqlToken.R_PAREN)
              SubQueryExpression(subQuery, spanFrom(t))
            case SqlToken.R_PAREN =>
              // Empty parameter list for lambda: () -> expression
              consume(SqlToken.R_PAREN)
              scanner.lookAhead().token match
                case SqlToken.R_ARROW =>
                  consume(SqlToken.R_ARROW)
                  val body = expression()
                  LambdaExpr(Nil, body, spanFrom(t))
                case _ =>
                  // Not a lambda, just empty parentheses - should not happen in normal SQL
                  ParenthesizedExpression(NullLiteral(spanFrom(t2)), spanFrom(t))
            case id if id.isIdentifier || id == SqlToken.UNDERSCORE =>
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
                  // Lambda: parameters must be identifiers, body is a general expression
                  consume(SqlToken.R_ARROW)
                  val body = expression()
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
        case SqlToken.DATE =>
          parseFunctionCallOrLiteral { (token, lit) =>
            GenericLiteral(DataType.DateType, lit.stringValue, spanFrom(token))
          }
        case SqlToken.TIME =>
          parseFunctionCallOrLiteral { (token, lit) =>
            GenericLiteral(
              DataType.TimestampType(DataType.TimestampField.TIME, true),
              lit.stringValue,
              spanFrom(token)
            )
          }
        case SqlToken.TIMESTAMP =>
          parseFunctionCallOrLiteral { (token, lit) =>
            GenericLiteral(
              DataType.TimestampType(DataType.TimestampField.TIMESTAMP, true),
              lit.stringValue,
              spanFrom(token)
            )
          }
        case SqlToken.DECIMAL =>
          consume(SqlToken.DECIMAL)
          val i = literal()
          DecimalLiteral(
            i.stringValue.stripPrefix("'").stripSuffix("'"),
            s"DECIMAL ${i.stringValue}",
            spanFrom(t)
          )
        case SqlToken.JSON =>
          consume(SqlToken.JSON)
          val i = literal()
          JsonLiteral(i.unquotedValue, spanFrom(t))
        case SqlToken.INTERVAL =>
          interval()
        case SqlToken.EXTRACT =>
          extractExpression()
        case id if id.isIdentifier =>
          identifier()
        case SqlToken.DOUBLE_QUOTE_STRING =>
          identifier()
        case SqlToken.STAR =>
          identifier()
        case SqlToken.UNDERSCORE =>
          identifier()
        case SqlToken.QUESTION =>
          consume(SqlToken.QUESTION)
          // Use a counter to track the position of '?' parameters
          questionMarkParamIndex += 1
          Parameter(questionMarkParamIndex, spanFrom(t))
        case SqlToken.DOLLAR =>
          consume(SqlToken.DOLLAR)
          val nextToken = scanner.lookAhead()
          nextToken.token match
            case SqlToken.INTEGER_LITERAL =>
              val indexToken = consumeToken()
              val index      = indexToken.str.toInt
              Parameter(index, spanFrom(t))
            case id if id.isIdentifier =>
              val nameToken = consumeToken()
              // Named parameter for prepared statements (e.g., $name_param)
              NamedParameter(nameToken.str, spanFrom(t))
            case _ =>
              unexpected(nextToken)
        case _ =>
          unexpected(t)

    primaryExpressionRest(expr)

  end primaryExpression

  def array(): ArrayConstructor =
    consumeIfExist(SqlToken.ARRAY)
    // Support both ARRAY[...] and ARRAY(...) syntax for Hive compatibility
    val t = scanner.lookAhead()
    val (startToken, endToken) =
      t.token match
        case SqlToken.L_PAREN =>
          (consume(SqlToken.L_PAREN), SqlToken.R_PAREN)
        case _ =>
          // Default to bracket syntax
          (consume(SqlToken.L_BRACKET), SqlToken.R_BRACKET)

    val elements = List.newBuilder[Expression]

    def nextElement: Unit =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.COMMA =>
          consume(SqlToken.COMMA)
          nextElement
        case token if token == endToken =>
        // ok
        case _ =>
          elements += expression()
          nextElement

    nextElement
    consume(endToken)
    ArrayConstructor(elements.result(), spanFrom(startToken))

  end array

  def map(): Expression =
    val t = consume(SqlToken.MAP)

    def parseMapLiteral(): MapValue =
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

      consume(SqlToken.L_BRACE)
      nextEntry
      consume(SqlToken.R_BRACE)
      MapValue(entries.result(), spanFrom(t))

    def parseMapFunction(): FunctionApply =
      consume(SqlToken.L_PAREN)
      // Support both two-array form: map(keys, values)
      // and variadic key-value pairs: map(k1, v1, k2, v2, ...)
      val args = List.newBuilder[FunctionArg]

      if scanner.lookAhead().token != SqlToken.R_PAREN then
        var continueParsing = true
        while continueParsing do
          val e = expression()
          args += FunctionArg(None, e, isDistinct = false, orderBy = Nil, spanFrom(t))
          if scanner.lookAhead().token == SqlToken.COMMA then
            consume(SqlToken.COMMA)
          else
            continueParsing = false

      consume(SqlToken.R_PAREN)
      FunctionApply(
        UnquotedIdentifier("map", spanFrom(t)),
        args.result(),
        None,
        None,
        None,
        spanFrom(t)
      )

    scanner.lookAhead().token match
      case SqlToken.L_BRACE =>
        parseMapLiteral()
      case SqlToken.L_PAREN =>
        parseMapFunction()
      case _ =>
        unexpected(scanner.lookAhead(), "Expected '{' or '(' after MAP")

  end map

  def interval(): IntervalLiteral =
    // interval : INTERVAL sign = (PLUS | MINUS) ? str intervalField (TO intervalField)?
    // intervalField: YEAR | MONTH | DAY | HOUR | MINUTE | SECOND;

    val t = consume(SqlToken.INTERVAL)

    val sign =
      scanner.lookAhead().token match
        case SqlToken.PLUS =>
          consume(SqlToken.PLUS)
          Sign.Positive
        case SqlToken.MINUS =>
          consume(SqlToken.MINUS)
          Sign.Negative
        case _ =>
          Sign.NoSign

    val value = literal()

    def intervalField(): IntervalField =
      val t   = consumeToken()
      val opt = IntervalField.unapply(t.str)
      opt.getOrElse(unexpected(t))

    val f1 = intervalField()
    scanner.lookAhead().token match
      case SqlToken.TO =>
        consume(SqlToken.TO)
        val f2 = intervalField()
        IntervalLiteral(value.unquotedValue, sign, f1, Some(f2), spanFrom(t))
      case _ =>
        IntervalLiteral(value.unquotedValue, sign, f1, None, spanFrom(t))
  end interval

  def extractExpression(): Extract =
    val t = consume(SqlToken.EXTRACT)
    consume(SqlToken.L_PAREN)
    val fieldNode = identifier()
    val field = IntervalField
      .unapply(fieldNode.leafName)
      .getOrElse {
        throw StatusCode
          .SYNTAX_ERROR
          .newException(
            s"Unknown extract field: ${fieldNode.leafName}",
            fieldNode.sourceLocationOfCompilationUnit
          )
      }
    consume(SqlToken.FROM)
    val expr = expression()
    consume(SqlToken.R_PAREN)
    Extract(field, expr, spanFrom(t))

  def literal(): Literal =
    def removeUnderscore(s: String): String = s.replaceAll("_", "")

    val t = consumeToken()
    t.token match
      case SqlToken.NULL =>
        NullLiteral(spanFrom(t))
      case SqlToken.TRUE =>
        TrueLiteral(spanFrom(t))
      case SqlToken.FALSE =>
        FalseLiteral(spanFrom(t))
      case SqlToken.INTEGER_LITERAL =>
        LongLiteral(removeUnderscore(t.str).toLong, t.str, spanFrom(t))
      case SqlToken.DOUBLE_LITERAL =>
        DoubleLiteral(t.str.toDouble, t.str, spanFrom(t))
      case SqlToken.FLOAT_LITERAL =>
        DoubleLiteral(t.str.toFloat, t.str, spanFrom(t))
      case SqlToken.DECIMAL_LITERAL =>
        DecimalLiteral(removeUnderscore(t.str), t.str, spanFrom(t))
      case SqlToken.EXP_LITERAL =>
        DecimalLiteral(t.str, t.str, spanFrom(t))
      case SqlToken.SINGLE_QUOTE_STRING =>
        SingleQuoteString(t.str, spanFrom(t))
      case SqlToken.DOUBLE_QUOTE_STRING =>
        DoubleQuoteString(t.str, spanFrom(t))
      case SqlToken.TRIPLE_QUOTE_STRING =>
        TripleQuoteString(t.str, spanFrom(t))
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

    def nullOrdering(): Option[NullOrdering] =
      scanner.lookAhead().token match
        case SqlToken.NULLS =>
          consume(SqlToken.NULLS)
          scanner.lookAhead().token match
            case SqlToken.FIRST =>
              consume(SqlToken.FIRST)
              Some(NullOrdering.NullIsFirst)
            case SqlToken.LAST =>
              consume(SqlToken.LAST)
              Some(NullOrdering.NullIsLast)
            case _ =>
              None
        case _ =>
          None

    def items(): List[SortItem] =
      val expr      = expression()
      val order     = sortOrder()
      val nullOrder = nullOrdering()
      val item      = SortItem(expr, order, nullOrder, spanFrom(expr.span))
      scanner.lookAhead().token match
        case SqlToken.COMMA =>
          consume(SqlToken.COMMA)
          item :: items()
        case _ =>
          List(item)

    items()

  end sortItems

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

    def partitionBy(): List[Expression] =
      scanner.lookAhead().token match
        case SqlToken.PARTITION =>
          consume(SqlToken.PARTITION)
          consume(SqlToken.BY)
          partitionKeys()
        case _ =>
          Nil

    def orderBy(): List[SortItem] =
      scanner.lookAhead().token match
        case SqlToken.ORDER =>
          consume(SqlToken.ORDER)
          consume(SqlToken.BY)
          sortItems()
        case _ =>
          Nil

    def windowFrame(): Option[WindowFrame] =
      def bracketWindowFrame(): WindowFrame =
        val t = consume(SqlToken.L_BRACKET)
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
        WindowFrame(FrameType.RowsFrame, frameStart, frameEnd, spanFrom(t))
      end bracketWindowFrame

      def frameBound(): FrameBound =
        val t = scanner.lookAhead()
        t.token match
          case SqlToken.UNBOUNDED =>
            consume(SqlToken.UNBOUNDED)
            scanner.lookAhead().token match
              case SqlToken.PRECEDING =>
                consume(SqlToken.PRECEDING)
                FrameBound.UnboundedPreceding
              case SqlToken.FOLLOWING =>
                consume(SqlToken.FOLLOWING)
                FrameBound.UnboundedFollowing
              case _ =>
                unexpected(t)
          case SqlToken.CURRENT =>
            consume(SqlToken.CURRENT)
            scanner.lookAhead().token match
              case SqlToken.ROW =>
                consume(SqlToken.ROW)
                FrameBound.CurrentRow
              case SqlToken.ROWS =>
                consume(SqlToken.ROWS)
                FrameBound.CurrentRow
              case _ =>
                unexpected(t)
          case _ =>
            val t = scanner.lookAhead()
            val bound: Long =
              expression() match
                case l: LongLiteral =>
                  l.value
                case ArithmeticUnaryExpr(sign, l: LongLiteral, _) =>
                  sign match
                    case Sign.NoSign | Sign.Positive =>
                      l.value
                    case Sign.Negative =>
                      -l.value
                case _ =>
                  unexpected(t)
            scanner.lookAhead().token match
              case SqlToken.PRECEDING =>
                consume(SqlToken.PRECEDING)
                FrameBound.Preceding(bound)
              case SqlToken.FOLLOWING =>
                consume(SqlToken.FOLLOWING)
                FrameBound.Following(bound)
              case _ =>
                unexpected(t)
        end match
      end frameBound

      def sqlWindowFrame(): WindowFrame =
        val t          = scanner.lookAhead()
        val hasBetween = consumeIfExist(SqlToken.BETWEEN)
        val start      = frameBound()
        if hasBetween then
          consume(SqlToken.AND)
          val end = frameBound()
          WindowFrame(FrameType.RowsFrame, start, end, spanFrom(t))
        else
          WindowFrame(FrameType.RowsFrame, start, FrameBound.CurrentRow, spanFrom(t))

      val t = scanner.lookAhead()
      t.token match
        case SqlToken.ROWS | SqlToken.RANGE =>
          consumeToken()
          val t = scanner.lookAhead()
          t.token match
            case SqlToken.L_BRACKET =>
              Some(bracketWindowFrame())
            case SqlToken.BETWEEN | SqlToken.UNBOUNDED | SqlToken.CURRENT =>
              Some(sqlWindowFrame())
            case _ =>
              unexpected(t)
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

  def relation(): Relation =
    val r = relationPrimary()
    scanner.lookAhead().token match
      case SqlToken.AS =>
        consume(SqlToken.AS)
        tableAlias(r)
      case id if id.isIdentifier || id == SqlToken.DOUBLE_QUOTE_STRING =>
        tableAlias(r)
      case _ =>
        r

  end relation

  def relationPrimary(): Relation =
    val t = scanner.lookAhead()
    val r =
      t.token match
        case id if id.isIdentifier =>
          val name = qualifiedName()
          TableRef(name, spanFrom(t))
        case SqlToken.DOUBLE_QUOTE_STRING =>
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
          val t2 = scanner.lookAhead()

          t2.token match
            case id if id.isIdentifier || id == SqlToken.DOUBLE_QUOTE_STRING =>
              // Single level parenthesized relation: (table alias LEFT JOIN ...)
              val r = relationRest(relation())
              consume(SqlToken.R_PAREN)
              r
            case _ =>
              val subQuery =
                if t2.token.isQueryStart then
                  // Subquery: (SELECT ... UNION ...)
                  query()
                else
                  // Other parenthesized expressions or nested parentheses
                  relationRest(relation())
              consume(SqlToken.R_PAREN)
              BracedRelation(subQuery, spanFrom(t2))
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
        case q if q.isQueryStart =>
          query()
        case _ =>
          unexpected(t)
      end match
    end r

    r
  end relationPrimary

  def tableAlias(input: Relation): AliasedRelation =
    val alias = identifier()
    val columns: Option[List[NamedType]] =
      scanner.lookAhead().token match
        case SqlToken.L_PAREN =>
          consume(SqlToken.L_PAREN)
          val cols = namedTypes()
          consume(SqlToken.R_PAREN)
          Some(cols)
        case _ =>
          None
    AliasedRelation(input, alias, columns, spanFrom(alias.span))

  def queryRest(r: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.LEFT | SqlToken.RIGHT | SqlToken.INNER | SqlToken.FULL | SqlToken.CROSS |
          SqlToken.ASOF | SqlToken.JOIN =>
        queryRest(join(r))
      case SqlToken.UNION =>
        queryRest(union(r))
      case SqlToken.INTERSECT | SqlToken.EXCEPT =>
        queryRest(intersectOrExcept(r))
      case _ =>
        r

  def join(r: Relation): Relation =
    def joinType(): JoinType =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.LEFT =>
          consume(SqlToken.LEFT)
          consumeIfExist(SqlToken.OUTER)
          consume(SqlToken.JOIN)
          JoinType.LeftOuterJoin
        case SqlToken.RIGHT =>
          consume(SqlToken.RIGHT)
          consumeIfExist(SqlToken.OUTER)
          consume(SqlToken.JOIN)
          JoinType.RightOuterJoin
        case SqlToken.INNER =>
          consume(SqlToken.INNER)
          consume(SqlToken.JOIN)
          JoinType.InnerJoin
        case SqlToken.FULL =>
          consume(SqlToken.FULL)
          consumeIfExist(SqlToken.OUTER)
          consume(SqlToken.JOIN)
          JoinType.FullOuterJoin
        case _ =>
          JoinType.InnerJoin

    def joinCriteria(): JoinCriteria =
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.ON =>
          consume(SqlToken.ON)
          val cond = booleanExpression()
          cond match
            case i: Identifier =>
              val joinKeys = List.newBuilder[NameExpr]
              joinKeys += i

              def nextKey: Unit =
                val la = scanner.lookAhead()
                la.token match
                  case SqlToken.COMMA =>
                    consume(SqlToken.COMMA)
                    val k = identifier()
                    joinKeys += k
                    nextKey
                  case other =>
              // stop the search
              nextKey
              JoinUsing(joinKeys.result(), spanFrom(t))
            case _ =>
              JoinOn(cond, spanFrom(t))
        case SqlToken.USING =>
          consume(SqlToken.USING)
          consume(SqlToken.L_PAREN)
          val joinKeys = parseIdentifierList()
          consume(SqlToken.R_PAREN)
          JoinUsing(joinKeys, spanFrom(t))
        case _ =>
          NoJoinCriteria
      end match
    end joinCriteria

    val isAsOfJoin =
      scanner.lookAhead().token match
        case SqlToken.ASOF =>
          consume(SqlToken.ASOF)
          true
        case _ =>
          false
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.CROSS =>
        consume(SqlToken.CROSS)
        consume(SqlToken.JOIN)
        val right = relation()
        Join(JoinType.CrossJoin, r, right, NoJoinCriteria, asof = isAsOfJoin, spanFrom(t))
      case SqlToken.JOIN =>
        consume(SqlToken.JOIN)
        val right  = relation()
        val joinOn = joinCriteria()
        Join(JoinType.InnerJoin, r, right, joinOn, asof = isAsOfJoin, spanFrom(t))
      case SqlToken.LEFT | SqlToken.RIGHT | SqlToken.INNER | SqlToken.FULL =>
        val joinTpe = joinType()
        val right   = relation()
        val joinOn  = joinCriteria()
        Join(joinTpe, r, right, joinOn, asof = isAsOfJoin, spanFrom(t))
      case _ =>
        unexpected(t)

  end join

  def union(r: Relation): Relation =
    val t          = consume(SqlToken.UNION)
    val isUnionAll = consumeIfExist(SqlToken.ALL)
    val right      = query()
    Union(r, right, !isUnionAll, spanFrom(t))

  def intersectOrExcept(r: Relation): Relation =
    def isAll: Boolean = consumeIfExist(SqlToken.ALL)

    val t = scanner.lookAhead()
    t.token match
      case SqlToken.INTERSECT =>
        consume(SqlToken.INTERSECT)
        val isDistinct = !isAll
        val right      = query()
        Intersect(r, right, isDistinct, spanFrom(t))
      case SqlToken.EXCEPT =>
        consume(SqlToken.EXCEPT)
        val isDistinct = !isAll
        val right      = query()
        Except(r, right, isDistinct, spanFrom(t))
      case _ =>
        unexpected(t)

  def namedTypes(): List[NamedType] =
    val t = scanner.lookAhead()
    t.token match
      case SqlToken.EOF | SqlToken.END | SqlToken.R_PAREN =>
        List.empty
      case SqlToken.COMMA =>
        consume(SqlToken.COMMA)
        namedTypes()
      case _ =>
        val e = namedType()
        e :: namedTypes()

  def namedType(): NamedType =
    val id   = identifier()
    val name = id.toTermName
    scanner.lookAhead().token match
      case SqlToken.COLON =>
        consume(SqlToken.COLON)
        val tpeName   = identifier().fullName
        val tpeParams = typeParams()
        NamedType(name, DataType.parse(tpeName, tpeParams))
      case _ =>
        NamedType(name, DataType.UnknownType)

  def typeName(): DataType =
    val id   = identifier()
    val name = id.toTermName

    // Special handling for timestamp types that may have "with time zone" or "without time zone"
    if name.name.toLowerCase == "timestamp" then
      parseTimestampType()
    else
      scanner.lookAhead().token match
        case SqlToken.L_PAREN =>
          val tpeParams = typeParams()
          DataType.parse(name.name, tpeParams)
        case SqlToken.LT =>
          // Support Hive-style array<T>, map<K,V> syntax with angle brackets
          val tpeParams = typeParamsWithAngleBrackets()
          DataType.parse(name.name, tpeParams)
        case _ =>
          DataType.parse(name.name)

  def parseTimestampType(): DataType =
    // Check for optional precision parameter like timestamp(3)
    val precision =
      scanner.lookAhead().token match
        case SqlToken.L_PAREN =>
          consume(SqlToken.L_PAREN)
          val t = consume(SqlToken.INTEGER_LITERAL)
          consume(SqlToken.R_PAREN)
          Some(IntConstant(t.str.toInt))
        case _ =>
          None

    // Check for with/without time zone
    val withTimeZone =
      scanner.lookAhead().token match
        case token @ (SqlToken.WITH | SqlToken.WITHOUT) =>
          consume(token)
          consume(SqlToken.TIME)
          consume(SqlToken.ZONE)
          token == SqlToken.WITH
        case _ =>
          false // default to timestamp without time zone

    TimestampType(TimestampField.TIMESTAMP, withTimeZone, precision)

  def typeParams(): List[TypeParameter] =
    scanner.lookAhead().token match
      case SqlToken.L_PAREN =>
        consume(SqlToken.L_PAREN)
        val params = List.newBuilder[TypeParameter]

        // Read a single type parameter, supporting nested parentheses like row(key bigint, value bigint)
        def readOneParam(): Option[TypeParameter] =
          // Skip any leading commas to avoid stack overflow from recursion
          while scanner.lookAhead().token == SqlToken.COMMA do
            consume(SqlToken.COMMA)

          val t0 = scanner.lookAhead()
          t0.token match
            case SqlToken.R_PAREN =>
              None
            case SqlToken.INTEGER_LITERAL =>
              val i = consume(SqlToken.INTEGER_LITERAL)
              Some(IntConstant(i.str.toInt))
            case _ =>
              // Capture a possibly complex type parameter as raw text, including nested (...) pairs.
              // This allows expressions like ROW(key bigint, value bigint) to be treated as a single parameter.
              val sb            = new StringBuilder
              var parenDepth    = 0
              var continueParam = true

              def appendTokenText(tok: TokenData[SqlToken]): Unit =
                // Minimal spacing rules to reconstruct a parsable type string
                tok.token match
                  case SqlToken.L_PAREN | SqlToken.L_BRACKET =>
                    sb.append(tok.str)
                  case SqlToken.R_PAREN | SqlToken.R_BRACKET =>
                    sb.append(tok.str)
                  case SqlToken.COMMA =>
                    sb.append(", ")
                  case _ =>
                    if sb.nonEmpty then
                      val last = sb.last
                      if last != ' ' && last != '(' && last != '[' && last != ':' then
                        sb.append(' ')
                    sb.append(tok.str)

              // Consume the first token of the parameter
              appendTokenText(consumeToken())

              while continueParam do
                val la = scanner.lookAhead()
                la.token match
                  case SqlToken.L_PAREN =>
                    parenDepth += 1
                    appendTokenText(consumeToken())
                  case SqlToken.R_PAREN =>
                    if parenDepth > 0 then
                      parenDepth -= 1
                      appendTokenText(consumeToken())
                    else
                      // End of this parameter list (do not consume here)
                      continueParam = false
                  case SqlToken.COMMA if parenDepth == 0 =>
                    // End of current parameter
                    continueParam = false
                  case _ =>
                    appendTokenText(consumeToken())

              Some(UnresolvedTypeParameter(sb.result().trim, None))
          end match
        end readOneParam

        // Read parameters until ')'
        var done = false
        while !done do
          readOneParam() match
            case Some(p) =>
              params += p
              // If the next token is a comma, consume it and continue
              if scanner.lookAhead().token == SqlToken.COMMA then
                consume(SqlToken.COMMA)
            case None =>
              done = true

        consume(SqlToken.R_PAREN)
        params.result()
      case _ =>
        Nil

  def typeParamsWithAngleBrackets(): List[TypeParameter] =
    consume(SqlToken.LT)
    val params = List.newBuilder[TypeParameter]

    def readOneParam(): TypeParameter =
      val t0 = scanner.lookAhead()
      t0.token match
        case SqlToken.INTEGER_LITERAL =>
          val i = consume(SqlToken.INTEGER_LITERAL)
          IntConstant(i.str.toInt)
        case _ =>
          // Parse the type name, which might have angle brackets
          val typeName = identifier()
          val la       = scanner.lookAhead()
          la.token match
            case SqlToken.LT =>
              // This type has its own type parameters
              val nestedParams = typeParamsWithAngleBrackets()
              val fullType     = DataType.parse(typeName.unquotedValue, nestedParams)
              UnresolvedTypeParameter(fullType.toString, None)
            case _ =>
              // Simple type without parameters
              UnresolvedTypeParameter(typeName.unquotedValue, None)
      end match
    end readOneParam

    // Read parameters until '>'
    while scanner.lookAhead().token != SqlToken.GT do
      params += readOneParam()
      // If the next token is a comma, consume it and continue
      if scanner.lookAhead().token == SqlToken.COMMA then
        consume(SqlToken.COMMA)
        if scanner.lookAhead().token == SqlToken.GT then
          // A comma must be followed by a type parameter
          unexpected(scanner.lookAhead(), "Trailing comma in type parameter list")
      else if scanner.lookAhead().token != SqlToken.GT then
        // Expected comma or closing bracket
        unexpected(scanner.lookAhead())

    consume(SqlToken.GT)
    params.result()

  end typeParamsWithAngleBrackets

  def identifierList(): List[QualifiedName] =
    val t = scanner.lookAhead()

    def next(): List[QualifiedName] =
      val id = identifier()
      scanner.lookAhead().token match
        case SqlToken.COMMA =>
          consume(SqlToken.COMMA)
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
      case SqlToken.UNDERSCORE =>
        consume(SqlToken.UNDERSCORE)
        UnquotedIdentifier(t.str, spanFrom(t))
      case SqlToken.INTEGER_LITERAL =>
        consume(SqlToken.INTEGER_LITERAL)
        DigitIdentifier(t.str, spanFrom(t))
      case SqlToken.DOUBLE_QUOTE_STRING =>
        consume(SqlToken.DOUBLE_QUOTE_STRING)
        DoubleQuotedIdentifier(t.str, spanFrom(t))
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

  def partitionWriteOptions(): List[PartitionWriteOption] =
    import PartitionWriteMode.*
    import scala.collection.mutable.ListBuffer

    val options         = ListBuffer.empty[PartitionWriteOption]
    var hasClusterBy    = false
    var hasDistributeBy = false
    var hasSortBy       = false
    var continueLoop    = true

    while continueLoop do
      val t = scanner.lookAhead()
      t.token match
        case SqlToken.CLUSTER =>
          if hasDistributeBy || hasSortBy then
            unexpected(t, "CLUSTER BY cannot be used with DISTRIBUTE BY or SORT BY")
          consume(SqlToken.CLUSTER)
          consume(SqlToken.BY)
          val expressions = expressionList()
          options += PartitionWriteOption(HIVE_CLUSTER_BY, expressions = expressions)
          hasClusterBy = true
        case SqlToken.DISTRIBUTE =>
          if hasClusterBy then
            unexpected(t, "DISTRIBUTE BY cannot be used with CLUSTER BY")
          consume(SqlToken.DISTRIBUTE)
          consume(SqlToken.BY)
          val expressions = expressionList()
          options += PartitionWriteOption(HIVE_DISTRIBUTE_BY, expressions = expressions)
          hasDistributeBy = true
        case SqlToken.SORT =>
          if hasClusterBy then
            unexpected(t, "SORT BY cannot be used with CLUSTER BY")
          consume(SqlToken.SORT)
          consume(SqlToken.BY)
          val items = sortItems()
          options += PartitionWriteOption(HIVE_SORT_BY, sortItems = items)
          hasSortBy = true
        case _ =>
          continueLoop = false
    options.toList

  end partitionWriteOptions

  private def extractAllPartitionOptions(q: Relation): (Relation, List[PartitionWriteOption]) =
    val (childRelation, hintOptions) = Update.extractPartitionOptions(q)
    val explicitOptions              = partitionWriteOptions()
    (childRelation, hintOptions ++ explicitOptions)

  def hivePartitionClauses(input: Relation): Relation =
    val options = partitionWriteOptions()
    if options.nonEmpty then
      PartitioningHint(input, options, input.span)
    else
      input

  def reserved(): Identifier =
    val t = consumeToken()
    t.token match
      case token if token.isReservedKeyword =>
        UnquotedIdentifier(t.str, spanFrom(t))
      case _ =>
        unexpected(t)

end SqlParser
