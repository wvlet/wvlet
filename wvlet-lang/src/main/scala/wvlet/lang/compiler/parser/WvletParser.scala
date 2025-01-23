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

import wvlet.airframe.SourceCode
import wvlet.lang.api.{Span, StatusCode}
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.parser.WvletToken.isQueryDelimiter
import wvlet.lang.compiler.{CompilationUnit, Name, SourceFile}
import wvlet.lang.model.DataType.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.expr.NameExpr.EmptyName
import wvlet.lang.model.plan.*
import wvlet.lang.model.{DataType, plan}
import wvlet.log.LogSupport

import scala.collection.immutable.ListMap
import scala.util.Try

/**
  * Wvlet Language Parser. The grammar is described in `docs/internal/grammar.md` file.
  * @param unit
  */
class WvletParser(unit: CompilationUnit, isContextUnit: Boolean = false) extends LogSupport:

  given src: SourceFile                  = unit.sourceFile
  given compilationUnit: CompilationUnit = unit

  private val scanner = WvletScanner(
    unit.sourceFile,
    ScannerConfig(
      skipComments = true,
      // enable debug only for the context unit
      debugScanner = isContextUnit
    )
  )

  def parse(): LogicalPlan =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.PACKAGE =>
        packageDef()
      case _ =>
        val stmts = statements()
        PackageDef(EmptyName, stmts, unit.sourceFile, spanFrom(t))

  private var lastToken: TokenData[WvletToken] = null

  // private def sourceLocation: SourceLocation = SourceLocation(unit.sourceFile, nodeLocation())
  def consume(expected: WvletToken)(using code: SourceCode): TokenData[WvletToken] =
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

  def consumeToken(): TokenData[WvletToken] =
    val t = scanner.nextToken()
    lastToken = t
    t

  /**
    * Compute a span from the given token to the last read token
    * @param startToken
    * @return
    */
  private def spanFrom(startToken: TokenData[WvletToken]): Span = startToken
    .span
    .extendTo(lastToken.span)

  private def spanFrom(startSpan: Span): Span = startSpan.extendTo(lastToken.span)

  private def unexpected(t: TokenData[WvletToken])(using code: SourceCode): Nothing =
    throw StatusCode
      .SYNTAX_ERROR
      .newException(
        s"Unexpected token: <${t.token}> '${t.str}' (context: WvletParser.scala:${code.line})",
        t.sourceLocation
      )

  private def unexpected(expr: Expression)(using code: SourceCode): Nothing =
    throw StatusCode
      .SYNTAX_ERROR
      .newException(
        s"Unexpected expression: ${expr} (context: WvletParser.scala:${code.line})",
        expr.sourceLocationOfCompilationUnit
      )

  def identifier(): QualifiedName =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.IDENTIFIER =>
        consume(WvletToken.IDENTIFIER)
        UnquotedIdentifier(t.str, spanFrom(t))
      case WvletToken.BACKQUOTED_IDENTIFIER =>
        consume(WvletToken.BACKQUOTED_IDENTIFIER)
        BackQuotedIdentifier(t.str, DataType.UnknownType, spanFrom(t))
      case WvletToken.BACKQUOTE_INTERPOLATION_PREFIX if t.str == "s" =>
        interpolatedBackquoteString()
      case WvletToken.UNDERSCORE =>
        consume(WvletToken.UNDERSCORE)
        ContextInputRef(DataType.UnknownType, spanFrom(t))
      case WvletToken.STAR =>
        consume(WvletToken.STAR)
        Wildcard(spanFrom(t))
      case WvletToken.INTEGER_LITERAL =>
        consume(WvletToken.INTEGER_LITERAL)
        DigitIdentifier(t.str, spanFrom(t))
      case WvletToken.DOUBLE_QUOTE_STRING =>
        consume(WvletToken.DOUBLE_QUOTE_STRING)
        DoubleQuotedIdentifier(t.str, spanFrom(t))
      case _ =>
        // TODO Define what is reserved (e.g., select, add, true, etc.) or not (e.g., count, table, user)
        reserved()

  def identifierSingle(): Identifier =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.IDENTIFIER =>
        consume(WvletToken.IDENTIFIER)
        UnquotedIdentifier(t.str, spanFrom(t))
      case WvletToken.BACKQUOTED_IDENTIFIER =>
        consume(WvletToken.BACKQUOTED_IDENTIFIER)
        BackQuotedIdentifier(t.str, DataType.UnknownType, spanFrom(t))
      case _ =>
        reserved()

  def reserved(): Identifier =
    val t = consumeToken()
    t.token match
      case token if token.isReservedKeyword =>
        UnquotedIdentifier(t.str, spanFrom(t))
      case _ =>
        unexpected(t)

  /**
    * PackageDef := 'package' qualifiedId (statement)*
    */
  def packageDef(): PackageDef =
    val t = consumeToken()
    val packageName: QualifiedName =
      t.token match
        case WvletToken.PACKAGE =>
          val packageName = qualifiedId()
          packageName
        case _ =>
          EmptyName

    val stmts = statements()
    PackageDef(packageName, stmts, unit.sourceFile, spanFrom(t))

  /**
    * statements := statement+
    * @return
    */
  def statements(): List[LogicalPlan] =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.EOF =>
        List.empty
      case WvletToken.SEMICOLON =>
        consume(WvletToken.SEMICOLON)
        statements()
      case _ =>
        val stmt: LogicalPlan = statement()
        stmt :: statements()

  def statement(): LogicalPlan =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.IMPORT =>
        importStatement()
      case WvletToken.FROM | WvletToken.SELECT | WvletToken.L_BRACE | WvletToken.WITH =>
        query()
      case WvletToken.TYPE =>
        typeDef()
      case WvletToken.MODEL =>
        modelDef()
      case WvletToken.DEF =>
        val d = funDef()
        TopLevelFunctionDef(d, spanFrom(t))
      case WvletToken.VAL =>
        valDef()
      case WvletToken.SHOW =>
        showExpr()
      case WvletToken.EXECUTE =>
        executeExpr()
      case WvletToken.DESCRIBE =>
        consume(WvletToken.DESCRIBE)
        val r = relationPrimary()
        Describe(r, spanFrom(t))
      case _ =>
        unexpected(t)

  def showExpr(): LogicalPlan =
    val t    = consume(WvletToken.SHOW)
    val name = identifier()
    try
      ShowType.valueOf(name.leafName) match
        case ShowType.models | ShowType.tables | ShowType.schemas | ShowType.databases =>
          val inExpr: NameExpr =
            scanner.lookAhead().token match
              case WvletToken.IN =>
                consume(WvletToken.IN)
                qualifiedId()
              case _ =>
                EmptyName

          val s = Show(ShowType.valueOf(name.leafName), inExpr, spanFrom(t))
          val q = queryBlock(s)
          Query(q, spanFrom(t))
        case ShowType.catalogs =>
          val s = Show(ShowType.valueOf(name.leafName), NameExpr.EmptyName, spanFrom(t))
          val q = queryBlock(s)
          Query(q, spanFrom(t))
        case ShowType.query =>
          val ref = nameExpression()
          ShowQuery(ref, spanFrom(t))
    catch
      case e: IllegalArgumentException =>
        throw StatusCode
          .SYNTAX_ERROR
          .newException(s"Unknown argument for show: ${name.leafName}", t.sourceLocation)

  def executeExpr(): ExecuteExpr =
    val t    = consume(WvletToken.EXECUTE)
    val expr = expression()
    ExecuteExpr(expr, spanFrom(t))

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
    val q: Query =
      query() match
        case q: Query =>
          q
        case other =>
          throw StatusCode
            .SYNTAX_ERROR
            .newException(
              s"Expected a query block, but found ${other}",
              other.sourceLocationOfCompilationUnit
            )
    consume(WvletToken.END)
    ModelDef(
      TableName(name.fullName),
      params,
      // resolve the model type from the query if no type is given
      tpe.map(x => UnresolvedRelationType(x.fullName, Name.typeName(x.leafName))),
      q,
      spanFrom(t)
    )

  end modelDef

  def importStatement(): Import =
    val d: Import = importRef()
    scanner.lookAhead().token match
      case WvletToken.FROM =>
        consume(WvletToken.FROM)
        val source = stringLiteral()
        d.copy(fromSource = Some(source), span = d.span.extendTo(lastToken.span))
      case _ =>
        d

  def importRef(): Import =
    val i             = consume(WvletToken.IMPORT)
    val qid: NameExpr = qualifiedId()
    val t             = scanner.lookAhead()
    t.token match
      case WvletToken.DOT =>
        consume(WvletToken.DOT)
        val w = consume(WvletToken.STAR)
        Import(
          DotRef(qid, Wildcard(w.span), DataType.UnknownType, qid.span),
          None,
          None,
          spanFrom(i)
        )
      case WvletToken.AS =>
        // alias
        consume(WvletToken.AS)
        val alias = identifier()
        Import(qid, Some(alias), None, spanFrom(i))
      case _ =>
        Import(qid, None, None, spanFrom(i))

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
          s"extending multiple types is not supported: ${name} extends ${parents
              .map(_.fullName)
              .mkString(", ")}",
          t.sourceLocation
        )
    TypeDef(name, tp, scopes, parents.headOption, elems, spanFrom(t))

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
      case id if id.isIdentifier =>
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

        FieldDef(Name.termName(name.leafName), valType, tp, defaultValue, spanFrom(t))
      case _ =>
        unexpected(t)

  def tableAlias(input: Relation): AliasedRelation =
    val alias = identifierSingle()
    val columns: Option[List[NamedType]] =
      scanner.lookAhead().token match
        case WvletToken.L_PAREN =>
          consume(WvletToken.L_PAREN)
          val cols = namedTypes()
          consume(WvletToken.R_PAREN)
          Some(cols)
        case _ =>
          None
    AliasedRelation(input, alias, columns, spanFrom(alias.span))

  def namedTypes(): List[NamedType] =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.EOF | WvletToken.END | WvletToken.R_PAREN =>
        List.empty
      case WvletToken.COMMA =>
        consume(WvletToken.COMMA)
        namedTypes()
      case _ =>
        val e = namedType()
        e :: namedTypes()

  def namedType(): NamedType =
    val id   = identifierSingle()
    val name = id.toTermName
    // scan `: (type)`
    scanner.lookAhead().token match
      case WvletToken.COLON =>
        consume(WvletToken.COLON)
        val tpeName   = identifier().fullName
        val tpeParams = typeParams()
        NamedType(name, DataType.parse(tpeName, tpeParams))
      case _ =>
        NamedType(name, DataType.UnknownType)

  def funDef(): FunctionDef =
    def funName(): NameExpr =
      val t = scanner.lookAhead()
      t.token match
        case id if id.isIdentifier =>
          identifier()
        case WvletToken.PLUS | WvletToken.MINUS | WvletToken.STAR | WvletToken.DIV | WvletToken
              .MOD | WvletToken.AMP | WvletToken.EQ | WvletToken.NEQ | WvletToken.LT | WvletToken
              .LTEQ | WvletToken.GT | WvletToken.GTEQ =>
          // symbols
          consume(t.token)
          UnquotedIdentifier(t.str, spanFrom(t))
        case _ =>
          reserved()

    val t    = consume(WvletToken.DEF)
    val name = funName()
    val args: List[DefArg] =
      scanner.lookAhead().token match
        case WvletToken.L_PAREN =>
          consume(WvletToken.L_PAREN)
          val args      = defArgs()
          val lastToken = consume(WvletToken.R_PAREN)
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
          val t = scanner.lookAhead()
          t.token match
            case WvletToken.NATIVE =>
              val n = consume(WvletToken.NATIVE)
              Some(NativeExpression(name.fullName, retType, spanFrom(n)))
            case _ =>
              Some(expression())
        case _ =>
          None

    FunctionDef(Name.termName(name.leafName), args, defScope, retType, body, spanFrom(t))

  end funDef

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
    DefArg(Name.termName(name.leafName), dt, defaultValue, spanFrom(name.span))

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
              scopes += DefContext(None, nameOrType, spanFrom(t))
              nextScope
        nextScope
        scopes.result()
      case _ =>
        Nil

  def valDef(): ValDef =
    val t    = consume(WvletToken.VAL)
    val name = identifier()

    val valType: Option[DataType] =
      scanner.lookAhead().token match
        case WvletToken.COLON =>
          consume(WvletToken.COLON)
          val typeName = identifier()
          Some(DataType.parse(typeName.fullName))
        case _ =>
          None

    consume(WvletToken.EQ)
    val expr         = expression()
    val exprType     = expr.dataType
    val resolvedType = valType.getOrElse(exprType)
    ValDef(Name.termName(name.leafName), resolvedType, expr, spanFrom(t))

  def orderExpr(input: Relation): Sort =
    val t = consume(WvletToken.ORDER)
    consume(WvletToken.BY)
    val items = sortItems()
    Sort(input, items, spanFrom(t))

  def sortItems(): List[SortItem] =
    def sortOrder(): Option[SortOrdering] =
      scanner.lookAhead().token match
        case WvletToken.ASC =>
          consume(WvletToken.ASC)
          Some(SortOrdering.Ascending)
        case WvletToken.DESC =>
          consume(WvletToken.DESC)
          Some(SortOrdering.Descending)
        case _ =>
          None

    def nullOrder(): Option[NullOrdering] =
      scanner.lookAhead().token match
        case WvletToken.NULLS =>
          consume(WvletToken.NULLS)
          scanner.lookAhead().token match
            case WvletToken.FIRST =>
              consume(WvletToken.FIRST)
              Some(NullOrdering.NullIsFirst)
            case WvletToken.LAST =>
              consume(WvletToken.LAST)
              Some(NullOrdering.NullIsLast)
            case _ =>
              None
        case _ =>
          None

    val e      = expression()
    val order  = sortOrder()
    val nOrder = nullOrder()
    val si     = SortItem(e, order, nOrder, spanFrom(e.span))
    scanner.lookAhead().token match
      case WvletToken.COMMA =>
        consume(WvletToken.COMMA)
        si :: sortItems()
      case _ =>
        List(si)
  end sortItems

  def query(): Relation =
    val t           = scanner.lookAhead()
    var r: Relation = queryBody()
    r =
      r match
        case i: RelationInspector =>
          i
        case _ =>
          Query(r, spanFrom(t))

    updateRelationIfExists(r)
  end query

  def updateRelationIfExists(r: Relation): Relation =

    def saveOptions(): List[SaveOption] =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.WITH =>
          consume(WvletToken.WITH)
          val options = List.newBuilder[SaveOption]
          def nextOption: Unit =
            val t = scanner.lookAhead()
            t.token match
              case WvletToken.COMMA =>
                consume(WvletToken.COMMA)
                nextOption
              case WvletToken.IDENTIFIER =>
                val key = identifierSingle()
                consume(WvletToken.COLON)
                val value = expression()
                options += SaveOption(key, value, key.span.extendTo(value.span))
                nextOption
              case _ =>
              // finish
          nextOption
          options.result()
        case _ =>
          List.empty

    def literalOrQualifiedName(): StringLiteral | QualifiedName =
      scanner.lookAhead().token match
        case s if s.isStringLiteral =>
          stringLiteral()
        case _ =>
          qualifiedId()

    val t = scanner.lookAhead()
    t.token match
      case WvletToken.SAVE =>
        consume(WvletToken.SAVE)
        consume(WvletToken.TO)
        val target: StringLiteral | QualifiedName = literalOrQualifiedName()
        val opts                                  = saveOptions()
        SaveTo(r, target, opts, spanFrom(t))
      case WvletToken.APPEND =>
        consume(WvletToken.APPEND)
        consume(WvletToken.TO)
        val target: StringLiteral | QualifiedName = literalOrQualifiedName()
        AppendTo(r, target, spanFrom(t))
      case WvletToken.DELETE =>
        consume(WvletToken.DELETE)
        def iter(x: Relation): Relation =
          x match
            case f: FilteringRelation =>
              iter(f.child)
            case TableRef(qname: QualifiedName, _) =>
              Delete(r, qname, spanFrom(t))
            case f: FileRef =>
              Delete(r, f.path, spanFrom(t))
            case f: FileScan =>
              Delete(r, f.path, spanFrom(t))
            case other =>
              throw StatusCode
                .SYNTAX_ERROR
                .newException(
                  s"delete statement can't have ${other.nodeName} operator",
                  t.sourceLocation
                )
        iter(r)
      case _ =>
        r

    end match

  end updateRelationIfExists

  def queryBody(): Relation = queryBlock(querySingle())

  def querySingle(): Relation =
    def readRest(input: Relation): Relation =
      scanner.lookAhead().token match
        case WvletToken.COMMA =>
          val ct    = consume(WvletToken.COMMA)
          val rNext = relation()
          val rel = Join(
            JoinType.ImplicitJoin,
            input,
            rNext,
            NoJoinCriteria,
            false,
            spanFrom(rNext.span)
          )
          readRest(rel)
        case _ =>
          input

    var r: Relation = null
    val t           = scanner.lookAhead()
    t.token match
      case WvletToken.FROM =>
        r = fromRelation()
        r = readRest(r)
        r = queryBlock(r)
        r = readRest(r)
      case WvletToken.SELECT =>
        // select only query like select 1
        r = selectExpr(EmptyRelation(t.span))
        r = queryBlock(r)
      case WvletToken.L_BRACE =>
        // parenthesized query
        consume(WvletToken.L_BRACE)
        val q = queryBody()
        consume(WvletToken.R_BRACE)
        r = BracedRelation(q, spanFrom(t))
      case WvletToken.WITH =>
        r = withQuery()
      case _ =>
        unexpected(t)
    r
  end querySingle

  def withQuery(): Relation =
    def alias(): (NameExpr, Option[List[NamedType]]) =
      val name = identifierSingle()
      val columns: Option[List[NamedType]] =
        scanner.lookAhead().token match
          case WvletToken.L_PAREN =>
            consume(WvletToken.L_PAREN)
            val cols = namedTypes()
            consume(WvletToken.R_PAREN)
            Some(cols)
          case _ =>
            None
      (name, columns)
    end alias

    def withExpr(): List[AliasedRelation] =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.WITH =>
          consume(WvletToken.WITH)
          val (name, columns) = alias()
          consume(WvletToken.AS)

          // with query
          consume(WvletToken.L_BRACE)
          val q = queryBody()
          consume(WvletToken.R_BRACE)
          val a = AliasedRelation(q, name, columns, spanFrom(t))
          // Read next if exists
          scanner.lookAhead().token match
            case WvletToken.WITH =>
              a :: withExpr()
            case other =>
              List(a)
        case _ =>
          unexpected(t)
    end withExpr

    val t              = scanner.lookAhead()
    val withStatements = withExpr()
    val withSpan       = spanFrom(t)
    val child          = queryBody()

    def injectWithQuery(input: Relation): Relation =
      input match
        case t: TestRelation =>
          t.copy(child = injectWithQuery(t.child))
        case body =>
          val w = WithQuery(false, withStatements, body, withSpan)
          w
    end injectWithQuery

    injectWithQuery(child)
  end withQuery

  def fromRelation(): Relation =
    scanner.lookAhead().token match
      case WvletToken.FROM =>
        consume(WvletToken.FROM)
        relation()
      case _ =>
        relation()

  def relation(): Relation =
    val r = relationPrimary()
    scanner.lookAhead().token match
      case WvletToken.AS =>
        consume(WvletToken.AS)
        tableAlias(r)
      case _ =>
        r

  def queryBlock(input: Relation): Relation =
    queryBlockSingle(input) match
      case r if r eq input =>
        r
      case r =>
        queryBlock(r)

  def queryBlockSingle(input: Relation): Relation =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.PIPE =>
        consume(WvletToken.PIPE)
        queryBlockSingle(input)
      case WvletToken.LEFT | WvletToken.RIGHT | WvletToken.INNER | WvletToken.FULL | WvletToken
            .CROSS | WvletToken.ASOF | WvletToken.JOIN =>
        join(input)
      case WvletToken.WHERE =>
        consume(WvletToken.WHERE)
        val cond = booleanExpression()
        Filter(input, cond, spanFrom(t))
      case WvletToken.ADD =>
        addColumnsExpr(input)
      case WvletToken.EXCLUDE =>
        excludeColumnExpr(input)
      case WvletToken.RENAME =>
        renameColumnExpr(input)
      case WvletToken.SHIFT =>
        shiftColumnsExpr(input)
      case WvletToken.GROUP =>
        groupByExpr(input)
      case WvletToken.AGG =>
        aggExpr(input)
      case WvletToken.COUNT =>
        countExpr(input)
      case WvletToken.PIVOT =>
        pivotExpr(input)
      case WvletToken.SELECT =>
        selectExpr(input)
      case WvletToken.LIMIT =>
        limitExpr(input)
      case WvletToken.ORDER =>
        orderExpr(input)
      case WvletToken.TEST =>
        testExpr(input)
      case WvletToken.DESCRIBE =>
        consume(WvletToken.DESCRIBE)
        Describe(input, spanFrom(t))
      case WvletToken.SAMPLE =>
        sampleExpr(input)
      case WvletToken.CONCAT =>
        consume(WvletToken.CONCAT)
        val right =
          scanner.lookAhead().token match
            case WvletToken.L_BRACE =>
              val s = consume(WvletToken.L_BRACE)
              val r = fromRelation()
              consume(WvletToken.R_BRACE)
              BracedRelation(r, spanFrom(s))
            case _ =>
              fromRelation()
        Concat(input, right, spanFrom(t))
      case WvletToken.INTERSECT | WvletToken.EXCEPT =>
        consume(t.token)
        val isDistinct: Boolean =
          scanner.lookAhead().token match
            case WvletToken.ALL =>
              consume(WvletToken.ALL)
              false
            case _ =>
              true
        val right = fromRelation()
        val rel =
          t.token match
            case WvletToken.INTERSECT =>
              Intersect(input, right, isDistinct, spanFrom(t))
            case WvletToken.EXCEPT =>
              Except(input, right, isDistinct, spanFrom(t))
            case _ =>
              unexpected(t)
        rel
      case WvletToken.DEDUP =>
        consume(WvletToken.DEDUP)
        Dedup(input, spanFrom(t))
      case WvletToken.DEBUG =>
        debugExpr(input)
      case _ =>
        input

    end match

  end queryBlockSingle

  def join(input: Relation): Relation =
    val isAsOfJoin =
      scanner.lookAhead().token match
        case WvletToken.ASOF =>
          consume(WvletToken.ASOF)
          true
        case _ =>
          false

    val t = scanner.lookAhead()
    t.token match
      case WvletToken.CROSS =>
        consume(WvletToken.CROSS)
        consume(WvletToken.JOIN)
        val right = relation()
        Join(JoinType.CrossJoin, input, right, NoJoinCriteria, isAsOfJoin, spanFrom(t))
      case WvletToken.JOIN =>
        consume(WvletToken.JOIN)
        val right  = relation()
        val joinOn = joinCriteria()
        Join(JoinType.InnerJoin, input, right, joinOn, isAsOfJoin, spanFrom(t))
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
        val right  = relation()
        val joinOn = joinCriteria()
        Join(joinType, input, right, joinOn, isAsOfJoin, spanFrom(t))
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
            JoinUsing(joinKeys.result(), spanFrom(t))
          case _ =>
            JoinOn(cond, spanFrom(t))
      case _ =>
        NoJoinCriteria

  def addColumnsExpr(input: Relation): AddColumnsToRelation =
    val t     = consume(WvletToken.ADD)
    val items = selectItems()
    AddColumnsToRelation(input, items, spanFrom(t))

  def excludeColumnExpr(input: Relation): ExcludeColumnsFromRelation =
    val t     = consume(WvletToken.EXCLUDE)
    val items = List.newBuilder[Identifier]
    def nextItem: Unit =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          nextItem
        case token if WvletToken.isQueryDelimiter(token) =>
        // finish
        case t if t.tokenType == TokenType.Keyword =>
        // finish
        case _ =>
          items += identifierSingle()
          nextItem
    nextItem
    ExcludeColumnsFromRelation(input, items.result, spanFrom(t))

  def renameColumnExpr(relation: Relation): RenameColumnsFromRelation =
    val t     = consume(WvletToken.RENAME)
    val items = List.newBuilder[Alias]

    def item(): Alias =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.IDENTIFIER | WvletToken.BACKQUOTED_IDENTIFIER =>
          val name = identifierSingle()
          consume(WvletToken.AS)
          val alias = identifierSingle()
          Alias(alias, name, spanFrom(t))
        case _ =>
          unexpected(t)

    def nextItem(): Unit =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          nextItem()
        case token if WvletToken.isQueryDelimiter(token) =>
        // finish
        case t if t.tokenType == TokenType.Keyword =>
        // finish
        case _ =>
          items += item()
          nextItem()
    nextItem()
    RenameColumnsFromRelation(relation, items.result, spanFrom(t))

  end renameColumnExpr

  def shiftColumnsExpr(input: Relation): ShiftColumns =
    val t = consume(WvletToken.SHIFT)

    val isLeftShift =
      scanner.lookAhead().token match
        case WvletToken.TO =>
          consume(WvletToken.TO)
          val tt = scanner.lookAhead()
          tt.token match
            case WvletToken.LEFT =>
              consume(WvletToken.LEFT)
              true
            case WvletToken.RIGHT =>
              consume(WvletToken.RIGHT)
              false
            case _ =>
              unexpected(tt)
        case _ =>
          true

    val items = List.newBuilder[Identifier]
    def nextItem: Unit =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          nextItem
        case token if WvletToken.isQueryDelimiter(token) =>
        // finish
        case t if t.tokenType == TokenType.Keyword =>
        // finish
        case _ =>
          items += identifierSingle()
          nextItem
    nextItem
    ShiftColumns(input, isLeftShift, items.result, spanFrom(t))

  end shiftColumnsExpr

  def groupByExpr(input: Relation): GroupBy =
    val t = consume(WvletToken.GROUP)
    consume(WvletToken.BY)
    val items = groupByItemList()
    GroupBy(input, items, spanFrom(t))

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
        val key = UnquotedIdentifier(s"_${i + 1}", g.span)
        SingleColumn(key, key, g.span)
      }
    // report _1, _2, agg_expr...
    val items = selectItems()
    Agg(input, groupingKeys, items, spanFrom(t))

  def countExpr(input: Relation): Count =
    val t = consume(WvletToken.COUNT)
    Count(input, spanFrom(t))

  def pivotExpr(input: Relation): Pivot =
    def pivotValues: List[Literal] =
      val values = List.newBuilder[Literal]
      def nextValue: Unit =
        val t = scanner.lookAhead()
        t.token match
          case WvletToken.COMMA =>
            consume(WvletToken.COMMA)
            nextValue
          case WvletToken.R_PAREN =>
          // ok
          case _ =>
            val e = literal()
            values += e
            nextValue
      end nextValue

      nextValue
      values.result()
    end pivotValues

    def pivotKeys: List[PivotKey] =
      val t = scanner.lookAhead()
      t.token match
        case id if id.isIdentifier =>
          val pivotKey = identifierSingle()
          scanner.lookAhead().token match
            case WvletToken.IN =>
              consume(WvletToken.IN)
              consume(WvletToken.L_PAREN)
              val values = pivotValues
              consume(WvletToken.R_PAREN)
              PivotKey(pivotKey, values, spanFrom(t)) :: pivotKeys
            case _ =>
              PivotKey(pivotKey, Nil, spanFrom(t)) :: pivotKeys
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          pivotKeys
        case _ =>
          Nil
    end pivotKeys

    val t = consume(WvletToken.PIVOT)
    consume(WvletToken.ON)
    val keys = pivotKeys

    val groupByItems =
      scanner.lookAhead().token match
        case WvletToken.GROUP =>
          consume(WvletToken.GROUP)
          consume(WvletToken.BY)
          groupByItemList()
        case _ =>
          Nil
    Pivot(input, keys, groupByItems, spanFrom(t))
  end pivotExpr

  def groupByItemList(): List[GroupingKey] =
    val t = scanner.lookAhead()
    t.token match
      case id if id.isIdentifier =>
        val item = selectItem()
        val key  = UnresolvedGroupingKey(item.nameExpr, item.expr, spanFrom(t))
        key :: groupByItemList()
      case WvletToken.COMMA =>
        consume(WvletToken.COMMA)
        groupByItemList()
      case t if t.tokenType == TokenType.Keyword =>
        Nil
      case t if t.isQueryDelimiter =>
        Nil
      case _ =>
        // expression only
        val e   = expression()
        val key = UnresolvedGroupingKey(EmptyName, e, e.span)
        key :: groupByItemList()

  def selectExpr(input: Relation): Relation =
    val t = consume(WvletToken.SELECT)
    def proj: Project =
      val items = selectItems()
      Project(input, items, spanFrom(t))

    scanner.lookAhead().token match
      case WvletToken.DISTINCT =>
        val t1 = consume(WvletToken.DISTINCT)
        Distinct(proj, t1.span)
      case WvletToken.AS =>
        // select as
        consume(WvletToken.AS)
        val alias = identifier()
        val rel   = SelectAsAlias(input, alias, spanFrom(t))
        rel
      case _ =>
        proj

  def selectItems(): List[Attribute] =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.COMMA =>
        // Support consuming trailing comma
        consume(WvletToken.COMMA)
        selectItems()
      case token if WvletToken.isQueryDelimiter(token) =>
        Nil
      case t if !t.canStartSelectItem =>
        Nil
      case _ =>
        val item = selectItem()
        scanner.lookAhead().token match
          case WvletToken.COMMA =>
            consume(WvletToken.COMMA)
            item :: selectItems()
          case _ =>
            List(item)
    end match

  end selectItems

  def selectItem(): SingleColumn =
    def selectItemWithAlias(item: Expression): SingleColumn =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.AS =>
          consume(WvletToken.AS)
          val alias = identifier()
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

  def window(): Option[Window] =
    def partitionKeys(): List[Expression] =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.R_PAREN | WvletToken.ORDER | WvletToken.RANGE | WvletToken.ROWS =>
          Nil
        case _ =>
          val e = expression()
          scanner.lookAhead().token match
            case WvletToken.COMMA =>
              consume(WvletToken.COMMA)
              e :: partitionKeys()
            case _ =>
              List(e)
      end match
    end partitionKeys

    def partitionBy(): List[Expression] =
      scanner.lookAhead().token match
        case WvletToken.PARTITION =>
          consume(WvletToken.PARTITION)
          consume(WvletToken.BY)
          partitionKeys()
        case _ =>
          Nil

    def orderBy(): List[SortItem] =
      scanner.lookAhead().token match
        case WvletToken.ORDER =>
          consume(WvletToken.ORDER)
          consume(WvletToken.BY)
          sortItems()
        case _ =>
          Nil

    def integer(): Long =
      val t    = scanner.lookAhead()
      val expr = expression()
      expr match
        case l: LongLiteral =>
          l.value
        case ArithmeticUnaryExpr(sign, l: LongLiteral, _) =>
          sign match
            case Sign.Positive | Sign.NoSign =>
              l.value
            case Sign.Negative =>
              -l.value
        case _ =>
          unexpected(t)

    def windowFrame(): Option[WindowFrame] =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.ROWS =>
          consume(WvletToken.ROWS)
          consume(WvletToken.L_BRACKET)
          val frameStart: FrameBound =
            val t = scanner.lookAhead()
            t.token match
              case WvletToken.COMMA =>
                FrameBound.UnboundedPreceding
              case _ =>
                val n = integer()
                if n == 0 then
                  FrameBound.CurrentRow
                else
                  FrameBound.Preceding(-n)

          consume(WvletToken.COMMA)

          val frameEnd: FrameBound =
            val t = scanner.lookAhead()
            t.token match
              case WvletToken.R_BRACKET =>
                FrameBound.UnboundedFollowing
              case _ =>
                val n = integer()
                if n == 0 then
                  FrameBound.CurrentRow
                else
                  FrameBound.Following(n)
          consume(WvletToken.R_BRACKET)
          Some(WindowFrame(FrameType.RowsFrame, frameStart, frameEnd, spanFrom(t)))
        case _ =>
          // TODO Support WvletToken.RANGE
          None
      end match
    end windowFrame

    scanner.lookAhead().token match
      case WvletToken.OVER =>
        val t = consume(WvletToken.OVER)
        consume(WvletToken.L_PAREN)
        val partition = partitionBy()
        val order     = orderBy()
        val frame     = windowFrame()
        consume(WvletToken.R_PAREN)
        Some(Window(partition, order, frame, spanFrom(t)))
      case _ =>
        None

  end window

  def limitExpr(input: Relation): Limit =
    val t = consume(WvletToken.LIMIT)
    val n = consume(WvletToken.INTEGER_LITERAL)
    Limit(input, LongLiteral(n.str.toLong, n.str, n.span), spanFrom(t))

  def testExpr(input: Relation): Relation =
    val t    = consume(WvletToken.TEST)
    val item = expression()
    TestRelation(input, item, spanFrom(t))

  def sampleExpr(input: Relation): Sample =
    def samplingSize: SamplingSize =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.INTEGER_LITERAL =>
          val n  = consume(WvletToken.INTEGER_LITERAL)
          val t2 = scanner.lookAhead()
          t2.token match
            case WvletToken.MOD =>
              consume(WvletToken.MOD)
              SamplingSize.Percentage(n.str.toDouble)
            case _ =>
              SamplingSize.Rows(n.str.toInt)
        case WvletToken.DOUBLE_LITERAL | WvletToken.FLOAT_LITERAL | WvletToken.DECIMAL_LITERAL =>
          val n = consume(t.token)
          consume(WvletToken.MOD)
          SamplingSize.Percentage(n.str.toDouble)
        case _ =>
          unexpected(t)
    end samplingSize

    val t  = consume(WvletToken.SAMPLE)
    val st = scanner.lookAhead()
    st.token match
      case WvletToken.IDENTIFIER =>
        consume(WvletToken.IDENTIFIER)
        val method: Option[SamplingMethod] = Try(SamplingMethod.valueOf(st.str.toLowerCase))
          .toOption
          .orElse {
            unexpected(st)
          }
        consume(WvletToken.L_PAREN)
        val size = samplingSize
        consume(WvletToken.R_PAREN)
        Sample(input, method, size, spanFrom(t))
      case WvletToken.INTEGER_LITERAL =>
        val size = samplingSize
        Sample(input, None, size, spanFrom(t))
      case WvletToken.FLOAT_LITERAL | WvletToken.DOUBLE_LITERAL | WvletToken.DECIMAL_LITERAL =>
        val size = samplingSize
        // Use system sampling by default for percentage sampling
        Sample(input, Some(SamplingMethod.system), size, spanFrom(t))
      case _ =>
        unexpected(st)

  end sampleExpr

  def debugExpr(input: Relation): Debug =
    def loop(r: Relation): Relation =
      scanner.lookAhead().token match
        case WvletToken.R_BRACE =>
          r
        case t if isQueryDelimiter(t) =>
          r
        case _ =>
          val next =
            scanner.lookAhead().token match
              case WvletToken.SAVE | WvletToken.APPEND | WvletToken.DELETE =>
                updateRelationIfExists(r)
              case _ =>
                queryBlockSingle(r)
          if r eq next then
            r
          else
            loop(next)
    end loop

    val t = consume(WvletToken.DEBUG)
    consume(WvletToken.L_BRACE)
    val debugRel = loop(input)
    consume(WvletToken.R_BRACE)

    Debug(input, debugExpr = debugRel, spanFrom(t))

  def relationPrimary(): Relation =
    val t = scanner.lookAhead()
    t.token match
      case id if id.isIdentifier =>
        val tableOrFunctionName = qualifiedId()
        scanner.lookAhead().token match
          case WvletToken.L_PAREN =>
            // table function
            consume(WvletToken.L_PAREN)
            val args = functionArgs()
            consume(WvletToken.R_PAREN)
            TableFunctionCall(tableOrFunctionName, args, spanFrom(t.span))
          case _ =>
            TableRef(tableOrFunctionName, spanFrom(t.span))
      case WvletToken.SELECT | WvletToken.FROM | WvletToken.L_BRACE =>
        querySingle()
      case s if s.isStringLiteral =>
        val path = stringLiteral()
        FileRef(path, spanFrom(t))
      case i if i.isInterpolatedStringPrefix && t.str == "sql" =>
        val rawSQL = interpolatedString()
        RawSQL(rawSQL, spanFrom(t))
      case i if i.isInterpolatedStringPrefix && t.str == "json" =>
        val rawJSON = interpolatedString()
        RawJSON(rawJSON, spanFrom(t))
      case WvletToken.BACKQUOTE_INTERPOLATION_PREFIX if t.str == "s" =>
        val tableRef = interpolatedBackquoteString()
        TableRef(tableRef, spanFrom(t))
      case WvletToken.L_BRACKET =>
        arrayValue()
      case _ =>
        unexpected(t)
    end match
  end relationPrimary

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
    SingleColumn(EmptyName, expression(), spanFrom(t))

  def expression(): Expression = booleanExpression()

  def booleanExpression(): Expression =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.EXCLAMATION | WvletToken.NOT =>
        consume(t.token)
        val e = booleanExpression()
        Not(e, spanFrom(t))
      case _ =>
        val expr = valueExpression()
        booleanExpressionRest(expr)

  def booleanExpressionRest(expression: Expression): Expression =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.AND =>
        consume(WvletToken.AND)
        val right = booleanExpression()
        And(expression, right, spanFrom(t))
      case WvletToken.OR =>
        consume(WvletToken.OR)
        val right = booleanExpression()
        Or(expression, right, spanFrom(t))
      case _ =>
        expression

  def valueExpression(): Expression =
    val t = scanner.lookAhead()
    val expr =
      t.token match
        case WvletToken.PLUS =>
          consume(WvletToken.PLUS)
          val right = valueExpression()
          ArithmeticUnaryExpr(Sign.Positive, right, spanFrom(t))
        case WvletToken.MINUS =>
          consume(WvletToken.MINUS)
          val right = valueExpression()
          ArithmeticUnaryExpr(Sign.Negative, right, spanFrom(t))
        case _ =>
          primaryExpression()

    valueExpressionRest(expr)

  def valueExpressionRest(expr: Expression): Expression =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.PLUS =>
        consume(WvletToken.PLUS)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Add, expr, right, spanFrom(t))
      case WvletToken.MINUS =>
        consume(WvletToken.MINUS)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Subtract, expr, right, spanFrom(t))
      case WvletToken.STAR =>
        consume(WvletToken.STAR)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Multiply, expr, right, spanFrom(t))
      case WvletToken.DIV =>
        consume(WvletToken.DIV)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Divide, expr, right, spanFrom(t))
      case WvletToken.MOD =>
        consume(WvletToken.MOD)
        val right = valueExpression()
        ArithmeticBinaryExpr(BinaryExprType.Modulus, expr, right, spanFrom(t))
      case WvletToken.EQ =>
        consume(WvletToken.EQ)
        scanner.lookAhead().token match
          case WvletToken.EQ =>
            consume(WvletToken.EQ)
          case _ =>
        val right = valueExpression()
        Eq(expr, right, spanFrom(t))
      case WvletToken.NEQ =>
        consume(WvletToken.NEQ)
        val right = valueExpression()
        NotEq(expr, right, spanFrom(t))
      case WvletToken.IS =>
        consume(WvletToken.IS)
        scanner.lookAhead().token match
          case WvletToken.NOT =>
            consume(WvletToken.NOT)
            val right = valueExpression()
            NotEq(expr, right, spanFrom(t))
          case _ =>
            val right = valueExpression()
            Eq(expr, right, spanFrom(t))
      case WvletToken.LT =>
        consume(WvletToken.LT)
        val right = valueExpression()
        LessThan(expr, right, spanFrom(t))
      case WvletToken.GT =>
        consume(WvletToken.GT)
        val right = valueExpression()
        GreaterThan(expr, right, spanFrom(t))
      case WvletToken.LTEQ =>
        consume(WvletToken.LTEQ)
        val right = valueExpression()
        LessThanOrEq(expr, right, spanFrom(t))
      case WvletToken.GTEQ =>
        consume(WvletToken.GTEQ)
        val right = valueExpression()
        GreaterThanOrEq(expr, right, spanFrom(t))
      case WvletToken.IN =>
        consume(WvletToken.IN)
        val valueList = inExprList()
        In(expr, valueList, spanFrom(t))
      case WvletToken.LIKE =>
        consume(WvletToken.LIKE)
        val right = valueExpression()
        Like(expr, right, spanFrom(t))
      case WvletToken.NOT =>
        consume(WvletToken.NOT)
        val t2 = scanner.lookAhead()
        t2.token match
          case WvletToken.LIKE =>
            consume(WvletToken.LIKE)
            val right = valueExpression()
            NotLike(expr, right, spanFrom(t))
          case WvletToken.IN =>
            consume(WvletToken.IN)
            val valueList = inExprList()
            NotIn(expr, valueList, spanFrom(t))
          case other =>
            unexpected(t2)
      case WvletToken.BETWEEN =>
        consume(WvletToken.BETWEEN)
        val left = valueExpression()
        consume(WvletToken.AND)
        val right = valueExpression()
        Between(expr, left, right, spanFrom(t))
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
        ShouldExpr(testType, left = expr, right, spanFrom(t))
      case _ =>
        expr

    end match

  end valueExpressionRest

  def primaryExpression(): Expression =
    val t = scanner.lookAhead()
    val expr: Expression =
      t.token match
        case WvletToken.THIS =>
          consume(WvletToken.THIS)
          This(DataType.UnknownType, spanFrom(t))
        case WvletToken.UNDERSCORE =>
          consume(WvletToken.UNDERSCORE)
          ContextInputRef(DataType.UnknownType, spanFrom(t))
        case WvletToken.NULL | WvletToken.INTEGER_LITERAL | WvletToken.DOUBLE_LITERAL | WvletToken
              .FLOAT_LITERAL | WvletToken.DECIMAL_LITERAL | WvletToken.EXP_LITERAL | WvletToken
              .SINGLE_QUOTE_STRING | WvletToken.DOUBLE_QUOTE_STRING | WvletToken
              .TRIPLE_QUOTE_STRING =>
          literal()
        case WvletToken.CASE =>
          val cases                          = List.newBuilder[WhenClause]
          var elseClause: Option[Expression] = None
          def nextCase: Unit =
            val t = scanner.lookAhead()
            t.token match
              case WvletToken.WHEN =>
                consume(WvletToken.WHEN)
                val cond = booleanExpression()
                consume(WvletToken.THEN)
                val thenExpr = expression()
                cases += WhenClause(cond, thenExpr, spanFrom(t))
                nextCase
              case WvletToken.ELSE =>
                consume(WvletToken.ELSE)
                val elseExpr = expression()
                elseClause = Some(elseExpr)
              case _ =>
              // done
          end nextCase

          consume(WvletToken.CASE)
          val target =
            scanner.lookAhead().token match
              case WvletToken.WHEN =>
                None
              case other =>
                Some(expression())
          nextCase
          CaseExpr(target, cases.result(), elseClause, spanFrom(t))
        case WvletToken.EXISTS =>
          consume(WvletToken.EXISTS)
          consume(WvletToken.L_BRACE)
          val q = queryBody()
          consume(WvletToken.R_BRACE)
          Exists(SubQueryExpression(q, q.span), spanFrom(t))
        case WvletToken.IF =>
          consume(WvletToken.IF)
          val cond = booleanExpression()
          consume(WvletToken.THEN)
          val thenExpr = expression()
          consume(WvletToken.ELSE)
          val elseExpr = expression()
          IfExpr(cond, thenExpr, elseExpr, spanFrom(t))
        case i if i.isInterpolatedStringPrefix =>
          interpolatedString()
        case WvletToken.FROM =>
          val q: Relation = querySingle()
          SubQueryExpression(q, spanFrom(t))
        case WvletToken.L_BRACE =>
          val lt = consume(WvletToken.L_BRACE)
          val t2 = scanner.lookAhead()
          t2.token match
            case WvletToken.FROM =>
              val q = queryBody()
              consume(WvletToken.R_BRACE)
              SubQueryExpression(q, t2.span)
            case _ =>
              struct(lt)
        case WvletToken.L_PAREN =>
          consume(WvletToken.L_PAREN)
          val t2 = scanner.lookAhead()
          t2.token match
            case WvletToken.IDENTIFIER =>
              val exprs = List.newBuilder[Expression]

              // true if the expression is a list of identifiers
              def nextIdentifier: Boolean =
                scanner.lookAhead().token match
                  case WvletToken.COMMA =>
                    consume(WvletToken.COMMA)
                    nextIdentifier
                  case WvletToken.R_PAREN =>
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
              consume(WvletToken.R_PAREN)
              val args = exprs.result()
              val t3   = scanner.lookAhead()
              t3.token match
                case WvletToken.R_ARROW if isIdentifierList =>
                  // Lambda
                  consume(WvletToken.R_ARROW)
                  val body = expression()
                  LambdaExpr(args.map(_.asInstanceOf[Identifier]), body, spanFrom(t))
                case _ if args.size == 1 =>
                  ParenthesizedExpression(args.head, spanFrom(t))
                case _ =>
                  unexpected(t3)
            case _ =>
              val e = expression()
              consume(WvletToken.R_PAREN)
              ParenthesizedExpression(e, spanFrom(t))
          end match
        case WvletToken.L_BRACKET =>
          array()
        case WvletToken.MAP =>
          map()
        case id if id.isIdentifier =>
          identifier()
        case WvletToken.STAR | WvletToken.END =>
          identifier()
        case t if t.isNonReservedKeyword =>
          // For count(*), concat(...) expressions
          identifier()
        case _ =>
          unexpected(t)
    primaryExpressionRest(expr)

  end primaryExpression

  def inExprList(): List[Expression] =
    def rest(): List[Expression] =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.R_PAREN =>
          consume(WvletToken.R_PAREN)
          Nil
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          rest()
        case _ =>
          val e = valueExpression()
          e :: rest()

    consume(WvletToken.L_PAREN)
    rest()

  def arrayValue(): Values =
    val t      = consume(WvletToken.L_BRACKET)
    val values = List.newBuilder[ArrayConstructor]
    def nextValue: Unit =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          nextValue
        case WvletToken.R_BRACKET =>
        // ok
        case _ =>
          values += array()
          nextValue
    nextValue
    consume(WvletToken.R_BRACKET)
    Values(values.result(), spanFrom(t))

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
    ArrayConstructor(elements.result(), spanFrom(t))

  def struct(lBraceToken: TokenData[WvletToken]): StructValue =
    val fields = List.newBuilder[StructField]
    def nextField: Unit =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          nextField
        case WvletToken.R_BRACE =>
        // ok
        case s if s.isStringLiteral =>
          val name = consumeToken().str
          consume(WvletToken.COLON)
          val value = expression()
          fields += StructField(name, value, spanFrom(t))
          nextField
        case s if s.isStringStart =>
          val name = identifier()
          consume(WvletToken.COLON)
          val value = expression()
          fields += StructField(name.fullName, value, spanFrom(t))
          nextField
        case _ =>
          unexpected(t)

    nextField
    consume(WvletToken.R_BRACE)
    StructValue(fields.result(), spanFrom(lBraceToken))

  def map(): MapValue =
    val entries = List.newBuilder[MapEntry]
    def nextEntry: Unit =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.COMMA =>
          consume(WvletToken.COMMA)
          nextEntry
        case WvletToken.R_BRACE =>
        // ok
        case _ =>
          val key = identifier()
          consume(WvletToken.COLON)
          val value = expression()
          entries += MapEntry(key, value, spanFrom(t))
          nextEntry

    val t = consume(WvletToken.MAP)
    consume(WvletToken.L_BRACE)
    nextEntry
    consume(WvletToken.R_BRACE)
    MapValue(entries.result(), spanFrom(t))

  def dataType(): DataType =
    val tpe    = identifier().fullName
    val params = typeParams()
    DataTypeParser.parse(tpe, params)

  def literal(): Literal =
    def removeUnderscore(s: String): String = s.replaceAll("_", "")

    def literalRest(l: Literal): Literal =
      scanner.lookAhead().token match
        case WvletToken.COLON =>
          consume(WvletToken.COLON)
          val tpe = dataType()
          tpe.typeName.name match
            case _ =>
              GenericLiteral(tpe, l.stringValue, l.span)
        case _ =>
          l

    val t = consumeToken()
    val l =
      t.token match
        case WvletToken.NULL =>
          NullLiteral(spanFrom(t))
        case WvletToken.INTEGER_LITERAL =>
          LongLiteral(removeUnderscore(t.str).toLong, t.str, spanFrom(t))
        case WvletToken.DOUBLE_LITERAL =>
          DoubleLiteral(t.str.toDouble, t.str, spanFrom(t))
        case WvletToken.FLOAT_LITERAL =>
          DoubleLiteral(t.str.toFloat, t.str, spanFrom(t))
        case WvletToken.DECIMAL_LITERAL =>
          DecimalLiteral(removeUnderscore(t.str), t.str, spanFrom(t))
        case WvletToken.EXP_LITERAL =>
          DecimalLiteral(t.str, t.str, spanFrom(t))
        case WvletToken.SINGLE_QUOTE_STRING =>
          SingleQuoteString(t.str, spanFrom(t))
        case WvletToken.DOUBLE_QUOTE_STRING =>
          DoubleQuoteString(t.str, spanFrom(t))
        case WvletToken.TRIPLE_QUOTE_STRING =>
          TripleQuoteString(t.str, spanFrom(t))
        case _ =>
          unexpected(t)

    literalRest(l)

  end literal

  def interpolatedString(): InterpolatedString =
    val prefix = consumeToken()
    val isTripleQuote =
      prefix.token match
        case WvletToken.TRIPLE_QUOTE_INTERPOLATION_PREFIX =>
          true
        case WvletToken.STRING_INTERPOLATION_PREFIX =>
          false
        case _ =>
          unexpected(prefix)
    val prefixNode = ResolvedIdentifier(prefix.str, NoType, prefix.span)
    val parts      = List.newBuilder[Expression]

    def nextPart(): Unit =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.STRING_PART =>
          val part = consume(WvletToken.STRING_PART)
          parts += StringPart(part.str, part.span)
          nextPart()
        case WvletToken.DOLLAR =>
          consume(WvletToken.DOLLAR)
          consume(WvletToken.L_BRACE)
          val expr = expression()
          consume(WvletToken.R_BRACE)
          parts += expr
          nextPart()
        case _ =>

    while scanner.lookAhead().token == WvletToken.STRING_PART do
      nextPart()
    if scanner.lookAhead().token.isStringLiteral then
      val part = consumeToken()
      parts += StringPart(part.str, part.span)

    InterpolatedString(
      prefixNode,
      parts.result(),
      DataType.UnknownType,
      isTripleQuote = isTripleQuote,
      spanFrom(prefix)
    )

  end interpolatedString

  def interpolatedBackquoteString(): BackquoteInterpolatedIdentifier =
    val prefix     = consume(WvletToken.BACKQUOTE_INTERPOLATION_PREFIX)
    val prefixNode = ResolvedIdentifier(prefix.str, NoType, prefix.span)
    val parts      = List.newBuilder[Expression]

    def nextPart(): Unit =
      val t = scanner.lookAhead()
      t.token match
        case WvletToken.STRING_PART =>
          val part = consume(WvletToken.STRING_PART)
          parts += StringPart(part.str, part.span)
          nextPart()
        case WvletToken.DOLLAR =>
          consume(WvletToken.DOLLAR)
          consume(WvletToken.L_BRACE)
          val expr = expression()
          consume(WvletToken.R_BRACE)
          parts += expr
          nextPart()
        case _ =>

    while scanner.lookAhead().token == WvletToken.STRING_PART do
      nextPart()
    if scanner.lookAhead().token.isStringLiteral then
      val part = consumeToken()
      parts += StringPart(part.str, part.span)

    BackquoteInterpolatedIdentifier(
      prefixNode,
      parts.result(),
      DataType.UnknownType,
      spanFrom(prefix)
    )

  end interpolatedBackquoteString

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
            val sel  = DotRef(expr, next, DataType.UnknownType, spanFrom(t))
            val p    = consume(WvletToken.L_PAREN)
            val args = functionArgs()
            consume(WvletToken.R_PAREN)
            val w = window()
            val f = FunctionApply(sel, args, w, p.span)
            primaryExpressionRest(f)
          case _ =>
            primaryExpressionRest(DotRef(expr, next, DataType.UnknownType, spanFrom(t)))
      case WvletToken.L_PAREN =>
        expr match
          case n: NameExpr =>
            consume(WvletToken.L_PAREN)
            val args = functionArgs()
            consume(WvletToken.R_PAREN)
            // Global function call
            val w = window()
            val f = FunctionApply(n, args, w, spanFrom(t))
            primaryExpressionRest(f)
          case _ =>
            unexpected(expr)
      case WvletToken.L_BRACKET =>
        consume(WvletToken.L_BRACKET)
        val index = expression()
        consume(WvletToken.R_BRACKET)
        primaryExpressionRest(ArrayAccess(expr, index, spanFrom(t)))
      case WvletToken.R_ARROW if expr.isIdentifier =>
        consume(WvletToken.R_ARROW)
        val body = expression()
        primaryExpressionRest(
          LambdaExpr(args = List(expr.asInstanceOf[Identifier]), body, spanFrom(t))
        )
      case WvletToken.OVER =>
        window() match
          case Some(w) =>
            WindowApply(expr, w, spanFrom(t))
          case None =>
            expr
      case _ =>
        expr
    end match

  end primaryExpressionRest

  def functionArgs(): List[FunctionArg] =
    val t = scanner.lookAhead()
    t.token match
      case WvletToken.R_PAREN =>
        // ok
        Nil
      case _ =>
        val arg = functionArg()
        scanner.lookAhead().token match
          case WvletToken.COMMA =>
            consume(WvletToken.COMMA)
            arg :: functionArgs()
          case _ =>
            List(arg)

  def functionArg(isDistinct: Boolean = false): FunctionArg =
    val t = scanner.lookAhead()
    scanner.lookAhead().token match
      case id if id.isIdentifier =>
        val nameOrArg = expression()
        nameOrArg match
          case i: Identifier =>
            scanner.lookAhead().token match
              case WvletToken.EQ =>
                consume(WvletToken.EQ)
                val expr = expression()
                FunctionArg(Some(Name.termName(i.leafName)), expr, isDistinct, spanFrom(t))
              case _ =>
                FunctionArg(None, nameOrArg, isDistinct, spanFrom(t))
          case Eq(i: Identifier, v: Expression, span) =>
            FunctionArg(Some(Name.termName(i.leafName)), v, isDistinct, spanFrom(t))
          case expr: Expression =>
            FunctionArg(None, nameOrArg, isDistinct, spanFrom(t))
      case WvletToken.DISTINCT =>
        consume(WvletToken.DISTINCT)
        functionArg(isDistinct = true)
      case _ =>
        val nameOrArg = expression()
        FunctionArg(None, nameOrArg, isDistinct, spanFrom(t))

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
            DotRef(expr, Wildcard(spanFrom(t)), DataType.UnknownType, spanFrom(token))
          case _ =>
            val id = identifier()
            dotRef(DotRef(expr, id, DataType.UnknownType, spanFrom(token)))
      case _ =>
        expr

  def stringLiteral(): StringLiteral =
    val t = scanner.nextToken()
    t.token match
      case WvletToken.SINGLE_QUOTE_STRING =>
        SingleQuoteString(t.str, t.span)
      case WvletToken.DOUBLE_QUOTE_STRING =>
        DoubleQuoteString(t.str, t.span)
      case WvletToken.TRIPLE_QUOTE_STRING =>
        TripleQuoteString(t.str, t.span)
      case other =>
        unexpected(t)

end WvletParser
