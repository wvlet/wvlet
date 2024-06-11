package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.parser.FlowToken.FROM
import com.treasuredata.flow.lang.compiler.{CompilationUnit, SourceFile}
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.plan.*
import wvlet.log.LogSupport

class Parsers(unit: CompilationUnit) extends LogSupport:

  given src: SourceFile                  = unit.sourceFile
  given compilationUnit: CompilationUnit = unit

  private val scanner = Scanner(unit.sourceFile, ScannerConfig(skipComments = true))

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
    val t = scanner.nextToken()
    t.token match
      case FlowToken.IDENTIFIER =>
        UnquotedIdentifier(t.str, t.nodeLocation)
      case FlowToken.STAR =>
        Wildcard(t.nodeLocation)
      case FlowToken.UNDERSCORE =>
        ContextRef(t.nodeLocation)
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
      case _ => ???

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
    val expr = simpleExpression()
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

  def simpleExpression(): Expression =
    val t = scanner.lookAhead()
    val expr = t.token match
      case FlowToken.NULL =>
        consume(FlowToken.NULL)
        NullLiteral(t.nodeLocation)
      case FlowToken.INTEGER_LITERAL =>
        consume(FlowToken.INTEGER_LITERAL)
        LongLiteral(t.str.toInt, t.nodeLocation)
      case FlowToken.DOUBLE_LITERAL =>
        consume(FlowToken.DOUBLE_LITERAL)
        DoubleLiteral(t.str.toDouble, t.nodeLocation)
      case FlowToken.FLOAT_LITERAL =>
        consume(FlowToken.FLOAT_LITERAL)
        DoubleLiteral(t.str.toFloat, t.nodeLocation)
      case FlowToken.DECIMAL_LITERAL =>
        consume(FlowToken.DECIMAL_LITERAL)
        DecimalLiteral(t.str, t.nodeLocation)
      case FlowToken.EXP_LITERAL =>
        consume(FlowToken.EXP_LITERAL)
        DecimalLiteral(t.str, t.nodeLocation)
      case FlowToken.STRING_LITERAL =>
        consume(FlowToken.STRING_LITERAL)
        StringLiteral(t.str, t.nodeLocation)
      case FlowToken.IDENTIFIER =>
        identifier()
      case FlowToken.L_PAREN =>
        consume(FlowToken.L_PAREN)
        val e = expression()
        consume(FlowToken.R_PAREN)
        ParenthesizedExpression(e, t.nodeLocation)
      case FlowToken.UNDERSCORE =>
        consume(FlowToken.UNDERSCORE)
        ContextRef(t.nodeLocation)
      case _ =>
        unexpected(t)
    simpleExpressionRest(expr)

  def nameExpression(): Name =
    simpleExpression() match
      case n: Name => n
      case other   => unexpected(other)

  def simpleExpressionRest(expr: Expression): Expression =
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

  def argExprs(): List[Expression] =
    ???

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
