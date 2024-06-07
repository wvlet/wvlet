package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.{CompilationUnit, SourceFile, SourceLocation}
import com.treasuredata.flow.lang.model.NodeLocation
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.plan.*
import wvlet.log.LogSupport

class Parsers(unit: CompilationUnit) extends LogSupport:

  given src: SourceFile                  = unit.sourceFile
  given compilationUnit: CompilationUnit = unit

  private val scanner = Scanner(unit.sourceFile)

  def parse(): LogicalPlan =
    val token = scanner.lookAhead()
    token.token match
      case FlowToken.PACKAGE => packageDef()
      case _                 => LogicalPlan.empty

  // private def sourceLocation: SourceLocation = SourceLocation(unit.sourceFile, nodeLocation())

  def consume(expected: FlowToken): TokenData =
    val t = scanner.nextToken()
    if t.token == expected then t
    else throw StatusCode.SYNTAX_ERROR.newException(s"Expected ${expected}, but found ${t.token}", t.sourceLocation)

  def identifier(): Identifier =
    val t = scanner.nextToken()
    t.token match
      case FlowToken.IDENTIFIER =>
        UnquotedIdentifier(t.str, t.nodeLocation)
      case _ => ???

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
    scanner.lookAhead().token match
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
        FileScan(t.str, t.nodeLocation)
      case _ => ???

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
