package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.CompilationUnit
import com.treasuredata.flow.lang.model.NodeLocation
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.plan.*
import wvlet.log.LogSupport

class Parsers(unit: CompilationUnit) extends LogSupport:

  private val scanner = Scanner(unit.sourceFile)

  def parse(): LogicalPlan =
    val token = scanner.lookAhead()
    token.token match
      case FlowToken.PACKAGE => parsePackage()
      case _                 => LogicalPlan.empty

  private def nodeLocation(): Option[NodeLocation] =
    val t = scanner.currentToken
    t.token match
      case FlowToken.EMPTY => None
      case _ =>
        Some(NodeLocation(unit.sourceFile.startOfLine(t.offset), unit.sourceFile.offsetToColumn(t.offset)))

  def termIdentifier(): Identifier =
    val t = scanner.nextToken()
    t.token match
      case FlowToken.IDENTIFIER =>
        UnquotedIdentifier(t.str, nodeLocation())
      case _ => ???

  /**
    * PackageDef := 'package' qualifiedId (statement)*
    */
  def parsePackage(): PackageDef =
    val t   = scanner.nextToken()
    val loc = nodeLocation()
    val packageName: Option[Expression] = t.token match
      case FlowToken.PACKAGE =>
        val packageName = qualifiedId()
        Some(packageName)
      case _ =>
        None

    val stmts = parseStatements()
    // TODO Pass package name as tree
    PackageDef(packageName, stmts, unit.sourceFile, loc)

  def parseStatements(): Seq[LogicalPlan] =
    var stmts = Seq.empty[LogicalPlan]
    var t     = scanner.nextToken()
    Seq.empty

  /**
    * qualifiedId := identifier ('.' identifier)*
    */
  def qualifiedId(): Expression = dotRef(termIdentifier())

  /**
    * dotRef := '.' identifier
    * @param expr
    * @return
    */
  def dotRef(expr: Expression): Expression =
    val t = scanner.lookAhead()
    t.token match
      case FlowToken.DOT =>
        scanner.nextToken()
        val id = termIdentifier()
        dotRef(Ref(expr, id, nodeLocation()))
      case _ =>
        expr
