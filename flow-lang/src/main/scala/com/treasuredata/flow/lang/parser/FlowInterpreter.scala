package com.treasuredata.flow.lang.parser

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.model.*
import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.TerminalNode
import wvlet.log.LogSupport

import javax.swing.SortOrder
import scala.jdk.CollectionConverters.*

object FlowInterpreter:
  private val parserRules = FlowLangParser.ruleNames.toList.asJava

  private def unquote(s: String): String =
    s.substring(1, s.length - 1)

/**
  * Translate ANTLR4's parse tree into LogicalPlan and Expression nodes
  */
class FlowInterpreter extends FlowLangBaseVisitor[Any] with LogSupport:
  import FlowInterpreter.*
  import FlowLangParser.*
  import com.treasuredata.flow.lang.model.*

  private def print(ctx: ParserRuleContext): String =
    ctx.toStringTree(parserRules)

  private def unknown(ctx: ParserRuleContext): Exception =
    new IllegalArgumentException("Unknown parser context: " + ctx.toStringTree(parserRules))

  private def getLocation(token: Token): Option[NodeLocation] =
    Some(NodeLocation(token.getLine, token.getCharPositionInLine + 1))

  private def getLocation(ctx: ParserRuleContext): Option[NodeLocation] = getLocation(ctx.getStart)

  private def getLocation(node: TerminalNode): Option[NodeLocation] = getLocation(node.getSymbol)

  def interpret(ctx: ParserRuleContext): FlowPlan =
    trace(s"interpret: ${print(ctx)}")
    val m = ctx.accept(this)
    debug(m)
    m.asInstanceOf[FlowPlan]

  def interpretExpression(ctx: ParserRuleContext): Expression =
    ctx.accept(this).asInstanceOf[Expression]

  private def visitIdentifier(ctx: IdentifierContext): Identifier =
    ctx match
      case u: UnquotedIdentifierContext     => visitUnquotedIdentifier(u)
      case b: BackQuotedIdentifierContext   => visitBackQuotedIdentifier(b)
      case j: ReservedWordIdentifierContext => visitReservedWordIdentifier(j)
      case _                                => throw unknown(ctx)

  override def visitUnquotedIdentifier(ctx: UnquotedIdentifierContext): Identifier =
    UnquotedIdentifier(ctx.getText, getLocation(ctx))

  override def visitBackQuotedIdentifier(ctx: BackQuotedIdentifierContext): Identifier =
    BackQuotedIdentifier(unquote(ctx.getText), getLocation(ctx))

  override def visitReservedWordIdentifier(ctx: ReservedWordIdentifierContext): Identifier =
    UnquotedIdentifier(ctx.getText, getLocation(ctx))

  override def visitStatements(ctx: StatementsContext): FlowPlan =
    val plans = ctx.singleStatement().asScala.map(s => visit(s).asInstanceOf[LogicalPlan]).toSeq
    FlowPlan(plans)

  override def visitTypeAlias(ctx: TypeAliasContext): TypeAlias =
    TypeAlias(
      alias = visitIdentifier(ctx.alias).value,
      sourceTypeName = visitIdentifier(ctx.sourceType).value,
      getLocation(ctx)
    )

//  override def visitSchemaDef(ctx: SchemaDefContext): SchemaDef =
//    val schemaName = ctx.identifier().getText
//    val columns = ctx
//      .schemaElement().asScala
//      .map { x =>
//        val key   = visitIdentifier(x.identifier(0))
//        val value = ColumnType(x.identifier(1).getText, getLocation(x.identifier(1)))
//        ColumnDef(key, value, getLocation(x))
//      }.toSeq
//    SchemaDef(schemaName, columns, getLocation(ctx))

  override def visitTypeDef(ctx: TypeDefContext): TypeDef =
    val name: String = visitIdentifier(ctx.identifier()).value
    val paramList: Seq[TypeParam] =
      Option(ctx.paramList())
        .flatMap(p => Option(p.param()).map(_.asScala.toSeq))
        .getOrElse(Seq.empty[ParamContext])
        .map { p =>
          TypeParam(
            name = visitIdentifier(p.identifier(0)).value,
            value = visitIdentifier(p.identifier(1)).value,
            nodeLocation = getLocation(p)
          )
        }

    val typeElems: Seq[TypeElem] = ctx
      .typeElem().asScala
      .map(e => e.accept(this))
      .collect { case e: TypeElem => e }
      .toSeq
    TypeDef(name, paramList, typeElems, getLocation(ctx))

  override def visitTypeDefDef(ctx: TypeDefDefContext): TypeDefDef =
    val expr: Expression = interpretExpression(ctx.primaryExpression())
    ctx.identifier().asScala.toList match
      case head :: Nil =>
        TypeDefDef(visitIdentifier(head).value, None, expr, getLocation(ctx))
      case head :: tpe :: Nil =>
        TypeDefDef(visitIdentifier(head).value, Option(visitIdentifier(tpe)).map(_.value), expr, getLocation(ctx))
      case _ =>
        // unexpected
        TypeDefDef("", None, expr, getLocation(ctx))

  override def visitTypeValDef(ctx: TypeValDefContext): TypeValDef =
    TypeValDef(
      visitIdentifier(ctx.columnName).value,
      visitIdentifier(ctx.typeName).value,
      getLocation(ctx)
    )

  override def visitFunctionDef(ctx: FunctionDefContext): FunctionDef =
    val name       = visitIdentifier(ctx.name).value
    val resultType = Option(ctx.resultTypeName).map(visitIdentifier(_).value)
    val params = ctx
      .paramList().param().asScala.map { p =>
        val paramName = visitIdentifier(p.identifier(0)).value
        val paramType = visitIdentifier(p.identifier(1)).value
        FunctionArg(paramName, paramType, getLocation(p))
      }.toSeq
    val body = interpretExpression(ctx.body)
    FunctionDef(name, params, resultType, body, getLocation(ctx))

  override def visitTableDef(ctx: TableDefContext): TableDef =
    val tableName = visitIdentifier(ctx.identifier()).value
    val params = ctx
      .tableParam().asScala
      .map { x =>
        val key   = visitIdentifier(x.identifier()).value
        val value = interpretExpression(x.primaryExpression())
        TableDefParam(key, value, getLocation(x))
      }.toSeq
    TableDef(tableName, params, getLocation(ctx))

  override def visitQuery(ctx: QueryContext): Relation =
    val inputRelation: Relation = interpretRelation(ctx.relation())
    var r: Relation             = inputRelation

    Option(ctx.queryBlock()).foreach { queryBlock =>
      r = queryBlock.asScala.foldLeft(r) { (rel, qb) =>
        interpretQueryBlock(rel, qb)
      }
    }
    val sortItems: List[SortItem] = ctx.sortItem().asScala.map(interpretSortItem).toList
    if sortItems.nonEmpty then r = Sort(r, sortItems, getLocation(ctx.sortItem(0)))

    Option(ctx.INTEGER_VALUE()).foreach { iv =>
      val limit = interpretIntegerValue(iv, ctx)
      r = Limit(r, limit, getLocation(ctx))
    }
    Query(r, getLocation(ctx))

  private def interpretSortItem(ctx: SortItemContext): SortItem =
    val expr = interpretExpression(ctx.expression())
    val ordering: Option[SortOrdering] = Option(ctx.ordering).map { x =>
      if ctx.ASC() != null then SortOrdering.Ascending
      else SortOrdering.Descending
    }
    SortItem(expr, ordering, None, getLocation(ctx))

  private def interpretQueryBlock(inputRelation: Relation, ctx: QueryBlockContext): Relation =
    ctx match
      case f: FilterRelationContext =>
        val filterExpr = interpretExpression(f.booleanExpression())
        Filter(inputRelation, filterExpr, getLocation(f))
      case p: ProjectRelationContext =>
        val r = Option(p.selectExpr().selectItemList()) match
          case Some(lst) =>
            val selectItems = lst.selectItem().asScala.map { si => visit(si).asInstanceOf[Attribute] }.toList
            Project(inputRelation, selectItems, getLocation(p))
          case None =>
            inputRelation
        Option(p.selectExpr().identifier()) match
          case Some(id) =>
            val alias = visitIdentifier(id)
            NamedRelation(r, alias, getLocation(p))
          case None =>
            r
      case a: AggregateRelationContext =>
        val groupingKeys: List[GroupingKey] =
          a.groupByItemList().groupByItem().asScala.map { gi => visitGroupByItem(gi) }.toList
        // TODO parse having
        Aggregate(inputRelation, groupingKeys, having = None, getLocation(a))
      case t: TransformRelationContext =>
        val transformItems = t.transformExpr().selectItemList().selectItem().asScala
        val lst            = transformItems.map { ti => visit(ti).asInstanceOf[Attribute] }.toList
        Transform(inputRelation, lst, getLocation(t))
      case j: JoinRelationContext =>
        interpretJoin(inputRelation, j.join())
      case l: LimitRelationContext =>
        val limit = interpretIntegerValue(l.INTEGER_VALUE(), l)
        Limit(inputRelation, limit, getLocation(l))
      case s: SubscribeRelationContext =>
        val name = visitIdentifier(s.subscribeExpr().identifier()).value
        val params = s
          .subscribeExpr().subscribeParam().asScala.map { p =>
            val name = visitIdentifier(p.identifier()).value
            val expr = interpretExpression(p.primaryExpression())
            SubscribeParam(name, expr, getLocation(p))
          }.toSeq
        Subscribe(inputRelation, name, params, getLocation(s))
      case _ =>
        throw unknown(ctx)

  override def visitGroupByItem(ctx: GroupByItemContext): GroupingKey =
    var expr = interpretExpression(ctx.expression())
    Option(ctx.identifier()).map(visitIdentifier).foreach { id =>
      expr = Alias(Qualifier.empty, id.value, expr, getLocation(ctx))
    }
    UnresolvedGroupingKey(expr, getLocation(ctx))

  override def visitSelectSingle(ctx: SelectSingleContext): Attribute =
    val alias = Option(ctx.identifier())
      .map(visitIdentifier(_))
    val child = interpretExpression(ctx.expression())
    val qualifier = child match
      case a: Attribute => a.qualifier
      case _            => Qualifier.empty
    SingleColumn(child, qualifier, getLocation(ctx))
      .withAlias(alias.map(_.value))

  override def visitSelectAll(ctx: SelectAllContext): Attribute =
    val qualifier = Option(ctx.qualifiedName()).map(_.getText).map(Qualifier.parse).getOrElse(Qualifier.empty)
    AllColumns(qualifier, None, getLocation(ctx))

  private def interpretRelation(ctx: RelationContext): Relation =
    var r = interpretRelationPrimary(ctx.relationPrimary())
    Option(ctx.identifier()).foreach { alias =>
      val columnNames = Option(ctx.columnAliases()).map(_.identifier().asScala.map(_.getText).toSeq)
      r = AliasedRelation(r, visitIdentifier(alias), columnNames, getLocation(ctx))
    }
    r

  private def interpretRelationPrimary(ctx: RelationPrimaryContext): Relation =
    ctx match
      case p: ParenthesizedRelationContext =>
        ParenthesizedRelation(visit(p.relation()).asInstanceOf[Relation], getLocation(ctx))
      case s: SubqueryRelationContext =>
        visitQuery(s.query())
      case t: TableNameContext =>
        TableRef(QName(t.qualifiedName().getText, getLocation(t)), getLocation(ctx))
      case other =>
        throw unknown(other)

  private def interpretJoin(inputRelation: Relation, ctx: JoinContext): Relation =
    val tmpJoinType = ctx.joinType() match
      case null                     => None
      case jt if jt.LEFT() != null  => Some(LeftOuterJoin)
      case jt if jt.RIGHT() != null => Some(RightOuterJoin)
      case jt if jt.FULL() != null  => Some(FullOuterJoin)
      case _ if ctx.CROSS() != null => Some(CrossJoin)
      case _                        => None

    val rightRelation = interpretRelation(ctx.relation())

    val (joinType, joinCriteria) = Option(ctx.joinCriteria()) match
      case Some(c) if c.ON() != null && c.booleanExpression() != null =>
        (
          tmpJoinType.getOrElse(InnerJoin),
          JoinOn(interpretExpression(c.booleanExpression()), getLocation(ctx))
        )
      case Some(c) if c.ON() != null && c.identifier() != null =>
        (
          tmpJoinType.getOrElse(InnerJoin),
          JoinUsing(c.identifier().asScala.map(visitIdentifier).toList, getLocation(ctx))
        )
      case _ =>
        (CrossJoin, NaturalJoin(getLocation(ctx)))

    val j = Join(joinType, inputRelation, rightRelation, joinCriteria, getLocation(ctx))
    j

  override def visitQualifiedName(ctx: QualifiedNameContext): QName =
    QName(ctx.identifier().asScala.map(_.getText).toList, getLocation(ctx))

  override def visitDereference(ctx: DereferenceContext): Expression =
    val base = interpretExpression(ctx.base)
    val next = interpretExpression(ctx.next)
    Dereference(base, next, getLocation(ctx))

  override def visitExpression(ctx: ExpressionContext): Expression =
    val b: BooleanExpressionContext = ctx.booleanExpression()
    b match
      case lb: LogicalBinaryContext =>
        if lb.AND() != null then And(interpretExpression(lb.left), interpretExpression(lb.right), getLocation(ctx))
        else if lb.OR() != null then Or(interpretExpression(lb.left), interpretExpression(lb.right), getLocation(ctx))
        else throw unknown(lb)
      case ln: LogicalNotContext =>
        visitLogicalNot(ln)
      case bd: BooleanDeafaultContext =>
        interpretExpression(bd.valueExpression())
      case other =>
        warn(s"Unknown expression: ${other.getClass}")
        visit(ctx.booleanExpression()).asInstanceOf[Expression]

  override def visitLogicalNot(ctx: LogicalNotContext): Expression =
    Not(interpretExpression(ctx.booleanExpression()), getLocation(ctx))

  override def visitValueExpressionDefault(ctx: ValueExpressionDefaultContext): Expression =
    interpretExpression(ctx.primaryExpression())

//  override def visitTypeConstructor(ctx: TypeConstructorContext): Expression =
//    val v = expression(ctx.str()).asInstanceOf[StringLiteral].value
//
//    if ctx.DOUBLE_PRECISION() != null then
//      // TODO Parse double-type precision properly
//      GenericLiteral("DOUBLE", v, getLocation(ctx))
//    else
//      val tpe = ctx.identifier().getText
//      tpe.toLowerCase match
//        case "time"      => TimeLiteral(v, getLocation(ctx))
//        case "timestamp" => TimestampLiteral(v, getLocation(ctx))
//        // TODO Parse decimal-type precision properly
//        case "decimal" => DecimalLiteral(v, getLocation(ctx))
//        case "char"    => CharLiteral(v, getLocation(ctx))
//        case other =>
//          GenericLiteral(tpe, v, getLocation(ctx))

  override def visitBasicStringLiteral(ctx: BasicStringLiteralContext): StringLiteral =
    StringLiteral(unquote(ctx.getText), getLocation(ctx))

//  override def visitUnicodeStringLiteral(ctx: UnicodeStringLiteralContext): StringLiteral =
//    // Decode unicode literal
//    StringLiteral(ctx.getText, getLocation(ctx))

  override def visitBinaryLiteral(ctx: BinaryLiteralContext): Expression =
    BinaryLiteral(ctx.BINARY_LITERAL().getText, getLocation(ctx))

//  override def visitParameter(ctx: ParameterContext): Expression =
//    // Prepared statement parameter
//    parameterPosition += 1
//    Parameter(parameterPosition, getLocation(ctx))
//
//  override def visitSimpleCase(ctx: SimpleCaseContext): Expression =
//    val operand       = expression(ctx.valueExpression())
//    val whenClauses   = ctx.whenClause().asScala.map(visitWhenClause(_)).toSeq
//    val defaultClause = Option(ctx.elseExpression).map(expression(_))
//
//    CaseExpr(Some(operand), whenClauses, defaultClause, getLocation(ctx))
//
//  override def visitWhenClause(ctx: WhenClauseContext): WhenClause =
//    WhenClause(expression(ctx.condition), expression(ctx.result), getLocation(ctx))
//
//  override def visitSearchedCase(ctx: SearchedCaseContext): Expression =
//    val whenClauses    = ctx.whenClause().asScala.map(visitWhenClause(_)).toSeq
//    val defaultClauses = Option(ctx.elseExpression).map(expression(_))
//
//    CaseExpr(None, whenClauses, defaultClauses, getLocation(ctx))
//
//  override def visitCast(ctx: CastContext): Expression =
//    if ctx.CAST() != null then
//      Cast(expression(ctx.expression()), ctx.`type`().getText, tryCast = false, getLocation(ctx))
//    else if ctx.TRY_CAST() != null then
//      Cast(expression(ctx.expression()), ctx.`type`().getText, tryCast = true, getLocation(ctx))
//    else throw unknown(ctx)

  override def visitParenthesizedExpression(ctx: ParenthesizedExpressionContext): Expression =
    ParenthesizedExpression(interpretExpression(ctx.expression()), getLocation(ctx))

  override def visitSubqueryExpression(ctx: SubqueryExpressionContext): Expression =
    SubQueryExpression(visitQuery(ctx.query()), getLocation(ctx))

  override def visitArrayConstructor(ctx: ArrayConstructorContext): Expression =
    val values = ctx.expression().asScala.map(interpretExpression(_)).toSeq
    ArrayConstructor(values, getLocation(ctx))

//  override def visitConcatenation(ctx: ConcatenationContext): Expression =
//    FunctionCall(
//      "concat",
//      ctx.valueExpression().asScala.map(expression(_)).toSeq,
//      isDistinct = false,
//      Option.empty,
//      Option.empty,
//      getLocation(ctx)
////    )
//
//  override def visitPredicated(ctx: PredicatedContext): Expression =
//    val e = expression(ctx.valueExpression)
//    if ctx.predicate != null then
//      // TODO add predicate
//      ctx.predicate() match
//        case n: NullPredicateContext =>
//          if n.NOT() == null then IsNull(e, getLocation(n))
//          else IsNotNull(e, getLocation(n))
//        case b: BetweenContext =>
//          if b.NOT() != null then NotBetween(e, expression(b.lower), expression(b.upper), getLocation(b))
//          else Between(e, expression(b.lower), expression(b.upper), getLocation(b))
//        case i: InSubqueryContext =>
//          val subQuery = visitQuery(i.query())
//          if i.NOT() == null then InSubQuery(e, subQuery, getLocation(i))
//          else NotInSubQuery(e, subQuery, getLocation(i))
//        case i: InListContext =>
//          val inList = i.expression().asScala.map(x => expression(x)).toSeq
//          if i.NOT() == null then In(e, inList, getLocation(i))
//          else NotIn(e, inList, getLocation(i))
//        case l: LikeContext =>
//          // TODO: Handle ESCAPE
//          val likeExpr = expression(l.pattern)
//          if l.NOT() == null then Like(e, likeExpr, getLocation(l))
//          else NotLike(e, likeExpr, getLocation(l))
//        case d: DistinctFromContext =>
//          val distinctExpr = expression(d.valueExpression())
//          if d.NOT() == null then DistinctFrom(e, distinctExpr, getLocation(d))
//          else NotDistinctFrom(e, distinctExpr, getLocation(d))
//        case other =>
//          // TODO
//          warn(s"unhandled predicate ${ctx.predicate().getClass}:\n${print(ctx.predicate())}")
//          e
//    else e

  override def visitLogicalBinary(ctx: LogicalBinaryContext): Expression =
    val left  = interpretExpression(ctx.left)
    val right = interpretExpression(ctx.right)
    ctx.operator.getType match
      case FlowLangParser.AND =>
        And(left, right, getLocation(ctx))
      case FlowLangParser.OR =>
        Or(left, right, getLocation(ctx))

  override def visitArithmeticBinary(ctx: ArithmeticBinaryContext): Expression =
    val left  = interpretExpression(ctx.left)
    val right = interpretExpression(ctx.right)
    val binaryExprType: BinaryExprType =
      ctx.operator match
        case op if ctx.PLUS() != null     => Add
        case op if ctx.MINUS() != null    => Subtract
        case op if ctx.ASTERISK() != null => Multiply
        case op if ctx.SLASH() != null    => Divide
        case op if ctx.PERCENT() != null  => Modulus
        case _ =>
          throw unknown(ctx)
    ArithmeticBinaryExpr(binaryExprType, left, right, getLocation(ctx))

  override def visitComparison(ctx: ComparisonContext): Expression =
    trace(s"comparison: ${print(ctx)}")
    val left  = interpretExpression(ctx.left)
    val right = interpretExpression(ctx.right)
    val op    = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    val loc   = getLocation(ctx.comparisonOperator())
    op.getSymbol.getType match
      case FlowLangParser.EQ =>
        Eq(left, right, getLocation(ctx))
      case FlowLangParser.IS =>
        Option(ctx.getChild(1)) match
          case None =>
            Eq(left, right, getLocation(ctx))
          case Some(op) =>
            NotEq(left, right, getLocation(ctx))
      case FlowLangParser.LT =>
        LessThan(left, right, getLocation(ctx))
      case FlowLangParser.LTE =>
        LessThanOrEq(left, right, getLocation(ctx))
      case FlowLangParser.GT =>
        GreaterThan(left, right, getLocation(ctx))
      case FlowLangParser.GTE =>
        GreaterThanOrEq(left, right, getLocation(ctx))
      case FlowLangParser.NEQ =>
        NotEq(left, right, getLocation(ctx))

  override def visitBooleanLiteral(ctx: BooleanLiteralContext): Literal =
    if ctx.booleanValue().TRUE() != null then TrueLiteral(getLocation(ctx))
    else FalseLiteral(getLocation(ctx))

  override def visitNumericLiteral(ctx: NumericLiteralContext): Literal =
    visit(ctx.number()).asInstanceOf[Literal]

  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal =
    DoubleLiteral(ctx.getText.toDouble, getLocation(ctx))

  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal =
    DecimalLiteral(ctx.getText, getLocation(ctx))

  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Literal =
    interpretIntegerValue(ctx.INTEGER_VALUE(), ctx)

  override def visitStringLiteral(ctx: StringLiteralContext): Literal =
    val text = ctx.str().getText.replaceAll("(^'|'$)", "")
    StringLiteral(text, getLocation(ctx))

  private def interpretIntegerValue(v: TerminalNode, ctx: ParserRuleContext): LongLiteral =
    LongLiteral(v.getText.replaceAll("_", "").toLong, getLocation(ctx))

//  override def visitQuotedIdentifier(ctx: QuotedIdentifierContext): Identifier =
//    QuotedIdentifier(ctx.getText.replaceAll("(^\"|\"$)", ""), getLocation(ctx))
//
//  override def visitDigitIdentifier(ctx: DigitIdentifierContext): Identifier =
//    DigitId(ctx.getText, getLocation(ctx))

  override def visitFunctionCall(ctx: FunctionCallContext): FunctionCall =
    val name = visitIdentifier(ctx.identifier()).value
    val args = ctx.valueExpression().asScala.map(interpretExpression(_)).toSeq
    FunctionCall(name, args, getLocation(ctx))

  override def visitNullLiteral(ctx: NullLiteralContext): Literal = NullLiteral(getLocation(ctx))
