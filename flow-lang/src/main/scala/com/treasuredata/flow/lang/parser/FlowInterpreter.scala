package com.treasuredata.flow.lang.parser

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.model.*
import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.TerminalNode
import wvlet.log.LogSupport

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
    trace(s"interpret: ${print(ctx)}")
    val m = ctx.accept(this)
    trace(m)
    m.asInstanceOf[Expression]

  private def visitIdentifier(ctx: IdentifierContext): Identifier =
    visit(ctx).asInstanceOf[Identifier]

  override def visitUnquotedIdentifier(ctx: UnquotedIdentifierContext): Identifier =
    UnquotedIdentifier(ctx.getText, getLocation(ctx))

  override def visitBackQuotedIdentifier(ctx: BackQuotedIdentifierContext): Identifier =
    BackQuotedIdentifier(unquote(ctx.getText), getLocation(ctx))

  override def visitStatements(ctx: StatementsContext): FlowPlan =
    val plans = ctx.singleStatement().asScala.map(s => visit(s).asInstanceOf[LogicalPlan]).toSeq
    FlowPlan(plans)

  override def visitSchemaDef(ctx: SchemaDefContext): SchemaDef =
    val schemaName = ctx.identifier().getText
    val columns = ctx
      .schemaElement().asScala
      .map { x =>
        val key   = visitIdentifier(x.identifier(0))
        val value = ColumnType(x.identifier(1).getText, getLocation(x.identifier(1)))
        ColumnDef(key, value, getLocation(x))
      }.toSeq
    SchemaDef(schemaName, columns, getLocation(ctx))

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

    val typeDefs: Seq[TypeDefDef] = ctx.typeDefElem().asScala.map(visitTypeDefElem).toSeq
    TypeDef(name, paramList, typeDefs, getLocation(ctx))

  override def visitTypeDefElem(ctx: TypeDefElemContext): TypeDefDef =
    val expr: Expression = expression(ctx.primaryExpression())
    ctx.identifier().asScala.toList match
      case head :: Nil =>
        TypeDefDef(visitIdentifier(head).value, None, expr, getLocation(ctx))
      case head :: tpe :: Nil =>
        TypeDefDef(visitIdentifier(head).value, Option(visitIdentifier(tpe)).map(_.value), expr, getLocation(ctx))
      case _ =>
        // unexpected
        TypeDefDef("", None, expr, getLocation(ctx))

  override def visitQuery(ctx: QueryContext): Relation =
    val filterExpr: Option[Expression] = Option(ctx.booleanExpression()).map { cond =>
      expression(cond)
    }
    val inputRelation: Relation = ctx.forExpr() match
      case f: ForInputContext =>
        null
      case f: FromInputContext =>
        interpretRelation(f.relation())

    var r: Relation = inputRelation

    if filterExpr.isDefined then r = Filter(r, filterExpr.get, getLocation(ctx.booleanExpression()))

    val selectItems: List[Attribute] =
      Option(ctx.selectExpr().selectItemList()) match
        case Some(lst) =>
          lst.selectItem().asScala.map { si => visit(si).asInstanceOf[Attribute] }.toList
        case None =>
          List(AllColumns(Qualifier.empty, None, getLocation(ctx)))

    Option(ctx.groupBy()) match
      case Some(groupBy) =>
        // aggregate
        val groupingKeys: List[GroupingKey] = groupBy
          .groupByItemList().groupByItem().asScala
          .map { gi => visitGroupByItem(gi) }
          .toList
        r = Aggregate(r, selectItems, groupingKeys, having = None, getLocation(ctx))
      case None =>
        r = Project(r, selectItems, getLocation(ctx))

    Option(ctx.selectExpr()).foreach { selectExpr =>
      val alias: Option[Identifier] = Option(selectExpr.identifier()).map { alias =>
        visitIdentifier(alias)
      }

      if alias.isDefined then NamedRelation(r, alias.get, getLocation(ctx))
      else r
    }
    Query(r, getLocation(ctx))

  override def visitGroupByItem(ctx: GroupByItemContext): GroupingKey =
    val alias = Option(ctx.identifier()).map(visitIdentifier)
    var expr  = expression(ctx.expression())
    UnresolvedGroupingKey(expr, getLocation(ctx))

  override def visitSelectSingle(ctx: SelectSingleContext): Attribute =
    val alias = Option(ctx.identifier())
      .map(visitIdentifier(_))
    val child = expression(ctx.expression())
    val qualifier = child match
      case a: Attribute => a.qualifier
      case _            => Qualifier.empty
    SingleColumn(child, qualifier, getLocation(ctx))
      .withAlias(alias.map(_.value))

  override def visitSelectAll(ctx: SelectAllContext): Attribute =
    val qualifier = Option(ctx.qualifiedName()).map(_.getText).map(Qualifier.parse).getOrElse(Qualifier.empty)
    AllColumns(qualifier, None, getLocation(ctx))

  private def interpretRelation(ctx: RelationContext): Relation =
    ctx match
      case r: RelationDefaultContext =>
        visitRelationDefault(r)
      case r: JoinRelationContext =>
        visitJoinRelation(r)
//      // case r: LateralViewContext =>
//      //        visitLateralView(r)
//      case r: ParenthesizedRelationContext =>
//        visitParenthesizedRelation(r)
//      case r: UnnestContext =>
//        visitUnnest(r)
//      case r: SubqueryRelationContext =>
//        visitSubqueryRelation(r)
//      case r: TableNameContext =>
//        visitTableName(r)
//      case r: LateralContext =>
//        visitLateral(r)
      case _ =>
        throw unknown(ctx)

  override def visitRelationDefault(ctx: RelationDefaultContext): Relation =
    visitAliasedRelation(ctx.aliasedRelation())

  override def visitAliasedRelation(ctx: AliasedRelationContext): Relation =
    val r: Relation = ctx.relationPrimary() match
      case p: ParenthesizedRelationContext =>
        ParenthesizedRelation(visit(p.relation()).asInstanceOf[Relation], getLocation(ctx))
      case u: UnnestContext =>
        val ord = Option(u.ORDINALITY()).map(x => true).getOrElse(false)
        Unnest(
          u.primaryExpression().asScala.toSeq.map(x => expression(x)),
          withOrdinality = ord,
          getLocation(ctx)
        )
      case s: SubqueryRelationContext =>
        visitQuery(s.query())
      case l: LateralContext =>
        Lateral(visitQuery(l.query()), getLocation(ctx))
      case t: TableNameContext =>
        TableRef(QName(t.qualifiedName().getText, getLocation(t)), getLocation(ctx))
      case other =>
        throw unknown(other)

    ctx.identifier() match
      case i: IdentifierContext =>
        val columnNames = Option(ctx.columnAliases()).map(_.identifier().asScala.map(_.getText).toSeq)
        // table alias name is always resolved identifier
        AliasedRelation(r, visitIdentifier(i).toResolved, columnNames, getLocation(ctx))
      case other =>
        r

  override def visitJoinRelation(ctx: JoinRelationContext): Relation =
    val tmpJoinType = ctx.joinType() match
      case null                     => None
      case jt if jt.LEFT() != null  => Some(LeftOuterJoin)
      case jt if jt.RIGHT() != null => Some(RightOuterJoin)
      case jt if jt.FULL() != null  => Some(FullOuterJoin)
      case _ if ctx.CROSS() != null => Some(CrossJoin)
      case _                        => None

    val (joinType, joinCriteria, right) = Option(ctx.joinCriteria()) match
      case Some(c) if c.ON() != null =>
        (
          tmpJoinType.getOrElse(InnerJoin),
          JoinUsing(c.identifier().asScala.toSeq.map(visitIdentifier), getLocation(ctx)),
          ctx.rightRelation
        )
      case Some(c) if c.booleanExpression() != null =>
        (
          tmpJoinType.getOrElse(InnerJoin),
          JoinOn(expression(c.booleanExpression()), getLocation(ctx)),
          ctx.rightRelation
        )
      case _ =>
        (CrossJoin, NaturalJoin(getLocation(ctx)), ctx.right)
    val l = visit(ctx.left).asInstanceOf[Relation]
    val r = visit(right).asInstanceOf[Relation]

    val j = Join(joinType, l, r, joinCriteria, getLocation(ctx))
    j

  override def visitQualifiedName(ctx: QualifiedNameContext): QName =
    QName(ctx.identifier().asScala.map(_.getText).toList, getLocation(ctx))

  override def visitDereference(ctx: DereferenceContext): Attribute =
    val qualifier = if ctx.base.getText.isEmpty then Qualifier.empty else Qualifier.parse(ctx.base.getText)
    val name      = QName.unquote(ctx.fieldName.getText)
    UnresolvedAttribute(qualifier, name, getLocation(ctx))

  private def expression(ctx: ParserRuleContext): Expression =
    ctx.accept(this).asInstanceOf[Expression]

  override def visitExpression(ctx: ExpressionContext): Expression =
    val b: BooleanExpressionContext = ctx.booleanExpression()
    b match
      case lb: LogicalBinaryContext =>
        if lb.AND() != null then And(expression(lb.left), expression(lb.right), getLocation(ctx))
        else if lb.OR() != null then Or(expression(lb.left), expression(lb.right), getLocation(ctx))
        else throw unknown(lb)
      case ln: LogicalNotContext =>
        visitLogicalNot(ln)
      case bd: BooleanDeafaultContext =>
        expression(bd.valueExpression())
      case other =>
        warn(s"Unknown expression: ${other.getClass}")
        visit(ctx.booleanExpression()).asInstanceOf[Expression]

  /*
  override def visitSelectAll(ctx: SelectAllContext): Attribute =
    // TODO parse qName
    val qualifier = Option(ctx.qualifiedName()).map(_.getText)
    AllColumns(qualifier, None, None, getLocation(ctx))

  override def visitSelectSingle(ctx: SelectSingleContext): Attribute =
    val alias = Option(ctx.AS())
      .map(_ => visitIdentifier(ctx.identifier()))
      .orElse(Option(ctx.identifier()).map(visitIdentifier))
    val child = expression(ctx.expression())
    val qualifier = child match
      case a: Attribute => a.qualifier
      case _            => None
    SingleColumn(child, qualifier, None, getLocation(ctx))
      .withAlias(alias.map(_.value))
   */

  override def visitLogicalNot(ctx: LogicalNotContext): Expression =
    Not(expression(ctx.booleanExpression()), getLocation(ctx))

  override def visitValueExpressionDefault(ctx: ValueExpressionDefaultContext): Expression =
    expression(ctx.primaryExpression())

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
    StringLiteral(unquote(ctx.STRING().getText), getLocation(ctx))

  override def visitUnicodeStringLiteral(ctx: UnicodeStringLiteralContext): StringLiteral =
    // Decode unicode literal
    StringLiteral(ctx.getText, getLocation(ctx))

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
    ParenthesizedExpression(expression(ctx.expression()), getLocation(ctx))

  override def visitSubqueryExpression(ctx: SubqueryExpressionContext): Expression =
    SubQueryExpression(visitQuery(ctx.query()), getLocation(ctx))

//  override def visitSubquery(ctx: SubqueryContext): LogicalPlan =
//    visitQueryNoWith(ctx.queryNoWith())
//
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
    val left  = expression(ctx.left)
    val right = expression(ctx.right)
    ctx.operator.getType match
      case FlowLangParser.AND =>
        And(left, right, getLocation(ctx))
      case FlowLangParser.OR =>
        Or(left, right, getLocation(ctx))

  override def visitArithmeticBinary(ctx: ArithmeticBinaryContext): Expression =
    val left  = expression(ctx.left)
    val right = expression(ctx.right)
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
    val left  = expression(ctx.left)
    val right = expression(ctx.right)
    val op    = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    op.getSymbol.getType match
      case FlowLangParser.EQ =>
        Eq(left, right, getLocation(ctx.comparisonOperator()))
      case FlowLangParser.LT =>
        LessThan(left, right, getLocation(ctx.comparisonOperator()))
      case FlowLangParser.LTE =>
        LessThanOrEq(left, right, getLocation(ctx.comparisonOperator()))
      case FlowLangParser.GT =>
        GreaterThan(left, right, getLocation(ctx.comparisonOperator()))
      case FlowLangParser.GTE =>
        GreaterThanOrEq(left, right, getLocation(ctx.comparisonOperator()))
      case FlowLangParser.NEQ =>
        NotEq(left, right, op.getText, getLocation(ctx.comparisonOperator()))

//  override def visitExists(ctx: ExistsContext): Expression =
//    Exists(
//      SubQueryExpression(visitQuery(ctx.query()), getLocation(ctx)),
//      getLocation(ctx)
//    )

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
    LongLiteral(ctx.getText.replaceAll("_", "").toLong, getLocation(ctx))

  override def visitStringLiteral(ctx: StringLiteralContext): Literal =
    val text = ctx.str().getText.replaceAll("(^'|'$)", "")
    StringLiteral(text, getLocation(ctx))

//  override def visitQuotedIdentifier(ctx: QuotedIdentifierContext): Identifier =
//    QuotedIdentifier(ctx.getText.replaceAll("(^\"|\"$)", ""), getLocation(ctx))
//
//  override def visitDigitIdentifier(ctx: DigitIdentifierContext): Identifier =
//    DigitId(ctx.getText, getLocation(ctx))

//  override def visitOver(ctx: OverContext): Window =
//    // PARTITION BY
//    val partition = Option(ctx.PARTITION())
//      .map { p =>
//        ctx.partition.asScala.map(expression(_)).toSeq
//      }
//      .getOrElse(Seq.empty)
//    val orderBy = Option(ctx.ORDER())
//      .map { o =>
//        ctx.sortItem().asScala.map(visitSortItem(_)).toSeq
//      }
//      .getOrElse(Seq.empty)
//    val windowFrame = Option(ctx.windowFrame()).map(visitWindowFrame(_))
//
//    Window(partition, orderBy, windowFrame, getLocation(ctx))
//
//  override def visitWindowFrame(ctx: WindowFrameContext): WindowFrame =
//    val s = visitFrameBound(ctx.start)
//    val e = Option(ctx.BETWEEN()).map { x =>
//      visitFrameBound(ctx.end)
//    }
//    if ctx.RANGE() != null then WindowFrame(RangeFrame, s, e, getLocation(ctx))
//    else WindowFrame(RowsFrame, s, e, getLocation(ctx))

//  private def visitFrameBound(ctx: FrameBoundContext): FrameBound =
//    ctx match
//      case bf: BoundedFrameContext =>
//        val bound: Long = expression(bf.expression()) match
//          case l: LongLiteral =>
//            l.value
//          case other =>
//            throw new IllegalArgumentException(s"Unknown bound context: ${other}")
//        if bf.PRECEDING() != null then Preceding(bound)
//        else if bf.FOLLOWING() != null then Following(bound)
//        else throw unknown(bf)
//      case ub: UnboundedFrameContext =>
//        if ub.PRECEDING() != null then UnboundedPreceding
//        else if ub.FOLLOWING() != null then UnboundedFollowing
//        else throw unknown(ctx)
//      case cb: CurrentRowBoundContext =>
//        CurrentRow
//
//  override def visitBoundedFrame(ctx: BoundedFrameContext): Expression =
//    super.visitBoundedFrame(ctx).asInstanceOf[Expression]

  override def visitFunctionCall(ctx: FunctionCallContext): FunctionCall =
    val name = ctx.qualifiedName().getText
    val args = ctx.valueExpression().asScala.map(expression(_)).toSeq
    FunctionCall(name, args, getLocation(ctx))

//  override def visitSubstring(ctx: SubstringContext): Any =
//    FunctionCall(
//      ctx.SUBSTRING().getText,
//      ctx.valueExpression.asScala.map(expression(_)).toSeq,
//      getLocation(ctx)
//    )

//  override def visitSetQuantifier(ctx: SetQuantifierContext): SetQuantifier =
//    if ctx.DISTINCT() != null then DistinctSet(getLocation(ctx))
//    else All(getLocation(ctx))

  override def visitNullLiteral(ctx: NullLiteralContext): Literal = NullLiteral(getLocation(ctx))

//  override def visitInterval(ctx: IntervalContext): IntervalLiteral =
//    val sign =
//      if ctx.MINUS() != null then Negative
//      else Positive
//
//    val value = ctx.str().getText
//
//    val from = visitIntervalField(ctx.from)
//    val to   = Option(ctx.TO()).map(x => visitIntervalField(ctx.intervalField(0)))
//
//    IntervalLiteral(unquote(value), sign, from, to, getLocation(ctx))
//
//  override def visitIntervalField(ctx: IntervalFieldContext): IntervalField =
//    if ctx.YEAR() != null then Year(getLocation(ctx.YEAR()))
//    else if ctx.MONTH() != null then Month(getLocation(ctx.MONTH()))
//    else if ctx.DAY() != null then Day(getLocation(ctx.DAY()))
//    else if ctx.HOUR() != null then Hour(getLocation(ctx.HOUR()))
//    else if ctx.MINUTE() != null then Minute(getLocation(ctx.MINUTE()))
//    else if ctx.SECOND() != null then Second(getLocation(ctx.SECOND()))
//    else throw unknown(ctx)
//
//  override def visitArrayConstructor(ctx: ArrayConstructorContext): Expression =
//    val elems = ctx.expression().asScala.map(expression(_)).toSeq
//    ArrayConstructor(elems, getLocation(ctx))
//
//  override def visitCreateSchema(ctx: CreateSchemaContext): LogicalPlan =
//    val schemaName  = visitQualifiedName(ctx.qualifiedName())
//    val ifNotExists = Option(ctx.EXISTS()).map(_ => true).getOrElse(false)
//    val props = Option(ctx.properties())
//      .map(
//        _.property().asScala
//          .map { p =>
//            val key   = visitIdentifier(p.identifier())
//            val value = expression(p.expression())
//            SchemaProperty(key, value, getLocation(p))
//          }.toSeq
//      )
//    CreateSchema(schemaName, ifNotExists, props, getLocation(ctx))
//
//  override def visitDropSchema(ctx: DropSchemaContext): LogicalPlan =
//    val schemaName = visitQualifiedName(ctx.qualifiedName())
//    val ifExists   = Option(ctx.EXISTS()).map(x => true).getOrElse(false)
//    val cascade =
//      Option(ctx.CASCADE()).map(x => true).getOrElse(false)
//    DropSchema(schemaName, ifExists, cascade, getLocation(ctx))
//
//  override def visitRenameSchema(ctx: RenameSchemaContext): LogicalPlan =
//    val schemaName = visitQualifiedName(ctx.qualifiedName())
//    val renameTo   = visitIdentifier(ctx.identifier())
//    RenameSchema(schemaName, renameTo, getLocation(ctx))
//
//  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan =
//    val ifNotExists = Option(ctx.EXISTS()).map(x => true).getOrElse(false)
//    val tableName   = visitQualifiedName(ctx.qualifiedName())
//    val tableElements =
//      ctx.tableElement().asScala.toSeq.map(x => visitTableElement(x))
//    CreateTable(tableName, ifNotExists, tableElements, getLocation(ctx))
//
//  override def visitCreateTableAsSelect(ctx: CreateTableAsSelectContext): LogicalPlan =
//    val ifNotExists = Option(ctx.EXISTS()).map(x => true).getOrElse(false)
//    val tableName   = visitQualifiedName(ctx.qualifiedName())
//    val columnAliases = Option(ctx.columnAliases())
//      .map(_.identifier().asScala.toSeq.map(visitIdentifier(_)))
//    val q = visitQuery(ctx.query())
//    CreateTableAs(tableName, ifNotExists, columnAliases, q, getLocation(ctx))
//
//  override def visitTableElement(ctx: TableElementContext): TableElement =
//    Option(ctx.columnDefinition())
//      .map(x => visitColumnDefinition(x))
//      .getOrElse {
//        val l         = ctx.likeClause()
//        val tableName = visitQualifiedName(l.qualifiedName())
//        val includingProps =
//          Option(l.EXCLUDING()).map(x => false).getOrElse(true)
//        ColumnDefLike(tableName, includingProps, getLocation(ctx))
//      }
//
//  override def visitColumnDefinition(ctx: ColumnDefinitionContext): ColumnDef =
//    val name = visitIdentifier(ctx.identifier())
//    val tpe  = visitType(ctx.`type`())
//    ColumnDef(name, tpe, getLocation(ctx))
//
//  override def visitType(ctx: TypeContext): ColumnType =
//    ColumnType(ctx.getText, getLocation(ctx))
//
//  override def visitDropTable(ctx: DropTableContext): LogicalPlan =
//    val table    = visitQualifiedName(ctx.qualifiedName())
//    val ifExists = Option(ctx.EXISTS()).map(x => true).getOrElse(false)
//    DropTable(table, ifExists, getLocation(ctx))
//
//  override def visitInsertInto(ctx: InsertIntoContext): LogicalPlan =
//    val table = visitQualifiedName(ctx.qualifiedName())
//    val aliases = Option(ctx.columnAliases())
//      .map(x => x.identifier().asScala.toSeq)
//      .map(x => x.map(visitIdentifier(_)))
//    val query = visitQuery(ctx.query())
//    InsertInto(table, aliases, query, getLocation(ctx))
//
//  override def visitDelete(ctx: DeleteContext): LogicalPlan =
//    val table = visitQualifiedName(ctx.qualifiedName())
//    val cond = Option(ctx.booleanExpression()).map { x =>
//      expression(x)
//    }
//    Delete(table, cond, getLocation(ctx))
//
//  override def visitRenameTable(ctx: RenameTableContext): LogicalPlan =
//    val from = visitQualifiedName(ctx.qualifiedName(0))
//    val to   = visitQualifiedName(ctx.qualifiedName(1))
//    RenameTable(from, to, getLocation(ctx))
//
//  override def visitRenameColumn(ctx: RenameColumnContext): LogicalPlan =
//    val table = visitQualifiedName(ctx.tableName)
//    val from  = visitIdentifier(ctx.from)
//    val to    = visitIdentifier(ctx.to)
//    RenameColumn(table, from, to, getLocation(ctx))
//
//  override def visitDropColumn(ctx: DropColumnContext): LogicalPlan =
//    val table = visitQualifiedName(ctx.tableName)
//    val c     = visitIdentifier(ctx.column)
//    DropColumn(table, c, getLocation(ctx))
//
//  override def visitAddColumn(ctx: AddColumnContext): LogicalPlan =
//    val table  = visitQualifiedName(ctx.tableName)
//    val coldef = visitColumnDefinition(ctx.column)
//    AddColumn(table, coldef, getLocation(ctx))
//
//  override def visitCreateView(ctx: CreateViewContext): LogicalPlan =
//    val viewName = visitQualifiedName(ctx.qualifiedName())
//    val replace  = Option(ctx.REPLACE()).map(x => true).getOrElse(false)
//    val query    = visitQuery(ctx.query())
//    CreateView(viewName, replace, query, getLocation(ctx))
//
//  override def visitDropView(ctx: DropViewContext): LogicalPlan =
//    val viewName = visitQualifiedName(ctx.qualifiedName())
//    val ifExists = Option(ctx.EXISTS()).map(x => true).getOrElse(false)
//    DropView(viewName, ifExists, getLocation(ctx))
//
