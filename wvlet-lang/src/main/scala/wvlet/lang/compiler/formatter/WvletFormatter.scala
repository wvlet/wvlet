package wvlet.lang.compiler.formatter

import wvlet.lang.model.plan.*
import wvlet.lang.model.expr.*
import CodeFormatter.*
import wvlet.log.LogSupport

object WvletFormatter:

  sealed trait SyntaxContext:
    def inFrom: Boolean = this == InFromClause
    def isNested: Boolean = this != InStatement

  case object InStatement extends SyntaxContext
  case object InExpression extends SyntaxContext
  case object InFromClause extends SyntaxContext
  case object InSubQuery extends SyntaxContext

end WvletFormatter


class WvletFormatter(config: CodeFormatterConfig = CodeFormatterConfig()) extends CodeFormatter(config) with LogSupport:
  import WvletFormatter.*

  def format(l: LogicalPlan): String =
    val doc: Doc = convert(l)
    render(0, doc)

  def convert(l: LogicalPlan): Doc =
    def loop(plan: LogicalPlan): Doc =
      plan match
        case p: PackageDef =>
          val stmts = p.statements.map(stmt => loop(stmt))
          verticalConcat(stmts, text(";") + newline + newline)
        case r: Relation =>
          convertRelation(r)(using InStatement)
        case s: TopLevelStatement =>
          convertStatement(s)(using InStatement)
        case other =>
          warn(s"Unsupported plan: ${other}")
          Text(other.toString)

    loop(l)

  private def convertUnary(r: UnaryRelation, op: String, item: Expression)(using sc: SyntaxContext): Doc =
    convertUnary(r, op, List(item))

  private def convertUnary(r: UnaryRelation, op: String, items: List[Expression])(using sc: SyntaxContext): Doc =
    val in = convertRelation(r.child)
    val lst = items.map(convertExpression)
    val next = if lst.isEmpty then None else Some(nest(1, itemList(lst)))
    group(text(op) + newline + next)

  private def convertRelation(r: Relation)(using sc: SyntaxContext): Doc =
    r match
      case q: Query =>
        convertRelation(q.child)
      case s: Sort =>
        convertUnary(s, "sort", s.orderBy.toList)
      case p: Project =>
        convertUnary(p, "select", p.selectItems)
      case g: GroupBy =>
        convertUnary(g, "group by", g.groupingKeys)
      case a: Agg =>
        convertUnary(a, "agg", a.aggExprs)
      case t: Transform =>
        convertUnary(t, "transform", t.transformItems)
      case f: Filter =>
        convertUnary(f, "where", f.filterExpr)
      case l: Limit =>
        convertUnary(l, "limit", l.limit)
      case t: TableInput =>
        if sc.inFrom then
          convertExpression(t.sqlExpr)
        else
          group(text("from") + newline + nest(1, convertExpression(t.sqlExpr)))
      case a: AddColumnsToRelation =>
        convertUnary(a, "add", a.newColumns)
      case s: ShiftColumns =>
        if s.isLeftShift then
          convertUnary(s, "shift", s.shiftItems)
        else
          convertUnary(s, "shift to right", s.shiftItems)
      case e: ExcludeColumnsFromRelation =>
        convertUnary(e, "exclude", e.columnNames)
      case r: RenameColumnsFromRelation =>
        convertUnary(r, "rename", r.columnAliases)
      case j: Join =>
        val asof: Option[Doc] = if j.asof then Some(text("asof")) else None
        val left = convertRelation(j.left)
        val right = convertRelation(j.right)(using InFromClause)
        val joinType = j.joinType match
          case JoinType.InnerJoin => text("join")
          case JoinType.LeftOuterJoin => text("left join")
          case JoinType.RightOuterJoin => text("right join")
          case JoinType.FullOuterJoin => text("full join")
          case JoinType.CrossJoin => text("cross join")
          case JoinType.ImplicitJoin => text(",")

        val cond = j.cond match
          case NoJoinCriteria => None
          case n: NaturalJoin => None
          case u: JoinOnTheSameColumns =>
            if u.columns.size == 1 then
              Some(group(list("on", convertExpression(u.columns.head))))
            else
              Some(group(list("on", paren(args(u.columns.map(convertExpression))))))
          case u:JoinOn =>
            Some(group(list("on", convertExpression(u.expr))))
          case u:JoinOnEq =>
            Some(group(list("on", convertExpression(Expression.concatWithEq(u.keys)))))

        group(
          left + newline + joinType + newline + right + newline + cond
        )
      case u: Union if u.isDistinct =>
        // union is not supported in Wvlet, so rewrite it to dedup(concat)
        convertRelation(Dedup(Concat(u.left, u.right, u.span), u.span))
      case s: SetOperation =>
        val rels: List[Doc] =
          s.children.toList match
            case Nil => Nil
            case head :: tail =>
              val hd = convertRelation(head)
              val tl = tail.map(x => wrapWithBraceIfNecessary(convertRelation(x)))
              hd :: tl

        // TODO union is not supported in Wvlet. Replace tree to dedup(concat)
        val op = s.toWvOp
        group(
          verticalConcat(rels, text(op) + newline)
        )
      case d: Dedup =>
        convertUnary(d, "dedup", Nil)
      case d: Distinct =>
        convertUnary(d, "distinct", Nil)
      case d: Describe =>
        convertUnary(d, "describe", Nil)
      case s: SelectAsAlias =>
        convertUnary(s, "select as", s.alias)
      case t: TestRelation =>
        convertUnary(t, "test", t.testExpr)
      case d: Debug =>
        val r = convertRelation(d.child)
        r / group(
          brace(convertRelation(d.partialDebugExpr)(using InSubQuery))
        )
      case s: Sample =>
        val prev = convertRelation(s.child)
        prev / group(
          list(
            "sample",
            s.method.toString,
            s.size.toExpr
          )
        )
      case v: Values =>
        convertValues(v)
      case b: BracedRelation =>
        brace(convertRelation(b.child)(using InSubQuery))
      case a: AliasedRelation =>
        val tableAlias: Doc =
          val name = convertExpression(a.alias)
          a.columnNames match
            case Some(columns) =>
              val cols = args(columns.map(c => text(c.toSQLAttributeName)))
              name + paren(cols)
            case None =>
              name

        a.child match
          case t: TableInput =>
            list(convertExpression(t.sqlExpr), text("as"), tableAlias)
          case v: Values =>
            list(convertValues(v), text("as"), tableAlias)
          case _ =>
            list(
              wrapWithBraceIfNecessary(convertRelation(a.child)(using InSubQuery)),
              text("as"),
              tableAlias
            )
      case p: Pivot =>
        val prev = convertRelation(p.child)
        val pivotKeys = args(p.pivotKeys.map(convertExpression))
        // TODO Add grouping keys
        val groupingKeys = args(p.groupingKeys.map(convertExpression))
        prev /
        group(
          list("pivot", "on", pivotKeys)
        )
      case d: Delete =>
        convertUnary(d, "delete", Nil)
      case a: AppendTo =>
        convertRelation(a.child) / group(
          list("append to", convertExpression(a.target))
        )
      case a: AppendToFile =>
        convertRelation(a.child) / group(
          list("append to", s"'${a.path}'")
        )
      case s: SaveTo =>
        val prev = convertRelation(s.child)
        val path = convertExpression(s.target)
        val opts =
          if s.saveOptions.isEmpty then None
          else
            val lst = s.saveOptions.map { x => convertExpression(x) }
            Some(list("with", lst))
        prev / group(
          list("save to", path, opts)
        )
      case s: SaveToFile =>
        val prev = convertRelation(s.child)
        val path = s.targetName
        val opts =
          if s.saveOptions.isEmpty then None
          else
            val lst = s.saveOptions.map { x => convertExpression(x) }
            Some(list("with", lst))
        prev / group(
          list("save to", s"'${path}'", opts)
        )
      case e: EmptyRelation =>
        empty
      case s: Show =>
        list("show", s.showType.toString,
          s.inExpr.map(x =>
            list("in",
              convertExpression(x)
            )
          )
        )

      case other =>
        warn(s"Unsupported relation: ${other}")
        Text(other.toString)


  end convertRelation

  private def wrapWithBraceIfNecessary(d: Doc)(using sc:SyntaxContext): Doc =
    if sc.isNested then
      brace(d)
    else
      d


  private def convertValues(values: Values)(using sc: SyntaxContext): Doc =
    val rows: List[Doc] = values
            .rows
            .map { row =>
              row match
                case a: ArrayConstructor =>
                  val elems = args(a.values.map(x => convertExpression(x)))
                  bracket(elems)
                case other =>
                  convertExpression(other)
            }
    if sc.inFrom then
      bracket(args(rows))
    else
      group(text("from") + newline + nest(1, bracket(args(rows))))

  private def convertStatement(s: TopLevelStatement)(using sc: SyntaxContext): Doc = ???

  private def convertExpression(e: Expression)(using sc: SyntaxContext): Doc = ???




end WvletFormatter

