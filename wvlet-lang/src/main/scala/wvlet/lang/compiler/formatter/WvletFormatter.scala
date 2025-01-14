package wvlet.lang.compiler.formatter

import wvlet.lang.model.plan.*
import wvlet.lang.model.expr.*
import CodeFormatter.*
import wvlet.log.LogSupport

object WvletFormatter:

  sealed trait SyntaxContext:
    def inFrom: Boolean = this == InFromClause

  case object InStatement extends SyntaxContext
  case object InExpression extends SyntaxContext
  case object InFromClause extends SyntaxContext

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
          horizontalConcat(stmts, text(";") + newline + newline)
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
              Some(group(text("on") + ws + convertExpression(u.columns.head)))
            else
              Some(group(text("on") + ws + text("(") + newline +
                      nest(1, functionArgs(u.columns.map(convertExpression)))
              + newline + text(")")))
          case u:JoinOn =>
            Some(group(text("on") + ws + convertExpression(u.expr)))
          case u:JoinOnEq =>
            Some(group(text("on") + ws + convertExpression(Expression.concatWithEq(u.keys))))

        group(
          left + newline + joinType + newline + right + newline + cond
        )
      case s: SetOperation =>
        val rels: List[Doc] =
          s.children.toList match
            case Nil => Nil
            case head :: tail =>
              val hd = convertRelation(head)
              val tl = tail.map(x => wrapWithBlockIfNecessary(convertRelation(x)))
              hd :: tl

        val op = s.toWvOp
        group(
          horizontalConcat(rels, text(op) + newline)
        )
      case d: Dedup =>
        convertUnary(d, "dedup", Nil)


  end convertRelation

  private def wrapWithBlockIfNecessary(d: Doc)(using sc:SyntaxContext): Doc =
    d match
      case Text(_) => d
      case _ => group(text("{") + newline + nest(1, d) + newline + text("}"))


  private def convertStatement(s: TopLevelStatement)(using sc: SyntaxContext): Doc = ???

  private def convertExpression(e: Expression)(using sc: SyntaxContext): Doc = ???




end WvletFormatter

