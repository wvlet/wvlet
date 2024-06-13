package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.model.RelationType
import com.treasuredata.flow.lang.model.expr.*
import wvlet.log.LogSupport

import java.io.{PrintWriter, StringWriter}

object LogicalPlanPrinter extends LogSupport:

  extension (expr: Expression) def sqlExpr: String = printExpression(expr)

  def print(m: LogicalPlan): String =
    val s = new StringWriter()
    val p = new PrintWriter(s)
    print(m, p, 0)
    p.close()
    s.toString

  def print(m: LogicalPlan, out: PrintWriter, level: Int): Unit =
    m match
      case null | EmptyRelation(_) =>
      // print nothing
      case f: FunctionDef =>
        val rt = f.resultType.map(x => s": ${x}").getOrElse("")
        out.println(s"[FunctionDef] ${f.name}")
        out.println(
          s"  def ${f.name}(${f.args.map(x => s"${x.name}: ${x.tpe}").mkString(", ")})${rt} = ${printExpression(f.bodyExpr)}"
        )
      case _ =>
        val ws = "  " * level

        def wrap[A](s: Seq[A]): String =
          if s.length <= 1 then s.mkString(", ") else s"(${s.mkString(", ")})"

        def printRelationType(r: RelationType): String =
          s"<${r}${if r.isResolved then ">" else ">?"}"

        val inputType = m match
          case r: Relation => wrap(r.inputRelationTypes.map(printRelationType))
          case _           => wrap(m.inputAttributes.map(_.typeDescription))

        val outputType = m match
          case r: Relation => printRelationType(r.relationType)
          case _           => wrap(m.outputAttributes.map(_.typeDescription))

        val inputAttrs  = m.inputAttributes
        val outputAttrs = m.outputAttributes

        val attr        = m.childExpressions.map(expr => printExpression(expr))
        val functionSig = s" ${inputType} => ${outputType}"

        val loc = m.nodeLocation.map(l => s" (${l})").getOrElse("")
        val prefix = m match
          case t: TableScan =>
            s"${ws}[${m.modelName}${loc}] ${t.name}${functionSig}"
          case _ =>
            s"${ws}[${m.modelName}${loc}]${functionSig}"

        attr.length match
          case 0 =>
            out.println(prefix)
          case _ =>
            out.println(s"${prefix}")
            val attrWs  = "  " * (level + 1)
            val attrStr = attr.map(x => s"${attrWs}- ${x}").mkString("\n")
            out.println(attrStr)
        for c <- m.children do print(c, out, level + 1)

  private def printExpression(e: Expression): String =
    e match
      case i: Identifier => i.expr
      case f: FunctionCall =>
        f.toString
      case d: Dereference =>
        s"Dereference(${printExpression(d.base)}, ${printExpression(d.next)})"
      case s: SingleColumn =>
        s"${s.fullName}:${s.dataTypeName} := ${printExpression(s.expr)}"
      case a: Alias =>
        s"<${a.fullName}> ${printExpression(a.expr)}"
      case g: GroupingKey =>
        printExpression(g.child)
      case b: ArithmeticBinaryExpr =>
        s"${printExpression(b.left)} ${b.exprType.symbol} ${printExpression(b.right)}"
      case s: StringLiteral =>
        s"\"${s.stringValue}\""
      case l: Literal =>
        l.stringValue
      case s: SortItem =>
        s"sort key:${printExpression(s.sortKey)}${s.ordering.map(x => s" ${x}").getOrElse("")}"
      case a: ArrayConstructor =>
        s"[${a.children.map(printExpression).mkString(", ")}]"
      case t: TypeDefDef =>
        val defdef = s"def ${t.name}: ${t.tpe.getOrElse("?")}"
        t.expr match
          case Some(expr) => s"${defdef} = ${printExpression(expr)}"
          case None       => defdef
      case c: ConditionalExpression =>
        printConditionalExpression(c)
      case b: BinaryExpression =>
        s"${b.operatorName}(${printExpression(b.left)}, ${printExpression(b.right)})"
      case p: ParenthesizedExpression =>
        s"(${p.child.sqlExpr})"
      case null  => "null"
      case other => e.toString

  def printConditionalExpression(c: ConditionalExpression): String =
    c match
      case NoOp(_) => ""
      case Eq(a, b, _) =>
        b match
          case n: NullLiteral =>
            s"${a.sqlExpr} IS ${b.sqlExpr}"
          case _ =>
            s"${a.sqlExpr} = ${b.sqlExpr}"
      case n @ NotEq(a, b, _) =>
        b match
          case n: NullLiteral =>
            s"${a.sqlExpr} IS NOT ${b.sqlExpr}"
          case _ =>
            s"${a.sqlExpr} != ${b.sqlExpr}"
      case And(a, b, _) =>
        s"${a.sqlExpr} AND ${b.sqlExpr}"
      case Or(a, b, _) =>
        s"${a.sqlExpr} OR ${b.sqlExpr}"
      case Not(e, _) =>
        s"NOT ${e.sqlExpr}"
      case LessThan(a, b, _) =>
        s"${a.sqlExpr} < ${b.sqlExpr}"
      case LessThanOrEq(a, b, _) =>
        s"${a.sqlExpr} <= ${b.sqlExpr}"
      case GreaterThan(a, b, _) =>
        s"${a.sqlExpr} > ${b.sqlExpr}"
      case GreaterThanOrEq(a, b, _) =>
        s"${a.sqlExpr} >= ${b.sqlExpr}"
      case Between(e, a, b, _) =>
        s"${e.sqlExpr} BETWEEN ${a.sqlExpr} and ${b.sqlExpr}"
      case NotBetween(e, a, b, _) =>
        s"${e.sqlExpr} NOT BETWEEN ${a.sqlExpr} and ${b.sqlExpr}"
      case IsNull(a, _) =>
        s"${a.sqlExpr} IS NULL"
      case IsNotNull(a, _) =>
        s"${a.sqlExpr} IS NOT NULL"
      case In(a, list, _) =>
        val in = list.map(x => x.sqlExpr).mkString(", ")
        s"${a.sqlExpr} IN (${in})"
      case NotIn(a, list, _) =>
        val in = list.map(x => x.sqlExpr).mkString(", ")
        s"${a.sqlExpr} NOT IN (${in})"
//      case InSubQuery(a, in, _) =>
//        s"${a.sqlExpr} IN (${printRelation(in)})"
//      case NotInSubQuery(a, in, _) =>
//        s"${a.sqlExpr} NOT IN (${printRelation(in)})"
      case Like(a, e, _) =>
        s"${a.sqlExpr} LIKE ${e.sqlExpr}"
      case NotLike(a, e, _) =>
        s"${a.sqlExpr} NOT LIKE ${e.sqlExpr}"
      case DistinctFrom(a, e, _) =>
        s"${a.sqlExpr} IS DISTINCT FROM ${e.sqlExpr}"
      case NotDistinctFrom(a, e, _) =>
        s"${a.sqlExpr} IS NOT DISTINCT FROM ${e.sqlExpr}"
      case other => other.toString
