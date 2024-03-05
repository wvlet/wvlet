package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.model.expr.{
  Alias,
  ArithmeticBinaryExpr,
  ArrayConstructor,
  Attribute,
  BinaryExprType,
  BinaryExpression,
  Dereference,
  Expression,
  FunctionCall,
  GroupingKey,
  Identifier,
  Literal,
  SingleColumn,
  SortItem,
  StringLiteral
}
import wvlet.log.LogSupport

import java.io.{PrintWriter, StringWriter}

object LogicalPlanPrinter extends LogSupport:
  def print(m: LogicalPlan): String =
    val s = new StringWriter()
    val p = new PrintWriter(s)
    print(m, p, 0)
    p.close()
    s.toString

  def print(m: LogicalPlan, out: PrintWriter, level: Int): Unit =
    m match
      case EmptyRelation(_) =>
      // print nothing
      case _ =>
        val ws = "  " * level

        def wrap[A](s: Seq[A]): String =
          if s.length <= 1 then s.mkString(", ") else s"(${s.mkString(", ")})"

        val inputType = m match
          case r: Relation => wrap(r.inputRelationTypes)
          case _           => wrap(m.inputAttributes.map(_.typeDescription))

        val outputType = m match
          case r: Relation => r.relationType
          case _           => wrap(m.outputAttributes.map(_.typeDescription))

        val inputAttrs  = m.inputAttributes
        val outputAttrs = m.outputAttributes

        val attr        = m.childExpressions.map(expr => printExpression(expr))
        val functionSig = if inputAttrs.isEmpty && outputAttrs.isEmpty then "" else s": ${inputType} => ${outputType}"

        val prefix = m match
//          case t: TableScan =>
//            s"${ws}[${m.modelName}] ${t.table.fullName}${functionSig}"
          case _ =>
            s"${ws}[${m.modelName}]${functionSig}"

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
        val args = f.args.map(printExpression).mkString(", ")
        s"${f.name}(${args})"
      case d: Dereference =>
        s"${printExpression(d.base)}.${printExpression(d.next)}"
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
        s"def ${t.name}: ${t.tpe.getOrElse("?")} = ${printExpression(t.expr)}"
      case b: BinaryExpression =>
        s"${b.operatorName}(${printExpression(b.left)}, ${printExpression(b.right)})"
      case null  => "null"
      case other => e.toString
