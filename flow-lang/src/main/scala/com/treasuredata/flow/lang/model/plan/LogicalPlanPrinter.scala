package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.model.RelationType
import com.treasuredata.flow.lang.model.expr.*
import wvlet.log.LogSupport

import java.io.{PrintWriter, StringWriter}

object LogicalPlanPrinter extends LogSupport:

  extension (expr: Expression) def sqlExpr: String = printExpression(expr)

  def concat(lst: Seq[Any], sep: String = ""): String =
    val s = List.newBuilder[String]

    def add(x: Any): Unit = x match
      case str: String   => s += str
      case e: Expression => s += printExpression(e)

      case Some(e)                     => add(e)
      case None                        =>
      case Nil                         =>
      case lst: Seq[?] if lst.nonEmpty => lst.foreach(add)
      case null                        =>
      case _                           => s += x.toString

    lst.foreach(add)
    s.result().mkString(sep)

  def print(m: LogicalPlan): String =
    val s = new StringWriter()
    val p = new PrintWriter(s)
    print(m, p, 0)
    p.close()
    s.toString

  def print(m: LogicalPlan, out: PrintWriter, level: Int): Unit =
    def printChildExprs(children: Seq[Expression]): Unit =
      val attr    = children.map(x => printExpression(x))
      val ws      = "  " * (level + 1)
      val attrStr = attr.map(x => s"${ws}- ${x}").mkString("\n")
      out.println(attrStr)

    m match
      case null | EmptyRelation(_) =>
      // print nothing
//      case f: FunctionDef =>
//        val rt = f.resultType.map(x => s": ${x}").getOrElse("")
//        out.println(s"[FunctionDef] ${f.name}")
//        out.println(
//          s"  def ${f.name}(${f.args.map(x => s"${x.name}").mkString(", ")})${rt} = ${printExpression(f.bodyExpr)}"
//        )
      case t: TypeDef =>
        val s = concat(
          List(
            "  " * level,
            "[TypeDef (",
            t.nodeLocation,
            ")] ",
            t.name,
            if t.scopes.isEmpty then Nil else List("(in ", concat(t.scopes, ", "), ")"),
            if t.parents.isEmpty then Nil else List(" extends ", concat(t.parents, ", "))
          )
        )
        out.println(s)
        printChildExprs(t.elems)
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
            printChildExprs(m.childExpressions)
        for c <- m.children do print(c, out, level + 1)

  private def printExpression(e: Expression): String =
    e match
      case i: Identifier => i.expr
//      case f: FunctionCall =>
//        f.toString
      case d: DefArg =>
        concat(
          List(
            d.name,
            ": ",
            d.tpe,
            if d.defaultValue.isEmpty then Nil else List(" = ", d.defaultValue)
          )
        )
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
        concat(
          List(
            "def ",
            t.name.sqlExpr,
            if t.args.isEmpty then Nil else List("(", concat(t.args, ", "), ")"),
            if t.retType.isEmpty then Nil else List(": ", t.retType),
            if t.expr.isEmpty then Nil else List(" = ", t.expr)
          )
        )
      case t: TypeValDef =>
        concat(
          List(
            "val ",
            t.name,
            ": ",
            t.tpe,
            if t.body.isEmpty then Nil else List(" = ", t.body)
          )
        )
      case c: ConditionalExpression =>
        printConditionalExpression(c)
      case b: BinaryExpression =>
        s"${b.operatorName}(${printExpression(b.left)}, ${printExpression(b.right)})"
      case p: ParenthesizedExpression =>
        s"(${p.child.sqlExpr})"
      case null => "null"
      case arg: FunctionArg =>
        arg.name match
          case Some(name) =>
            concat(List(name, " = ", arg.value))
          case None =>
            concat(List(arg.value))
      case id: Identifier => id.value
      case i: InterpolatedString =>
        s"${printExpression(i.prefix)}{${i.parts.map(printExpression).mkString(", ")}}"
      case r: Ref =>
        s"${printExpression(r.base)}.${printExpression(r.name)}"
      case t: This =>
        "this"
      case d: DefScope =>
        if d.name.isEmpty then concat(List(d.tpe)) else concat(List(d.name, ": ", d.tpe), ", ")
      case other => e.toString

  def printConditionalExpression(c: ConditionalExpression): String =
    c match
      case NoOp(_) => ""
      case Eq(a, b, _) =>
        b match
          case n: NullLiteral =>
            s"IsNull(${a.sqlExpr})"
          case _ =>
            s"Eq(${a.sqlExpr}, ${b.sqlExpr})"
      case n @ NotEq(a, b, _) =>
        b match
          case n: NullLiteral =>
            s"NotNull(${a.sqlExpr})"
          case _ =>
            s"NotEq(${a.sqlExpr}, ${b.sqlExpr})"
      case And(a, b, _) =>
        s"And(${a.sqlExpr}, ${b.sqlExpr})"
      case Or(a, b, _) =>
        s"Or(${a.sqlExpr}, ${b.sqlExpr})"
      case Not(e, _) =>
        s"Not(${e.sqlExpr})"
      case LessThan(a, b, _) =>
        s"${a.sqlExpr} < ${b.sqlExpr}"
      case LessThanOrEq(a, b, _) =>
        s"${a.sqlExpr} <= ${b.sqlExpr}"
      case GreaterThan(a, b, _) =>
        s"${a.sqlExpr} > ${b.sqlExpr}"
      case GreaterThanOrEq(a, b, _) =>
        s"${a.sqlExpr} >= ${b.sqlExpr}"
      case Between(e, a, b, _) =>
        s"${e.sqlExpr} between ${a.sqlExpr} and ${b.sqlExpr}"
      case NotBetween(e, a, b, _) =>
        s"${e.sqlExpr} not between ${a.sqlExpr} and ${b.sqlExpr}"
      case IsNull(a, _) =>
        s"IsNull(${a.sqlExpr})"
      case IsNotNull(a, _) =>
        s"IsNotNull(${a.sqlExpr})"
      case In(a, list, _) =>
        val in = list.map(x => x.sqlExpr).mkString(", ")
        s"${a.sqlExpr} in (${in})"
      case NotIn(a, list, _) =>
        val in = list.map(x => x.sqlExpr).mkString(", ")
        s"${a.sqlExpr} not in (${in})"
//      case InSubQuery(a, in, _) =>
//        s"${a.sqlExpr} IN (${printRelation(in)})"
//      case NotInSubQuery(a, in, _) =>
//        s"${a.sqlExpr} NOT IN (${printRelation(in)})"
      case Like(a, e, _) =>
        s"${a.sqlExpr} like ${e.sqlExpr}"
      case NotLike(a, e, _) =>
        s"${a.sqlExpr} not like ${e.sqlExpr}"
      case DistinctFrom(a, e, _) =>
        s"${a.sqlExpr} is distinct from ${e.sqlExpr}"
      case NotDistinctFrom(a, e, _) =>
        s"${a.sqlExpr} is not distinct from ${e.sqlExpr}"
      case other => other.toString
