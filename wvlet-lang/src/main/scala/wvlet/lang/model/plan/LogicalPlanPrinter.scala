/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.model.plan

import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.{DataType, RelationType}
import wvlet.lang.model.expr.*
import wvlet.lang.api.Span
import wvlet.log.LogSupport

import java.io.{PrintWriter, StringWriter}

object LogicalPlanPrinter extends LogSupport:

  extension (expr: Expression)
    def sqlExpr: String = printExpression(expr)

  def concat(lst: Seq[Any], sep: String = ""): String =
    val s = List.newBuilder[String]

    def add(x: Any): Unit =
      x match
        case str: String =>
          s += str
        case e: Expression =>
          s += printExpression(e)

        case Some(e) =>
          add(e)
        case None =>
        case Nil  =>
        case lst: Seq[?] if lst.nonEmpty =>
          lst.foreach(add)
        case null =>
        case _ =>
          s += x.toString

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
            "[TypeDef ",
            t.span,
            "] ",
            t.name,
            if t.params.isEmpty then
              Nil
            else
              List("[", t.params.map(_.typeDescription).mkString(", "), "]")
            ,
            if t.defContexts.isEmpty then
              Nil
            else
              List(" in ", concat(t.defContexts, ", "))
            ,
            if t.parent.isEmpty then
              Nil
            else
              List(" extends ", t.parent)
          )
        )
        out.println("")
        out.println(s)
        printChildExprs(t.elems)
      case _ =>
        val ws = "  " * level

        def wrap[A](v: A): String =
          v match
            case s: Seq[String] @unchecked if s.length <= 1 =>
              s.headOption.map(_.toString).getOrElse("empty")
            case s: Seq[String] =>
              s"(${s.mkString(", ")})"
            case other =>
              other.toString

        def printRelationType(r: RelationType): String = s"<${r.typeDescription}>"

        val inputType =
          m.inputRelationType match
            case EmptyRelationType =>
              ""
            case other: RelationType =>
              wrap(printRelationType(other))

        val outputType =
          m.relationType match
            case DataType.EmptyRelationType =>
              ""
            case r: RelationType =>
              wrap(printRelationType(r))

        val inputAttrs  = m.inputRelationType.fields
        val outputAttrs = m.relationType.fields

        val attr        = m.childExpressions.map(expr => printExpression(expr))
        val functionSig = s" ${inputType} => ${outputType}"

        val loc = m.span.map(l => s" ${l}").getOrElse("")
        val prefix =
          m match
            case t: HasTableName =>
              s"${ws}[${m.modelName}${loc}] ${t.name}${functionSig}"
            case src: HasSourceFile =>
              s"${ws}[${m.modelName} ${src.sourceFile.fileName}${loc}]${functionSig} "
            case _ =>
              s"${ws}[${m.modelName}${loc}]${functionSig}"

        m match
          case p: PackageDef =>
          // do not add new line
          case m: ModelDef =>
            out.println()
          case l: LanguageStatement =>
            // add a new line for language statement
            out.println()
          case _ =>

        attr.length match
          case 0 =>
            out.println(prefix)
          case _ =>
            out.println(s"${prefix}")
            printChildExprs(m.childExpressions)

        for c <- m.children do
          // Add indent for child relations
          print(c, out, level + 1)

    end match

  end print

  def printExpression(e: Expression): String =
    e match
      case i: Identifier =>
        i.strExpr
//      case f: FunctionCall =>
//        f.toString
      case d: DefArg =>
        concat(
          List(
            d.name,
            ": ",
            d.dataType.typeDescription,
            if d.defaultValue.isEmpty then
              Nil
            else
              List(" = ", d.defaultValue)
          )
        )
//      case d: Dereference =>
//        s"Dereference(${printExpression(d.base)}, ${printExpression(d.next)})"
      case s: SingleColumn =>
        s"${s.fullName}:${s.dataTypeName} := ${printExpression(s.expr)}"
      case a: Alias =>
        s"<${a.fullName}> ${printExpression(a.expr)}"
      case g: GroupingKey =>
        printExpression(g.child)
      case b: ArithmeticBinaryExpr =>
        s"${printExpression(b.left)} ${b.exprType.expr} ${printExpression(b.right)}"
      case s: StringLiteral =>
        s"\"${s.stringValue}\""
      case l: Literal =>
        l.stringValue
      case s: SortItem =>
        s"sort key:${printExpression(s.sortKey)}${s.ordering.map(x => s" ${x}").getOrElse("")}"
      case a: ArrayConstructor =>
        s"[${a.children.map(printExpression).mkString(", ")}]"
      case t: FunctionDef =>
        concat(
          List(
            "def ",
            t.name,
            if t.args.isEmpty then
              Nil
            else
              List("(", concat(t.args, ", "), ")")
            ,
            if t.retType.isEmpty then
              Nil
            else
              List(": ", t.retType)
            ,
            if t.expr.isEmpty then
              Nil
            else
              List(" = ", t.expr)
          )
        )
      case t: FieldDef =>
        concat(
          List(
            "val ",
            t.name,
            ": ",
            t.tpe,
            if t.body.isEmpty then
              Nil
            else
              List(" = ", t.body)
          )
        )
      case c: ConditionalExpression =>
        printConditionalExpression(c)
      case b: BinaryExpression =>
        s"${b.operatorName}(${printExpression(b.left)}, ${printExpression(b.right)})"
      case p: ParenthesizedExpression =>
        s"(${p.child.sqlExpr})"
      case null =>
        "null"
      case arg: FunctionArg =>
        arg.name match
          case Some(name) =>
            concat(List(name, " = ", arg.value))
          case None =>
            concat(List(arg.value))
      case i: InterpolatedString =>
        s"${printExpression(i.prefix)}\"${i.parts.map(printExpression).mkString}\""
      case r: DotRef =>
        s"${printExpression(r.qualifier)}.${printExpression(r.name)}"
      case t: This =>
        "this"
      case d: DefContext =>
        if d.name.isEmpty then
          concat(List(d.tpe))
        else
          concat(List(d.name, ": ", d.tpe), ", ")
      case ShouldExpr(testType, left, right, _) =>
        s"${left.sqlExpr} ${testType.expr} ${right.sqlExpr}"
      case other =>
        e.toString

  def printConditionalExpression(c: ConditionalExpression): String =
    c match
      case NoOp(_) =>
        ""
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
      case other =>
        other.toString

end LogicalPlanPrinter
