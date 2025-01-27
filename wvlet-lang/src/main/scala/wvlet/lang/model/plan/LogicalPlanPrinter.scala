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

import wvlet.lang.compiler.Name
import wvlet.lang.compiler.formatter.CodeFormatter.*
import wvlet.lang.compiler.formatter.CodeFormatterConfig
import wvlet.lang.model.SyntaxTreeNode
import wvlet.lang.model.expr.*
import wvlet.log.LogSupport

object LogicalPlanPrinter extends LogSupport:

  def print(m: LogicalPlan): String =
    val d = plan(m)
    d.render(CodeFormatterConfig(maxLineWidth = 100, indentWidth = 2))

  def print(e: Expression): String =
    val d = expr(e)
    d.render()

//  def print(m: LogicalPlan, out: PrintWriter, level: Int): Unit =
//    def printChildExprs(children: Seq[Expression]): Unit =
//      val attr    = children.map(x => expr(x))
//      val ws      = "  " * (level + 1)
//      val attrStr = attr.map(x => s"${ws}- ${x}").mkString("\n")
//      out.println(attrStr)
//
//    m match
//      case null | EmptyRelation(_) =>
//      // print nothing
////      case f: FunctionDef =>
////        val rt = f.resultType.map(x => s": ${x}").getOrElse("")
////        out.println(s"[FunctionDef] ${f.name}")
////        out.println(
////          s"  def ${f.name}(${f.args.map(x => s"${x.name}").mkString(", ")})${rt} = ${printExpression(f.bodyExpr)}"
////        )
//      case t: TypeDef =>
//        val s = concat(
//          List(
//            "  " * level,
//            "[TypeDef ",
//            t.span,
//            "] ",
//            t.name,
//            if t.params.isEmpty then
//              Nil
//            else
//              List("[", t.params.map(_.typeDescription).mkString(", "), "]")
//            ,
//            if t.defContexts.isEmpty then
//              Nil
//            else
//              List(" in ", concat(t.defContexts, ", "))
//            ,
//            if t.parent.isEmpty then
//              Nil
//            else
//              List(" extends ", t.parent)
//          )
//        )
//        out.println("")
//        out.println(s)
//        printChildExprs(t.elems)
//      case _ =>
//        val ws = "  " * level
//
//        def wrap[A](v: A): String =
//          v match
//            case s: Seq[String] @unchecked if s.length <= 1 =>
//              s.headOption.map(_.toString).getOrElse("empty")
//            case s: Seq[String] =>
//              s"(${s.mkString(", ")})"
//            case other =>
//              other.toString
//
//        def printRelationType(r: RelationType): String = s"<${r.typeDescription}>"
//
////        val inputType =
////          m.inputRelationType match
////            case EmptyRelationType =>
////              ""
////            case other: RelationType =>
////              wrap(printRelationType(other))
////
//        val outputType =
//          m.relationType match
//            case DataType.EmptyRelationType =>
//              ""
//            case r: RelationType =>
//              wrap(printRelationType(r))
//
//        val inputAttrs  = m.inputRelationType.fields
//        val outputAttrs = m.relationType.fields
//
//        val attr        = m.childExpressions.map(expr)
//        val functionSig = s" => ${outputType}"
//
//        val resolvedSign =
//          if m.resolved then
//            ""
//          else
//            "*"
//        val loc = m.span.map(l => s" ${l}").getOrElse("")
//        val prefix =
//          m match
//            case t: HasTableName =>
//              s"${ws}[${resolvedSign}${m.nodeName}${loc}] ${t.name}${functionSig}"
//            case src: HasSourceFile =>
//              s"${ws}[${resolvedSign}${m.nodeName} ${src.sourceFile.fileName}${loc}]${functionSig} "
//            case _ =>
//              s"${ws}[${resolvedSign}${m.nodeName}${loc}]${functionSig}"
//
//        m match
//          case p: PackageDef =>
//          // do not add new line
//          case m: ModelDef =>
//            out.println()
//          case l: LanguageStatement =>
//            // add a new line for language statement
//            out.println()
//          case _ =>
//
//        attr.length match
//          case 0 =>
//            out.println(prefix)
//          case _ =>
//            out.println(s"${prefix}")
//            printChildExprs(m.childExpressions)
//
//        for c <- m.children do
//          // Add indent for child relations
//          print(c, out, level + 1)
//
//    end match
//
//  end print

  def plan(l: LogicalPlan): Doc =
    l match
      case null | EmptyRelation(_) =>
        empty
      case p: PackageDef =>
        val pkg =
          if p.name.isEmpty then
            empty
          else
            group(wl("package", expr(p.name))) + linebreak
        pkg + concat(p.statements.map(plan), linebreak + linebreak)
      case w: WithQuery =>
        val defs = concat(
          w.queryDefs
            .map { q =>
              wl("with", expr(q.alias), "as:", nest(linebreak + plan(q.child)))
            },
          linebreak
        )
        defs + linebreak + plan(w.queryBody)
      case f: Filter =>
        node(f)
      case other: LogicalPlan =>
        node(other)

  def expr(e: Expression): Doc =
    e match
      case i: Identifier =>
        text(i.strExpr)
      case d: DefArg =>
        wl(
          text(d.name.name) + ": ",
          d.dataType.typeDescription,
          d.defaultValue.map(v => wl("=", v))
        )
      case s: SingleColumn =>
        val body = expr(s.expr)
        if s.nameExpr.isEmpty || body.toString == s.fullName then
          expr(s.expr) + ":" + s.dataTypeName
        else
          wl(text(s.fullName) + ":" + s.dataTypeName, ":=", body)
      case a: Alias =>
        wl(expr(a.expr), "as", a.fullName)
      case g: GroupingKey =>
        expr(g.child)
      case s: SubQueryExpression =>
        indentedBrace(plan(s.query))
      case b: ArithmeticBinaryExpr =>
        wl(expr(b.left), b.exprType.expr, expr(b.right))
      case s: StringLiteral =>
        text(s.stringValue)
      case l: Literal =>
        text(l.stringValue)
      case a: ArrayConstructor =>
        bracket(cl(a.children.map(expr)))
      case t: FunctionDef =>
        val b = List.newBuilder[Doc]
        b += text("def")
        b += text(t.name.name) + {
          // function args
          if t.args.isEmpty then
            None
          else
            Some(paren(cl(t.args.map(expr))))
        }
        t.retType
          .foreach { r =>
            b += text(":")
            b += text(r.typeName.name)
          }
        t.expr
          .foreach { body =>
            b += text("=") + nest(wsOrNL + expr(body))
          }
        wl(b.result())
      case t: FieldDef =>
        group(
          wl(
            "val",
            t.name.name + ":",
            expr(t.tpe),
            t.body
              .map { b =>
                "=" + nest(wsOrNL + expr(b))
              }
          )
        )
      case p: ParenthesizedExpression =>
        paren(expr(p.child))
      case null =>
        text("null")
      case arg: FunctionArg =>
        arg.name match
          case Some(name) =>
            wl(name.name, "=", expr(arg.value))
          case None =>
            expr(arg.value)
      case i: InterpolatedString =>
        cat(expr(i.prefix) + "\"" + cat(i.parts.map(expr)) + "\"")
      case r: DotRef =>
        cat(expr(r.qualifier), ".", expr(r.name))
      case t: This =>
        text("this")
      case d: DefContext =>
        if d.name.isEmpty then
          expr(d.tpe)
        else
          expr(d.name.get) + ": " + expr(d.tpe)
      case ShouldExpr(testType, left, right, _) =>
        wl(expr(left), testType.expr, expr(right))
      case w: WindowFrame =>
        bracket(cat(text(w.start.wvExpr), ",", text(w.end.wvExpr)))
      case NoJoinCriteria =>
        empty
      case other =>
        node(other)

  def node(n: SyntaxTreeNode): Doc =
    val attr = List.newBuilder[Doc]
    val l    = List.newBuilder[Doc]

    def iter(x: Any): Unit =
      x match
        case null | Nil | None =>
        // skip
        case s: String =>
          attr += text(s)
        case n: Name =>
          attr += text(n.name)
        case e: Expression =>
          attr += expr(e)
        case p: LogicalPlan =>
          l += plan(p)
        case s: Seq[?] =>
          s.foreach(iter)
        case o: Option[?] =>
          o.foreach(iter)
        case i: Iterator[?] =>
          i.foreach(iter)
        case it: Iterable[?] =>
          it.foreach(iter)
        case other =>

    n.productIterator.foreach(iter)
    val attributes = attr.result().filter(_.nonEmpty)
    val childNodes = l.result().filter(_.nonEmpty)

    val isPlan =
      n match
        case _: LogicalPlan =>
          true
        case _ =>
          false

    val isLeaf = isPlan && childNodes.isEmpty

    var d =
      if isLeaf then
        text(s"▼ ${n.nodeName}")
      else
        text(n.nodeName)

    if isPlan then
      if attributes.size == 1 then
        val list = attributes.map(a => ws + a)
        d = group(d + n.span.toString + ":" + nest(maybeNewline + list.head))
      else
        val list = attributes.map(a => wl("-", a))
        if list.nonEmpty then
          d = d + n.span.toString + ":" + nest(linebreak + concat(list, linebreak))
        else
          d = d + n.span.toString
    else
      d = d + group(text("(") + nest(maybeNewline + cl(attributes) + text(")")))
      // d = d + paren(cl(attributes))

    childNodes match
      case Nil =>
        d
      case c :: Nil =>
        c + linebreak + text("↓ ") + d
      case c :: rest =>
        c + nest(linebreak + concat(rest, linebreak + linebreak)) + linebreak + text("↙ ") + d

  end node

end LogicalPlanPrinter
