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

import wvlet.lang.compiler.{Context, Name}
import wvlet.lang.compiler.formatter.CodeFormatter.*
import wvlet.lang.compiler.formatter.CodeFormatterConfig
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.Context.NoContext
import wvlet.lang.model.{DataType, SyntaxTreeNode}
import wvlet.lang.model.expr.*
import wvlet.log.LogSupport

import scala.collection.immutable.ListMap

object LogicalPlanPrinter extends LogSupport:

  def print(m: LogicalPlan)(using ctx: Context = NoContext): String =
    val fileName = ctx.compilationUnit.sourceFile.fileName
    val prefix   = text(s"[LogicalPlan: ${fileName}]")
    val printer  = LogicalPlanPrinter()
    val d        = prefix / printer.plan(m)
    d.render(CodeFormatterConfig(maxLineWidth = 100, indentWidth = 2))

  def printExpr(e: Expression)(using ctx: Context = NoContext): String =
    val printer = LogicalPlanPrinter()
    val d       = printer.expr(e)
    d.render()

  case class NodeRank(syntaxRank: Int, dataflowRank: Int)

  type RankTable = ListMap[LogicalPlan, NodeRank]

import LogicalPlanPrinter.*

class LogicalPlanPrinter(using ctx: Context) extends LogSupport:

  def plan(l: LogicalPlan)(using rankTable: RankTable = ListMap.empty[LogicalPlan, NodeRank]): Doc =
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
      case q: Query =>
        given RankTable = computeRank(q)
        wl("▶︎ Query", lineLocOf(q)) + nest(linebreak + plan(q.child))
      case m: ModelDef =>
        val params =
          if m.params.isEmpty then
            None
          else
            Some(paren(cl(m.params.map(expr))))
        wl(
          s"ModelDef: ${lineLocOf(m)}",
          text(m.name.fullName) + params + m.givenRelationType.map(t => wl(":", t.typeName.name)),
          "="
        ) + nest(linebreak + plan(m.child))

      case w: WithQuery =>
        val defs = concat(
          w.queryDefs
            .map { q =>
              wl("with", expr(q.alias), "as:", lineLocOf(q), nest(linebreak + plan(q.child)))
            },
          linebreak
        )
        defs + linebreak + plan(w.queryBody)
      case f: Filter =>
        node(f)
      case other: LogicalPlan =>
        node(other)

  def expr(e: Expression)(using ctx: Context): Doc =
    e match
      case i: Identifier =>
        text(i.strExpr)
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
        } + {
          if t.defContexts.isEmpty then
            None
          else
            Some(wl("in", cl(t.defContexts.map(expr))))
        } +
          t.retType
            .map { r =>
              wl(":", r.typeName.name)
            }
        t.expr
          .foreach { body =>
            b += text("=") + nest(wsOrNL + expr(body))
          }
        group(wl(b.result()))
      case d: DefArg =>
        wl(text(d.name.name) + ":", d.dataType.typeDescription, d.defaultValue.map(v => wl("=", v)))
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
        val parts = i
          .parts
          .map {
            case s: StringPart =>
              expr(s)
            case other =>
              cat("$", brace(expr(other)))
          }
        cat(expr(i.prefix) + "\"" + cat(parts) + "\"")
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

  private def node(n: SyntaxTreeNode)(using
      rankTable: RankTable = ListMap.empty[LogicalPlan, NodeRank]
  ): Doc =
    val attr = List.newBuilder[Doc]
    val l    = List.newBuilder[Doc]

    def iter(x: Any): Unit =
      x match
        case null | Nil | None =>
        // skip
        case s: String =>
          attr += text(s)
        case t: TableName =>
          attr += text(t.fullName)
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

    val dataflowOrder: Option[Doc] =
      if isPlan then
        rankTable
          .get(n.asInstanceOf[LogicalPlan])
          .map { r =>
            text(s"${r.dataflowRank})")
          }
      else
        None

    var d =
      if isLeaf then
        wl("▼", dataflowOrder, n.nodeName)
      else
        wl(dataflowOrder, n.nodeName)

    val loc = s" ${lineLocOf(n)}"

    if isPlan then
      if attributes.size == 1 then
        val list = attributes.map(a => ws + a)
        d = group(d + ":" + nest(maybeNewline + list.head) + loc)
      else
        val list = attributes.map(a => wl("-", a))
        if list.nonEmpty then
          d = d + ":" + loc + nest(linebreak + concat(list, linebreak))
        else
          d = d + loc
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

  private def lineLocOf(n: SyntaxTreeNode): String =
    val sourceLoc = n.sourceLocation
    // val sourceLoc = n.sourceLocation
    s"- (line:${sourceLoc.lineAndColString})"

  /**
    * Compute syntax-position rank (line position order) and dataflow order rank (dfs post-order)
    *
    * @param l
    * @return
    */
  private def computeRank(l: LogicalPlan): RankTable =
    val postOrder = List.newBuilder[LogicalPlan]

    def dfs(p: LogicalPlan): Unit =
      def traverseChildren(plan: LogicalPlan): Unit = plan
        .childNodes
        .foreach {
          case c: LogicalPlan =>
            dfs(c)
          case _ =>
        }

      p match
        case q: Query =>
          dfs(q.body)
        case p: PackageDef =>
          p.statements.foreach(dfs)
        case w: WithQuery =>
          w.queryDefs
            .foreach { n =>
              // Skip AliasedRelation
              dfs(n.child)
            }
          dfs(w.queryBody)
        case plan: LogicalPlan =>
          traverseChildren(plan)
          postOrder += plan

    dfs(l)

    val topologicalOrderList = postOrder.result()
    val syntaxOrderTable: Map[LogicalPlan, Int] = topologicalOrderList
      .sortBy { x =>
        val pos = x.sourceLocation.position
        (pos.line, pos.column)
      }
      .zipWithIndex
      .map { (n, rank) =>
        n -> (rank + 1)
      }
      .toMap[LogicalPlan, Int]

    val rankTable: ListMap[LogicalPlan, NodeRank] = ListMap.from(
      postOrder
        .result()
        .zipWithIndex
        .map { (n, i) =>
          n -> NodeRank(syntaxOrderTable(n), i + 1)
        }
    )
    //    info(
    //      rankTable
    //        .map { (n, rank) =>
    //          s"[${rank.dataflowRank}] ${n.nodeName}"
    //        }
    //        .mkString("\n")
    //    )
    rankTable
  end computeRank

end LogicalPlanPrinter
