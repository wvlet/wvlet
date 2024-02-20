package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.model.expr.Attribute
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

        val inputAttrs  = Seq.empty // m.inputAttributes
        val outputAttrs = Seq.empty // m.outputAttributes
        val attr        = m.childExpressions.map(_.toString)
        val functionSig =
          if inputAttrs.isEmpty && outputAttrs.isEmpty then ""
          else
            def printAttr(s: Seq[Attribute]): String =
              val lst = s
                .map(_.typeDescription)
                .mkString(", ")
              if s.size >= 1 then s"(${lst})"
              else lst
            s": ${printAttr(inputAttrs)} => ${printAttr(outputAttrs)}"

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
