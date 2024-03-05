package com.treasuredata.flow.lang.model.plan

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

        val attr        = m.childExpressions.map(expr => expr.toString)
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
