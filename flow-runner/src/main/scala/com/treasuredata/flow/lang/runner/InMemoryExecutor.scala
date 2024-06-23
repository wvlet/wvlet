package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.compiler.Context
import com.treasuredata.flow.lang.model.plan.*
import wvlet.airframe.codec.MessageCodec
import wvlet.airframe.control.IO
import wvlet.log.LogSupport
import wvlet.log.io.IOUtil

import java.io.File

object InMemoryExecutor:
  def default = InMemoryExecutor()

class InMemoryExecutor extends LogSupport:
  def execute(plan: LogicalPlan, context: Context): QueryResult =
    plan match
      case p: PackageDef =>
        val results = p
          .statements
          .map: stmt =>
            PlanResult(stmt, execute(stmt, context))
        QueryResultList(results)
      case q: Query =>
        execute(q.body, context)
      case t: TestDef =>
        warn(s"Test execution is not supported yet: ${t}")
        QueryResult.empty
      case r: JSONFileScan =>
        val json = IO.readAsString(new File(r.path))
        trace(json)
        val codec = MessageCodec.of[Seq[Map[String, Any]]]
        val data  = codec.fromJson(json)
        TableRows(r.schema, data)
