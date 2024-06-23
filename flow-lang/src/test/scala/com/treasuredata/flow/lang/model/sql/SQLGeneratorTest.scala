package com.treasuredata.flow.lang.model.sql

import com.treasuredata.flow.lang.compiler.{CompilationUnit, SourceFile}
import com.treasuredata.flow.lang.compiler.parser.FlowParser
import com.treasuredata.flow.lang.model.plan.Query
import wvlet.airspec.AirSpec

class SQLGeneratorTest extends AirSpec:
  pending("context-based sql generation for FunctionApply needs to be supported")
  test("generate SQL from behavior.flow"):
    val plan = FlowParser(
      CompilationUnit(SourceFile.fromResource("spec/cdp_behavior/src/behavior.flow"))
    ).parse()
    plan.traverse { case q: Query =>
      val sql = SQLGenerator.toSQL(q)
      debug(q)
      debug(sql)
    }
