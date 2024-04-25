package com.treasuredata.flow.lang.model.sql

import com.treasuredata.flow.lang.compiler.SourceFile
import com.treasuredata.flow.lang.compiler.parser.FlowParser
import com.treasuredata.flow.lang.model.plan.Query
import wvlet.airspec.AirSpec

class SQLGeneratorTest extends AirSpec:
  test("generate SQL from behavior.flow"):
    val plan = FlowParser.parse(SourceFile.fromResource("examples/cdp_behavior/src/behavior.flow"))
    plan.traverse { case q: Query =>
      val sql = SQLGenerator.toSQL(q)
      debug(q)
      debug(sql)
    }
