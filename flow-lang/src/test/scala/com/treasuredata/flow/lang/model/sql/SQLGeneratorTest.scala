package com.treasuredata.flow.lang.model.sql

import com.treasuredata.flow.lang.compiler.parser.FlowParser
import com.treasuredata.flow.lang.model.plan.Query
import wvlet.airspec.AirSpec
import wvlet.log.io.IOUtil

class SQLGeneratorTest extends AirSpec:
  test("generate SQL from behavior.flow"):
    val src  = IOUtil.readAsString("examples/cdp_behavior/src/behavior.flow")
    val plan = FlowParser.parse(src)
    plan.traverse { case q: Query =>
      val sql = SQLGenerator.toSQL(q)
      debug(q)
      debug(sql)
    }
