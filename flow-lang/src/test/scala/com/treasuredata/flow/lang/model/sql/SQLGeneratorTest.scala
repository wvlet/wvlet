package com.treasuredata.flow.lang.model.sql

import com.treasuredata.flow.lang.model.plan.Query
import com.treasuredata.flow.lang.parser.FlowParser
import wvlet.airspec.AirSpec
import wvlet.log.io.IOUtil

class SQLGeneratorTest extends AirSpec:
  test("generate SQL from behavior.flow"):
    val src     = IOUtil.readAsString("examples/cdp_behavior/src/behavior.flow")
    val plan    = FlowParser.parse(src)
    val queries = plan.plans.collect { case q: Query => q }
    queries.map { q =>
      val sql = SQLGenerator.toSQL(q)
      debug(sql)
    }
