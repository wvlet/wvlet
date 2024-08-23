package wvlet.lang.model.sql

import wvlet.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.lang.compiler.parser.WvletParser
import wvlet.lang.model.plan.Query
import wvlet.airspec.AirSpec

class SQLGeneratorTest extends AirSpec:
  pending("context-based sql generation for FunctionApply needs to be supported")
  test("generate SQL from behavior.wv"):
    val plan = WvletParser(
      CompilationUnit(SourceFile.fromResource("spec/cdp_behavior/src/behavior.wv"))
    ).parse()
    plan.traverse { case q: Query =>
      val sql = SQLGenerator.toSQL(q)
      debug(q)
      debug(sql)
    }
