package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.compiler.CompilationUnit
import com.treasuredata.flow.lang.compiler.parser.FlowParser
import com.treasuredata.flow.lang.runner.connector.DBConnector
import com.treasuredata.flow.lang.runner.connector.duckdb.DuckDBConnector
import com.treasuredata.flow.lang.runner.connector.trino.{TrinoConfig, TrinoConnector}
import wvlet.airspec.AirSpec

class ExecutionPlanTest extends AirSpec:

  test("create an execution plan") {
    val duckdb = new DuckDBConnector()
    val trino =
      new TrinoConnector(
        TrinoConfig(catalog = "memory", schema = "main", hostAndPort = "localhost:8080")
      )

    inline def query(q: String)(body: String => Unit)(using ctx: DBConnector): Unit =
      // var expr: SqlExpr = null
      test(s"query: ${q}") {
        val unit: CompilationUnit = CompilationUnit.fromString(q)

        val parser = FlowParser(unit)
        val plan   = parser.parse()
        debug(plan)

//        val planner = ExecutionPlanner(using unit, ctx)
//        val expr    = planner.plan(plan).toSQL
//        debug(expr)
//        body(expr)
      }

    // Extend Standard SQL, which doesn't support string concatenation with '+'
    given dbx: DBConnector = duckdb
    query("select 'hello' + ' treasure-flow!'"): sql =>
      sql shouldBe "select 'hello' || ' treasure-flow!'"

  }

end ExecutionPlanTest
