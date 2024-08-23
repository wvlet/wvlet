package wvlet.lang.runner

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.parser.WvletParser
import wvlet.lang.runner.connector.DBConnector
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.lang.runner.connector.trino.{TrinoConfig, TrinoConnector}
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

        val parser = WvletParser(unit)
        val plan   = parser.parse()
        debug(plan)

//        val planner = ExecutionPlanner(using unit, ctx)
//        val expr    = planner.plan(plan).toSQL
//        debug(expr)
//        body(expr)
      }

    // Extend Standard SQL, which doesn't support string concatenation with '+'
    given dbx: DBConnector = duckdb
    query("select 'hello' + ' wvlet-ql!'"): sql =>
      sql shouldBe "select 'hello' || ' wvlet-ql!'"

  }

end ExecutionPlanTest
