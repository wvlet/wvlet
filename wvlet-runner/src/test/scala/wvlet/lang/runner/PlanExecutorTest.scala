package wvlet.lang.runner

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.analyzer.duckdb.DuckDB
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.runner.connector.SqlConnectorProvider
import wvlet.uni.log.LogLevel
import wvlet.uni.test.UniTest

/**
  * Cross-platform tests for [[PlanExecutor]] — the shared ExecutionPlan interpreter. Compiles wvlet
  * sources and executes them over the session-backed DuckDB connector, exercising the plan walk,
  * multi-statement sequencing, and `test` evaluation identically on JVM, Node.js, and Native.
  * Auto-skips where libduckdb isn't loadable.
  */
class PlanExecutorTest extends UniTest:

  private def skipIfNoDuckDB(): Unit =
    if !DuckDB.canExecute then
      ignore("DuckDB execution is not available on this platform")

  private val profile = Profile(
    "test",
    List(ConnectorConfig(name = "duckdb", `type` = "duckdb", default = true))
  )

  private def run(query: String): QueryResult =
    val workEnv       = WorkEnv(".", logLevel = LogLevel.WARN)
    val compiler      = Compiler(CompilerOptions(sourceFolders = List("."), workEnv = workEnv))
    val unit          = CompilationUnit.fromWvletString(query)
    val compileResult = compiler.compileSingleUnit(unit)
    compileResult.reportAllErrors
    val ctx = compileResult
      .context
      .withCompilationUnit(unit)
      .withDebugRun(true)
      .newContext(Symbol.NoSymbol)
    val provider = SqlConnectorProvider(profile)
    try
      val executor = PlanExecutor(provider, profile, workEnv)
      executor.execute(ExecutionPlanner.plan(unit, ctx), ctx)
    finally
      provider.close()

  private def collect[A](r: QueryResult)(f: PartialFunction[QueryResult, A]): List[A] =
    r match
      case l: QueryResultList =>
        l.list.toList.flatMap(x => collect(x)(f))
      case x =>
        f.lift(x).toList

  test("execute a query and materialize rows") {
    skipIfNoDuckDB()
    val result = run("from [[1, 'a'], [2, 'b']] as t(id, name) select id, name")
    val tables =
      collect(result) { case t: TableRows =>
        t
      }
    tables.nonEmpty shouldBe true
    tables.head.totalRows shouldBe 2
    tables.head.schema.fields.map(_.name.name) shouldBe List("id", "name")
  }

  test("evaluate passing and failing test statements") {
    skipIfNoDuckDB()
    val passing = run("""from [[1], [2]] as t(a) select a
                        |test _.size should be 2""".stripMargin)
    collect(passing) { case t: TestSuccess =>
      t
    }.size shouldBe 1
    passing.hasError shouldBe false
    // The tested query runs exactly once; the trailing test reads its result directly
    collect(passing) { case t: TableRows =>
      t
    }.size shouldBe 1

    val failing = run("""from [[1]] as t(a) select a
                        |test _.size should be 5""".stripMargin)
    val failures =
      collect(failing) { case t: TestFailure =>
        t
      }
    failures.size shouldBe 1
    failing.hasError shouldBe true
  }

  test("run multi-statement scripts in order") {
    skipIfNoDuckDB()
    val result = run("""select 10 as x;
                       |select 20 as y""".stripMargin)
    val tables =
      collect(result) { case t: TableRows =>
        t
      }
    tables.size shouldBe 2
    // SqlConnector-backed results are string-coerced
    tables(0).rows.head.values.head shouldBe "10"
    tables(1).rows.head.values.head shouldBe "20"
  }

  test("share session state across statements") {
    skipIfNoDuckDB()
    val result = run("""execute sql"create table nums(n integer)";
                       |execute sql"insert into nums values (1), (2), (3)";
                       |from nums select count(*) as cnt""".stripMargin)
    val tables =
      collect(result) { case t: TableRows =>
        t
      }
    tables.last.rows.head.values.head shouldBe "3"
  }

end PlanExecutorTest
