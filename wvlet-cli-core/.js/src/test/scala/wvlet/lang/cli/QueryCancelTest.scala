package wvlet.lang.cli

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QuerySelection.All
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.QueryCancel
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.runner.PlanScriptRunner
import wvlet.lang.runner.QueryResult
import wvlet.uni.io.IO
import wvlet.uni.test.UniTest

import scala.scalajs.js

/**
  * Ctrl-C in the npm REPL raises the [[QueryCancel]] flag from the watcher thread. Raising it here
  * the same way checks that a query stops and the session stays usable
  */
class QueryCancelTest extends UniTest:
  given QueryProgressMonitor = QueryProgressMonitor.noOp

  private def raiseCancel(): Unit =
    val slots = js.Dynamic.newInstance(js.Dynamic.global.Int32Array)(QueryCancel.sharedBuffer)
    js.Dynamic.global.Atomics.store(slots, 0, 1)

  private def run(runner: PlanScriptRunner, query: String): QueryResult = runner.runStatement(
    QueryRequest(query = query, querySelection = All, isDebugRun = true)
  )

  private def statusOf(r: QueryResult): Option[StatusCode] = r
    .getError
    .collect { case e: WvletLangException =>
      e.statusCode
    }

  test("stop an engine query when the flag is raised, keeping saved tables") {
    val workEnv = WorkEnv(path = IO.createTempDirectory(prefix = "query-cancel-test").path)
    val runner  = PlanScriptRunner(workEnv, Profile.defaultWvletProfile, interactive = false)
    try
      run(runner, "from [[1, 'x']] as v(a, b) save to kept").getError shouldBe None

      QueryCancel.arm()
      raiseCancel()
      QueryCancel.isRequested shouldBe true
      statusOf(run(runner, "from tpch.tiny.lineitem agg _.count")) shouldBe
        Some(StatusCode.QUERY_CANCELLED)
      QueryCancel.disarm() shouldBe true
      QueryCancel.isRequested shouldBe false

      val kept = run(runner, "from kept")
      kept.getError shouldBe None
      kept.toPrettyBox() shouldContain "x"
    finally
      runner.close()
  }

  test("ignore the flag while no cancellable query runs") {
    raiseCancel()
    QueryCancel.isRequested shouldBe false
    QueryCancel.disarm() shouldBe false
  }

  test("run queries on the caller when no watcher is available") {
    val queries = WatchedQueries(None)
    queries.runThen(null)(1)(_ + 1) shouldBe Some(2)
    queries.close()
  }

end QueryCancelTest
