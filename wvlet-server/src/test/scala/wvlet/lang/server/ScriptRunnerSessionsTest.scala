/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.server

import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QuerySelection
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.ThreadManager
import wvlet.lang.runner.WvletScriptRunner
import wvlet.lang.runner.WvletScriptRunnerConfig
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.test.UniTest

import scala.concurrent.duration.Duration

/**
  * Per-session isolation of the active engine and default catalog/schema (#1867): `use` statements
  * of one session must not affect queries of another
  */
class ScriptRunnerSessionsTest extends UniTest:

  // An isolated working folder so each session's background source pre-compilation has no
  // repository files to chew through
  private val workDir = new java.io.File("target/script-runner-sessions-test")
  workDir.mkdirs()
  private val workEnv = WorkEnv(path = workDir.getPath)

  private val first = ConnectorConfig(
    name = "first",
    `type` = "duckdb",
    default = true,
    catalog = Some("memory"),
    schema = Some("main")
  )

  private val second = ConnectorConfig(
    name = "second",
    `type` = "duckdb",
    catalog = Some("memory"),
    schema = Some("main")
  )

  private val profile = Profile(name = "multi", connectors = Seq(first, second))

  private val provider      = ConnectorProvider(workEnv)
  private val threadManager = ThreadManager()
  private val config        = WvletScriptRunnerConfig(
    interactive = false,
    profile = profile,
    catalog = first.catalog,
    schema = first.schema
  )

  private val sessions = ScriptRunnerSessions(
    workEnv,
    config,
    provider,
    threadManager,
    ScriptRunnerSessions.DefaultIdleTimeout
  )

  // A table that only exists on the second connector's in-memory database
  provider.getDBConnector(second).execute("create table events as select 42 as id")

  override def afterAll: Unit =
    sessions.close()
    threadManager.close()
    provider.close()

  private given QueryProgressMonitor = QueryProgressMonitor.noOp

  private def run(runner: WvletScriptRunner, query: String) = runner.runStatement(
    QueryRequest(query = query, querySelection = QuerySelection.All)
  )

  test("should return the same runner for the same session id") {
    sessions.runnerFor(Some("a")) shouldBeTheSameInstanceAs sessions.runnerFor(Some("a"))
  }

  test("should share the default session for requests without a session id") {
    sessions.runnerFor(None) shouldBeTheSameInstanceAs sessions.runnerFor(None)
  }

  test("should isolate use statements between sessions") {
    val a = sessions.runnerFor(Some("session-a"))
    val b = sessions.runnerFor(Some("session-b"))

    run(a, "use second").isSuccess shouldBe true
    // session-a now targets the second engine
    run(a, "from second.events").toTSV shouldContain "42"

    // session-b still targets the first engine, so the cross-connector guard rejects the
    // reference — proving session-a's `use` did not leak
    val result = run(b, "from second.events")
    result.isSuccess shouldBe false
    result.getError.map(_.getMessage).getOrElse("") shouldContain "use second"

    // session-a keeps its switched engine after session-b's failed attempt
    run(a, "from second.events").toTSV shouldContain "42"
  }

  test("should compile and run concurrently in separate sessions") {
    // Sessions must not share mutable compiler state (e.g. standard-library units): concurrent
    // first compilations in separate sessions used to race symbol completion
    val pool = java.util.concurrent.Executors.newFixedThreadPool(4)
    try
      val futures = (0 until 4).map { i =>
        pool.submit(
          new java.util.concurrent.Callable[QueryResult]:
            override def call(): QueryResult = run(
              sessions.runnerFor(Some(s"concurrent-${i}")),
              "select 1 as x"
            )
        )
      }
      futures.foreach { f =>
        val result = f.get()
        result.getError.map(_.getMessage).getOrElse("") shouldBe ""
        result.isSuccess shouldBe true
      }
    finally
      pool.shutdownNow()
  }

  test("should evict idle sessions but keep the default session") {
    val shortLived = ScriptRunnerSessions(workEnv, config, provider, threadManager, Duration.Zero)
    try
      val defaultRunner = shortLived.runnerFor(None)
      val r1            = shortLived.runnerFor(Some("ephemeral"))
      shortLived.sessionCount shouldBe 2
      // Let the zero idle timeout elapse, then access again: the ephemeral session is rebuilt
      // while the default session survives
      Thread.sleep(10)
      val r2 = shortLived.runnerFor(Some("ephemeral"))
      (r1 eq r2) shouldBe false
      shortLived.runnerFor(None) shouldBeTheSameInstanceAs defaultRunner
    finally
      shortLived.close()
  }

end ScriptRunnerSessionsTest
