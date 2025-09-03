package wvlet.lang.runner

import wvlet.airspec.AirSpec
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.runner.connector.DBConnectorProvider

import java.nio.file.{Files, Paths}

class SaveToLocalParquetViaDuckDBTest extends AirSpec:

  test("save to local parquet via DuckDB handoff (Generic profile)") {
    // Set up a temporary working directory for this test
    val tmpDir = Files.createTempDirectory("wvlet-local-save-")
    val work   = WorkEnv(path = tmpDir.toString)

    val provider = DBConnectorProvider(work)
    val profile  = Profile.defaultGenericProfile
    val executor = QueryExecutor(provider, profile, work)

    val runner = WvletScriptRunner(
      workEnv = work,
      config = WvletScriptRunnerConfig(
        interactive = false,
        profile = profile,
        // Avoid pre-connecting to the default engine during compiler init to reduce flakiness
        catalog = None,
        schema = None
      ),
      queryExecutor = executor,
      threadManager = ThreadManager()
    )

    given QueryProgressMonitor = QueryProgressMonitor.noOp

    val outPathRel = "out.parquet"
    val outPathEsc = outPathRel.replace("'", "''") // single-quote escape for SQL literal
    val query =
      s"""
         |from [
         |  [1, "a"],
         |  [2, "b"]
         |] as t(id, name)
         |save to '${outPathEsc}'
         |""".stripMargin

    val result = runner.runStatement(QueryRequest(query, isDebugRun = false))
    result.isSuccess shouldBe true

    flaky {
      val absOut = Paths.get(tmpDir.toString, outPathRel).toFile
      absOut.exists() shouldBe true
    }
  }

end SaveToLocalParquetViaDuckDBTest
