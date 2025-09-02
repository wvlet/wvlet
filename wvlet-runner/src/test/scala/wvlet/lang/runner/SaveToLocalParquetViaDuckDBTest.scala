package wvlet.lang.runner

import wvlet.airspec.AirSpec
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.lang.runner.connector.duckdb.DuckDBConnector

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

    val outPathAbs = Paths.get(tmpDir.toString, "out.parquet").toFile.getAbsolutePath
    val outPathEsc = outPathAbs.replace("'", "''") // single-quote escape for SQL literal
    val query =
      s"""
         |from [
         |  [1, "a"],
         |  [2, "b"]
         |] as t(id, name)
         |save to '${outPathEsc}'
         |""".stripMargin

    val result = runner.runStatement(QueryRequest(query, isDebugRun = false))
    result.isSuccessfulQueryResult shouldBe true

    val absOut = new java.io.File(outPathAbs)

    def awaitExists(f: java.io.File, timeoutMs: Long = 10000L): Unit =
      val deadline = System.currentTimeMillis() + timeoutMs
      while !f.exists() && System.currentTimeMillis() < deadline do
        Thread.sleep(50)
      f.exists() shouldBe true

    awaitExists(absOut)

    // Verify the content is readable as Parquet via DuckDB with small retries
    val duck = DuckDBConnector(work)
    try
      val sql                          = s"select count(*) as c from read_parquet('${outPathEsc}')"
      var attempts                     = 0
      var lastError: Option[Throwable] = None
      var ok                           = false
      while !ok && attempts < 10 do
        attempts += 1
        try
          duck.runQuery(sql) { rs =>
            rs.next() shouldBe true
            rs.getLong(1) shouldBe 2L
          }
          ok = true
        catch
          case t: Throwable =>
            lastError = Some(t)
            Thread.sleep(100)
      if !ok then
        // Re-throw the last error for AirSpec to report
        throw lastError.get
    finally
      duck.close()
      // Cleanup best-effort (tmpDir may contain build artifacts)
      absOut.delete()
      // Close runner/provider to release any background resources
      try
        runner.close()
      catch
        case _: Throwable =>
          ()
      try
        provider.close()
      catch
        case _: Throwable =>
          ()
    end try
  }

end SaveToLocalParquetViaDuckDBTest
