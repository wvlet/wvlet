package wvlet.lang.runner

import wvlet.airspec.AirSpec
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.lang.runner.connector.duckdb.DuckDBConnector

import java.nio.file.{Files, Paths}

class TrinoLocalSaveTest extends AirSpec:

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
        catalog = Some("memory"),
        schema = Some("main")
      ),
      queryExecutor = executor,
      threadManager = ThreadManager()
    )

    given QueryProgressMonitor = QueryProgressMonitor.noOp

    val outPath = "out.parquet"
    val query =
      s"""
         |from [
         |  [1, "a"],
         |  [2, "b"]
         |] as t(id, name)
         |save to '${outPath}'
         |""".stripMargin

    val result = runner.runStatement(QueryRequest(query, isDebugRun = false))
    result.isSuccessfulQueryResult shouldBe true

    val absOut = Paths.get(tmpDir.toString, outPath).toFile
    absOut.exists() shouldBe true

    // Verify the content is readable as Parquet via DuckDB
    val duck = DuckDBConnector(work)
    try
      duck.runQuery(
        s"select count(*) as c from read_parquet('${absOut.getPath.replace("'", "''")}')"
      ) { rs =>
        rs.next() shouldBe true
        rs.getLong(1) shouldBe 2L
      }
    finally duck.close()

    // Cleanup best-effort (tmpDir may contain build artifacts)
    absOut.delete()
  }

end TrinoLocalSaveTest
