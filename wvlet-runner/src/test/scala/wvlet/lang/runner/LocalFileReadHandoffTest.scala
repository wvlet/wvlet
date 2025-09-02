package wvlet.lang.runner

import wvlet.airspec.AirSpec
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.lang.runner.connector.duckdb.DuckDBConnector

import java.nio.file.{Files, Paths}

class LocalFileReadHandoffTest extends AirSpec:

  test("auto-switch to DuckDB for local file read") {
    // This spec can be sensitive to engine init and file visibility timing.
    // Isolate into a temporary working directory
    val tmpDir = Files.createTempDirectory("wvlet-local-read-")
    val work   = WorkEnv(path = tmpDir.toString)

    // Prepare a local parquet file with DuckDB
    val duck = DuckDBConnector(work)
    val out  = Paths.get(tmpDir.toString, "out.parquet").toFile
    try
      val copySQL =
        s"""
           |copy (
           |  select * from (values (1, 'a'), (2, 'b')) as t(id, name)
           |) to '${out.getPath.replace("'", "''")}' (FORMAT 'parquet', USE_TMP_FILE true)
           |""".stripMargin
      given QueryProgressMonitor = QueryProgressMonitor.noOp
      duck.execute(copySQL)
    finally
      duck.close()

    // Run a query that reads the local file with a Trino default profile.
    // The executor should auto-switch to DuckDB and run successfully.
    val provider = DBConnectorProvider(work)
    val profile  = Profile.defaultProfileFor(wvlet.lang.compiler.DBType.Trino)
    val executor = QueryExecutor(provider, profile, work)

    val runner = WvletScriptRunner(
      workEnv = work,
      config = WvletScriptRunnerConfig(
        interactive = false,
        profile = profile,
        // Avoid pre-connecting to the default engine during compiler init
        catalog = None,
        schema = None
      ),
      queryExecutor = executor,
      threadManager = ThreadManager()
    )

    given QueryProgressMonitor = QueryProgressMonitor.noOp

    // Ensure the file is visible to the runner before executing
    def awaitExists(f: java.io.File, timeoutMs: Long = 3000L): Unit =
      val deadline = System.currentTimeMillis() + timeoutMs
      while !f.exists() && System.currentTimeMillis() < deadline do
        Thread.sleep(50)

    awaitExists(out)

    val q =
      s"""
         |from 'out.parquet'
         |select count(*) as c
         |""".stripMargin
    flaky {
      val result = runner.runStatement(QueryRequest(q, isDebugRun = false))
      // Print for debugging in CI/local runs
      debug(result.toPrettyBox())
      result.isSuccessfulQueryResult shouldBe true

      // Extract TableRows from the result, handling both direct TableRows and QueryResultList cases
      val tableRows =
        result shouldMatch {
          case t: TableRows =>
            t
          case qrl: QueryResultList =>
            // Find the TableRows within the QueryResultList
            qrl
              .list
              .collectFirst { case t: TableRows =>
                t
              } match
              case Some(rows) =>
                rows
              case _ =>
                fail(s"No TableRows found in QueryResultList: ${qrl}")
        }

      tableRows shouldMatch { case t: TableRows =>
        t.rows.size shouldBe 1
        val v = t.rows.head("c")
        val n =
          v match
            case x: java.lang.Number =>
              x.longValue()
            case x =>
              x.toString.toLong
        n shouldBe 2L
      }
    }

    // cleanup best-effort (tmpDir may contain build artifacts)
    out.delete()
  }

end LocalFileReadHandoffTest
