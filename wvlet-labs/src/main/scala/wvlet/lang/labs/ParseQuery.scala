package wvlet.lang.labs

import wvlet.log.LogSupport
import wvlet.airframe.launcher.*
import java.io.{FileWriter, PrintWriter}
import wvlet.airframe.codec.MessageCodec
import wvlet.airframe.control.{Control, Parallel}
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.compiler.{CompileResult, Context}
import org.duckdb.DuckDBDriver
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

case class QueryRecord(
    queryIndex: Int,
    tdAccountId: String,
    jobId: String,
    queryId: String,
    database: String,
    sql: String
)

case class QueryErrorRecord(
    queryIndex: Int,
    td_account_id: String,
    job_id: String,
    query_id: String,
    database: String,
    sql: String,
    errorType: String,
    errors: Option[List[String]] = None,
    exception: Option[String] = None,
    message: Option[String] = None,
    stackTrace: Option[List[String]] = None
)

object ParseQuery extends LogSupport:
  def main(args: Array[String]): Unit =
    val l = Launcher.of[ParseQuery]
    l.execute(args)

// Test command for parsing queries in batch
class ParseQuery() extends LogSupport:

  private val codec = MessageCodec.of[QueryErrorRecord]

  private def writeErrorRecord(
      errorWriter: PrintWriter,
      queryCount: Int,
      tdAccountId: String,
      jobId: String,
      queryId: String,
      database: String,
      sql: String,
      errorType: String,
      errors: Option[List[String]] = None,
      exception: Option[Exception] = None
  ): Unit =
    val errorRecord = QueryErrorRecord(
      queryIndex = queryCount,
      td_account_id = tdAccountId,
      job_id = jobId,
      query_id = queryId,
      database = database,
      sql = sql,
      errorType = errorType,
      errors = errors,
      exception = exception.map(_.getClass.getSimpleName),
      message = exception.map(_.getMessage),
      stackTrace = exception.map(_.getStackTrace.take(5).map(_.toString).toList)
    )
    synchronized {
      errorWriter.println(codec.toJson(errorRecord))
    }

  private def parseQueryRecord(
      queryRecord: QueryRecord,
      compiler: wvlet.lang.compiler.Compiler,
      errorWriter: PrintWriter
  ): Boolean =
    try
      // Create a compilation unit from the SQL string
      val unit = wvlet.lang.compiler.CompilationUnit.fromSqlString(queryRecord.sql)
      // Parse the SQL using the compiler with parseOnlyPhases
      val ctx = Context.NoContext
      ParserPhase.parse(unit, ctx)
      val compileResult = CompileResult(List(unit), null, ctx, Some(unit))

      if compileResult.hasFailures then
        val errorMessages = compileResult.failureReport.map(_._2.getMessage).toList
        writeErrorRecord(
          errorWriter,
          queryRecord.queryIndex,
          queryRecord.tdAccountId,
          queryRecord.jobId,
          queryRecord.queryId,
          queryRecord.database,
          queryRecord.sql,
          "compilation_failure",
          errors = Some(errorMessages)
        )
        true // Has error
      else
        debug(
          s"Successfully parsed query ${queryRecord.queryIndex} from database ${queryRecord
              .database}"
        )
        false // No error
    catch
      case e: Exception =>
        writeErrorRecord(
          errorWriter,
          queryRecord.queryIndex,
          queryRecord.tdAccountId,
          queryRecord.jobId,
          queryRecord.queryId,
          queryRecord.database,
          queryRecord.sql,
          "exception",
          exception = Some(e)
        )
        true // Has error

  @command(isDefault = true, description = "Parse query log")
  def help(): Unit = info(s"Use 'parse' subcommand to parse query log")

  @command(description = "Parse query log")
  def parse(
      @argument(description = "query log parquet file, with database and sql parameters")
      queryLogFile: String,
      @option(prefix = "-p,--parallelism", description = "Number of parallel threads")
      parallelism: Int = Runtime.getRuntime.availableProcessors(),
      @option(
        prefix = "--error-sampling",
        description = "Error sampling rate (1 = log all errors, 10 = log every 10th error)"
      )
      errorSampling: Int = 10
  ): Unit =
    info(s"Reading query logs from ${queryLogFile}")
    info(s"Using parallelism: ${parallelism} threads, error sampling: 1/${errorSampling}")

    // Use DuckDB JDBC to read the parquet file
    Class.forName("org.duckdb.DuckDBDriver")
    // Enable streaming results to avoid OOM
    val props = Properties()
    props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, String.valueOf(true))

    Control.withResource(java.sql.DriverManager.getConnection("jdbc:duckdb:", props)) {
      connection =>
        Control.withResource(connection.createStatement()) { stmt =>
          Control.withResource(
            stmt.executeQuery(
              s"SELECT td_account_id, job_id, query_id, database, sql FROM '${queryLogFile}' WHERE error_code_name IS NULL"
            )
          ) { rs =>

            // Create a compiler with parseOnlyPhases for lightweight parsing
            val compiler =
              new wvlet.lang.compiler.Compiler(
                wvlet
                  .lang
                  .compiler
                  .CompilerOptions(
                    phases = wvlet.lang.compiler.Compiler.parseOnlyPhases,
                    sourceFolders = List("target/test"),
                    workEnv = wvlet.lang.compiler.WorkEnv(".", logLevel = wvlet.log.LogLevel.INFO)
                  )
              )

            // Thread-safe counters
            val queryCount = new AtomicInteger(0)
            val errorCount = new AtomicInteger(0)

            // Create error log file in target folder
            val queryLogFileName = java.nio.file.Paths.get(queryLogFile).getFileName.toString
            val targetDir        = java.nio.file.Paths.get("target")
            java.nio.file.Files.createDirectories(targetDir)
            val errorLogFile = targetDir.resolve(s"${queryLogFileName}.errors.json").toString

            Control.withResource(new PrintWriter(new FileWriter(errorLogFile))) { errorWriter =>
              // Create an Iterator that wraps the ResultSet for streaming processing
              val queryIterator =
                new Iterator[QueryRecord]:
                  private var currentIndex = 0

                  def hasNext: Boolean = rs.next()

                  def next(): QueryRecord =
                    currentIndex += 1
                    QueryRecord(
                      queryIndex = currentIndex,
                      tdAccountId = rs.getString("td_account_id"),
                      jobId = rs.getString("job_id"),
                      queryId = rs.getString("query_id"),
                      database = rs.getString("database"),
                      sql = rs.getString("sql")
                    )

              // Process queries in parallel using Parallel.iterate for memory-efficient streaming
              val results =
                Parallel.iterate(queryIterator, parallelism = parallelism) { queryRecord =>
                  val hasError = parseQueryRecord(queryRecord, compiler, errorWriter)

                  // Update counters
                  queryCount.incrementAndGet()
                  if hasError then
                    errorCount.incrementAndGet()

                  // Report progress every 10,000 queries
                  val currentQueryCount = queryCount.get()
                  if currentQueryCount % 10000 == 0 then
                    val currentErrorCount = errorCount.get()
                    val errorRate =
                      if currentQueryCount > 0 then
                        (currentErrorCount.toDouble / currentQueryCount * 100)
                      else
                        0.0
                    info(
                      f"Processed ${currentQueryCount}%,d queries, ${currentErrorCount}%,d failed (${errorRate}%.1f%% error rate)"
                    )

                  hasError
                }

              // Consume the iterator to trigger parallel processing
              results.foreach(_ => ()) // Just consume the results

              val finalQueryCount = queryCount.get()
              val finalErrorCount = errorCount.get()

              if finalErrorCount > 0 then
                info(s"Errors logged to: ${errorLogFile}")

              val errorRate =
                if finalQueryCount > 0 then
                  (finalErrorCount.toDouble / finalQueryCount * 100)
                else
                  0.0
              info(
                f"Final: ${finalQueryCount}%,d queries, ${finalErrorCount}%,d failed (${errorRate}%.3f%% error rate)"
              )
            }
          }
        }
    }

  end parse

end ParseQuery
