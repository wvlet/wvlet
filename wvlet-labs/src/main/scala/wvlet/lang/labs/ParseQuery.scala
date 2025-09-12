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
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable.ListBuffer

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
      compiler: wvlet.lang.compiler.Compiler
  ): Option[QueryErrorRecord] =
    try
      // Create a compilation unit from the SQL string
      val unit = wvlet.lang.compiler.CompilationUnit.fromSqlString(queryRecord.sql)
      // Parse the SQL using the compiler with parseOnlyPhases
      val ctx = Context.NoContext
      ParserPhase.parse(unit, ctx)
      val compileResult = CompileResult(List(unit), null, ctx, Some(unit))

      if compileResult.hasFailures then
        val errorMessages = compileResult.failureReport.map(_._2.getMessage).toList
        Some(
          QueryErrorRecord(
            queryIndex = queryRecord.queryIndex,
            td_account_id = queryRecord.tdAccountId,
            job_id = queryRecord.jobId,
            query_id = queryRecord.queryId,
            database = queryRecord.database,
            sql = queryRecord.sql,
            errorType = "compilation_failure",
            errors = Some(errorMessages)
          )
        )
      else
        debug(
          s"Successfully parsed query ${queryRecord.queryIndex} from database ${queryRecord
              .database}"
        )
        None
    catch
      case e: Exception =>
        Some(
          QueryErrorRecord(
            queryIndex = queryRecord.queryIndex,
            td_account_id = queryRecord.tdAccountId,
            job_id = queryRecord.jobId,
            query_id = queryRecord.queryId,
            database = queryRecord.database,
            sql = queryRecord.sql,
            errorType = "exception",
            exception = Some(e.getClass.getSimpleName),
            message = Some(e.getMessage),
            stackTrace = Some(e.getStackTrace.take(5).map(_.toString).toList)
          )
        )

  @command(isDefault = true, description = "Parse query log")
  def help(): Unit = info(s"Use 'parse' subcommand to parse query log")

  @command(description = "Parse query log")
  def parse(
      @argument(description = "query log parquet file, with database and sql parameters")
      queryLogFile: String,
      @option(prefix = "-b,--batch-size", description = "Batch size for parallel processing")
      batchSize: Int = 1000,
      @option(prefix = "-p,--parallelism", description = "Number of parallel threads")
      parallelism: Int = Runtime.getRuntime.availableProcessors()
  ): Unit =
    info(s"Reading query logs from ${queryLogFile}")
    info(s"Using batch size: ${batchSize}, parallelism: ${parallelism}")

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
            val queryCount   = new AtomicInteger(0)
            val errorCount   = new AtomicInteger(0)
            var totalQueries = 0

            // Create error log file in target folder
            val queryLogFileName = java.nio.file.Paths.get(queryLogFile).getFileName.toString
            val targetDir        = java.nio.file.Paths.get("target")
            java.nio.file.Files.createDirectories(targetDir)
            val errorLogFile = targetDir.resolve(s"${queryLogFileName}.errors.json").toString

            Control.withResource(new PrintWriter(new FileWriter(errorLogFile))) { errorWriter =>
              // Collect queries in batches and process them in parallel
              var batch = ListBuffer[QueryRecord]()

              while rs.next() do
                val tdAccountId = rs.getString("td_account_id")
                val jobId       = rs.getString("job_id")
                val queryId     = rs.getString("query_id")
                val database    = rs.getString("database")
                val sql         = rs.getString("sql")

                totalQueries += 1
                batch +=
                  QueryRecord(
                    queryIndex = totalQueries,
                    tdAccountId = tdAccountId,
                    jobId = jobId,
                    queryId = queryId,
                    database = database,
                    sql = sql
                  )

                // Process batch when it reaches the specified size
                if batch.size >= batchSize then
                  processBatch(
                    batch.toList,
                    compiler,
                    errorWriter,
                    queryCount,
                    errorCount,
                    parallelism
                  )
                  batch.clear()

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
              end while

              // Process remaining queries in the last batch
              if batch.nonEmpty then
                processBatch(
                  batch.toList,
                  compiler,
                  errorWriter,
                  queryCount,
                  errorCount,
                  parallelism
                )

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

  private def processBatch(
      batch: List[QueryRecord],
      compiler: wvlet.lang.compiler.Compiler,
      errorWriter: PrintWriter,
      queryCount: AtomicInteger,
      errorCount: AtomicInteger,
      parallelism: Int
  ): Unit =
    // Process the batch in parallel
    val errorResults =
      Parallel.run(batch, parallelism = parallelism) { queryRecord =>
        parseQueryRecord(queryRecord, compiler)
      }

    // Write errors and update counters
    errorResults.foreach { errorOpt =>
      queryCount.incrementAndGet()
      errorOpt match
        case Some(errorRecord) =>
          errorCount.incrementAndGet()
          synchronized {
            errorWriter.println(codec.toJson(errorRecord))
          }
        case None =>
        // Successfully parsed
    }

end ParseQuery
