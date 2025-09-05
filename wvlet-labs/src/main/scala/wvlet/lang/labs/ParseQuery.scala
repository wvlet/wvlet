package wvlet.lang.labs

import wvlet.log.LogSupport
import wvlet.airframe.launcher.*
import java.io.{FileWriter, PrintWriter}
import wvlet.airframe.codec.MessageCodec
import wvlet.airframe.control.Control

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
    errorWriter.println(MessageCodec.of[QueryErrorRecord].toJson(errorRecord))
    errorWriter.flush()

  @command(isDefault = true, description = "Parse query log")
  def help(): Unit = info(s"Use 'parse' subcommand to parse query log")

  @command(description = "Parse query log")
  def parse(
      @argument(description = "query log parquet file, with database and sql parameters")
      queryLogFile: String
  ): Unit =
    info(s"Reading query logs from ${queryLogFile}")
    // Use DuckDB JDBC to read the parquet file
    Class.forName("org.duckdb.DuckDBDriver")
    val connection = java.sql.DriverManager.getConnection("jdbc:duckdb:")

    try
      val stmt = connection.createStatement()
      val rs = stmt.executeQuery(
        s"SELECT td_account_id, job_id, query_id, database, sql FROM '${queryLogFile}' WHERE error_code_name IS NULL"
      )

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

      var queryCount   = 0
      var successCount = 0
      var errorCount   = 0

      // Create error log file in target folder
      val queryLogFileName = java.nio.file.Paths.get(queryLogFile).getFileName.toString
      val targetDir        = java.nio.file.Paths.get("target")
      java.nio.file.Files.createDirectories(targetDir)
      val errorLogFile = s"target/${queryLogFileName}.errors.json"

      Control.withResource(new PrintWriter(new FileWriter(errorLogFile))) { errorWriter =>
        // For each query, parse with WvletParser and generate a LogicalPlan
        while rs.next() do
          val tdAccountId = rs.getString("td_account_id")
          val jobId       = rs.getString("job_id")
          val queryId     = rs.getString("query_id")
          val database    = rs.getString("database")
          val sql         = rs.getString("sql")
          queryCount += 1

          try
            // Create a compilation unit from the SQL string
            val unit = wvlet.lang.compiler.CompilationUnit.fromSqlString(sql)

            // Parse the SQL using the compiler with parseOnlyPhases
            val compileResult = compiler.compileSingleUnit(unit)

            if compileResult.hasFailures then
              errorCount += 1
              val errorMessages = compileResult.failureReport.map(_._2.getMessage).toList
              writeErrorRecord(
                errorWriter,
                queryCount,
                tdAccountId,
                jobId,
                queryId,
                database,
                sql,
                "compilation_failure",
                errors = Some(errorMessages)
              )
              if errorCount % 100 == 0 then
                info(s"Progress: ${errorCount} compilation failures so far...")
            else
              successCount += 1
              debug(s"Successfully parsed query ${queryCount} from database ${database}")
              // Optionally log the logical plan
              debug(s"Logical plan: ${compileResult.contextUnit.get.unresolvedPlan}")
          catch
            case e: Exception =>
              errorCount += 1
              writeErrorRecord(
                errorWriter,
                queryCount,
                tdAccountId,
                jobId,
                queryId,
                database,
                sql,
                "exception",
                exception = Some(e)
              )
              if errorCount % 100 == 0 then
                info(s"Progress: ${errorCount} exceptions so far...")
          end try
      }

      if errorCount > 0 then
        info(s"Errors logged to: ${errorLogFile}")

      info(s"Processed ${queryCount} queries: ${successCount} successful, ${errorCount} failed")

    finally
      connection.close()

    end try

  end parse

end ParseQuery
