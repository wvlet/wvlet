package wvlet.lang.sdk.js

import scala.scalajs.js
import scala.scalajs.js.annotation._
import wvlet.airframe.codec.MessageCodec
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, DBType, Symbol, WorkEnv}
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.api.{WvletLangException, StatusCode, SourceLocation}
import wvlet.lang.api.v1.compile.{CompileResponse, CompileError, ErrorLocation}
import wvlet.lang.BuildInfo

/**
  * JavaScript API for Wvlet compiler. This provides a JSON-based interface
  * similar to the native library's wvlet_compile_query_json function.
  */
@JSExportTopLevel("WvletJS")
object WvletJS {

  /**
    * Compile a Wvlet query and return the result as JSON.
    * @param query The Wvlet query string
    * @param options JSON string with compilation options (e.g., {"target": "duckdb"})
    * @return JSON string with compilation result
    */
  @JSExport
  def compile(query: String, options: String = "{}"): String = {
    try {
      val opts = MessageCodec.of[CompileOptions].fromJson(options)
      
      // Create compiler with options
      val targetDB = opts.target.getOrElse("duckdb").toLowerCase match {
        case "trino" => DBType.Trino
        case _ => DBType.DuckDB
      }
      
      val compiler = Compiler(
        CompilerOptions(
          workEnv = WorkEnv(path = "."),
          dbType = targetDB
        )
      )
      
      // Compile the query
      val inputUnit = CompilationUnit.fromWvletString(query)
      val compileResult = compiler.compileSingleUnit(inputUnit)
      compileResult.reportAllErrors
      
      // Generate SQL
      val ctx = compileResult
        .context
        .withCompilationUnit(inputUnit)
        .withDebugRun(false)
        .newContext(Symbol.NoSymbol)
      
      val sql = GenSQL.generateSQL(inputUnit)(using ctx)
      
      val response = CompileResponse(
        success = true,
        sql = Some(sql)
      )
      
      MessageCodec.of[CompileResponse].toJson(response)
    } catch {
      case e: WvletLangException =>
        val locationOpt = if e.sourceLocation != SourceLocation.NoSourceLocation then
          Some(ErrorLocation(
            path = e.sourceLocation.path,
            fileName = e.sourceLocation.fileName,
            line = e.sourceLocation.position.line,
            column = e.sourceLocation.position.column,
            lineContent = if e.sourceLocation.codeLineAt.nonEmpty then
              Some(e.sourceLocation.codeLineAt)
            else
              None
          ))
        else
          None
        
        val error = CompileError(
          statusCode = e.statusCode,
          message = e.getMessage,
          location = locationOpt
        )
        
        val response = CompileResponse(
          success = false,
          error = Some(error)
        )
        
        MessageCodec.of[CompileResponse].toJson(response)
        
      case e: Throwable =>
        val error = CompileError(
          statusCode = StatusCode.INTERNAL_ERROR,
          message = Option(e.getMessage).getOrElse(e.getClass.getName)
        )
        
        val response = CompileResponse(
          success = false,
          error = Some(error)
        )
        
        MessageCodec.of[CompileResponse].toJson(response)
    }
  }

  /**
    * Get the version of the Wvlet compiler
    */
  @JSExport
  def getVersion(): String = {
    wvlet.lang.BuildInfo.version
  }

  /**
    * Parse command-line style arguments and compile
    * @param argsJson JSON array of command line arguments
    * @return JSON string with compilation result
    */
  @JSExport
  def compileWithArgs(argsJson: String): String = {
    try {
      val args = MessageCodec.of[Array[String]].fromJson(argsJson)
      
      // Parse arguments to extract query and options
      var query: Option[String] = None
      var target: Option[String] = None
      var i = 0
      
      while (i < args.length) {
        args(i) match {
          case "-q" | "--query" =>
            if (i + 1 < args.length) {
              query = Some(args(i + 1))
              i += 1
            }
          case "--target" =>
            if (i + 1 < args.length) {
              target = Some(args(i + 1))
              i += 1
            }
          case arg if !arg.startsWith("-") && query.isEmpty =>
            query = Some(arg)
          case _ => // Ignore other arguments
        }
        i += 1
      }
      
      query match {
        case Some(q) =>
          val options = CompileOptions(target = target)
          compile(q, MessageCodec.of[CompileOptions].toJson(options))
        case None =>
          val error = CompileError(
            statusCode = StatusCode.INVALID_ARGUMENT,
            message = "No query provided"
          )
          val response = CompileResponse(
            success = false,
            error = Some(error)
          )
          MessageCodec.of[CompileResponse].toJson(response)
      }
    } catch {
      case e: Throwable =>
        val error = CompileError(
          statusCode = StatusCode.INTERNAL_ERROR,
          message = s"Failed to parse arguments: ${e.getMessage}"
        )
        val response = CompileResponse(
          success = false,
          error = Some(error)
        )
        MessageCodec.of[CompileResponse].toJson(response)
    }
  }
}

/**
  * Compilation options
  */
case class CompileOptions(
    target: Option[String] = None,
    profile: Option[String] = None
)