package wvlet.lang.sdk.js

import scala.scalajs.js
import scala.scalajs.js.annotation.*
import wvlet.airframe.codec.MessageCodec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.StatusCode
import wvlet.lang.api.SourceLocation
import wvlet.lang.api.v1.compile.CompileResponse
import wvlet.lang.api.v1.compile.CompileError
import wvlet.lang.api.v1.compile.ErrorLocation
import wvlet.lang.BuildInfo
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.model.plan.*

import scala.collection.mutable.ListBuffer

/**
  * JavaScript API for Wvlet compiler. This provides a JSON-based interface similar to the native
  * library's wvlet_compile_query_json function.
  */
@JSExportTopLevel("WvletJS")
object WvletJS:

  /**
    * Compile a Wvlet query and return the result as JSON.
    * @param query
    *   The Wvlet query string
    * @param options
    *   JSON string with compilation options (e.g., {"target": "duckdb"})
    * @return
    *   JSON string with compilation result
    */
  @JSExport
  def compile(query: String, options: String = "{}"): String =
    try
      val opts = MessageCodec.of[CompileOptions].fromJson(options)

      // Create compiler with options
      val targetDB =
        opts.target.getOrElse("duckdb").toLowerCase match
          case "trino" =>
            DBType.Trino
          case _ =>
            DBType.DuckDB

      val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(path = "."), dbType = targetDB))

      // Compile the query
      val inputUnit     = CompilationUnit.fromWvletString(query)
      val compileResult = compiler.compileSingleUnit(inputUnit)
      compileResult.reportAllErrors

      // Generate SQL
      val ctx = compileResult
        .context
        .withCompilationUnit(inputUnit)
        .withDebugRun(false)
        .newContext(Symbol.NoSymbol)

      val sql = GenSQL.generateSQL(inputUnit)(using ctx)

      val response = CompileResponse(success = true, sql = Some(sql))

      MessageCodec.of[CompileResponse].toJson(response)
    catch
      case e: WvletLangException =>
        val locationOpt =
          if e.sourceLocation != SourceLocation.NoSourceLocation then
            Some(
              ErrorLocation(
                path = e.sourceLocation.path,
                fileName = e.sourceLocation.fileName,
                line = e.sourceLocation.position.line,
                column = e.sourceLocation.position.column,
                lineContent =
                  if e.sourceLocation.codeLineAt.nonEmpty then
                    Some(e.sourceLocation.codeLineAt)
                  else
                    None
              )
            )
          else
            None

        val error = CompileError(
          statusCode = e.statusCode,
          message = e.getMessage,
          location = locationOpt
        )

        val response = CompileResponse(success = false, error = Some(error))

        MessageCodec.of[CompileResponse].toJson(response)

      case e: Throwable =>
        val error = CompileError(
          statusCode = StatusCode.INTERNAL_ERROR,
          message = Option(e.getMessage).getOrElse(e.getClass.getName)
        )

        val response = CompileResponse(success = false, error = Some(error))

        MessageCodec.of[CompileResponse].toJson(response)

  /**
    * Get the version of the Wvlet compiler
    */
  @JSExport
  def getVersion(): String = wvlet.lang.BuildInfo.version

  // Cached compiler instance for diagnostics (avoids repeated initialization and filesystem scans)
  private lazy val diagnosticsCompiler = Compiler(CompilerOptions(workEnv = WorkEnv(path = ".")))

  private def makeDiagnostic(
      line: Int,
      column: Int,
      endLine: Int,
      endColumn: Int,
      message: String,
      statusCode: String
  ): LspDiagnostic = LspDiagnostic(
    line = line,
    column = column,
    endLine = endLine,
    endColumn = endColumn,
    message = message,
    severity = "error",
    statusCode = statusCode
  )

  /**
    * Analyze a Wvlet source and return diagnostics (errors/warnings) as JSON.
    * @param content
    *   The Wvlet source code
    * @return
    *   JSON array of diagnostics: [{line, column, endLine, endColumn, message, severity,
    *   statusCode}]
    */
  @JSExport
  def analyzeDiagnostics(content: String): String =
    val diagnostics = ListBuffer.empty[LspDiagnostic]
    try
      val inputUnit = CompilationUnit.fromWvletString(content)
      val result    = diagnosticsCompiler.compileSingleUnit(inputUnit)
      result.reportAllErrors
    catch
      case e: WvletLangException =>
        val loc = e.sourceLocation
        if loc != SourceLocation.NoSourceLocation then
          diagnostics +=
            makeDiagnostic(
              loc.position.line,
              loc.position.column,
              loc.position.line,
              loc.position.column,
              e.getMessage,
              e.statusCode.toString
            )
        else
          diagnostics += makeDiagnostic(1, 1, 1, 1, e.getMessage, e.statusCode.toString)
      case e: Throwable =>
        diagnostics +=
          makeDiagnostic(
            1,
            1,
            1,
            1,
            Option(e.getMessage).getOrElse(e.getClass.getName),
            StatusCode.INTERNAL_ERROR.toString
          )
    end try

    MessageCodec.of[List[LspDiagnostic]].toJson(diagnostics.toList)

  end analyzeDiagnostics

  /**
    * Extract document symbols from a Wvlet source as JSON. Uses parse-only mode for speed.
    * @param content
    *   The Wvlet source code
    * @return
    *   JSON array of symbols: [{name, kind, startLine, startColumn, endLine, endColumn}]
    */
  @JSExport
  def getDocumentSymbols(content: String): String =
    val symbols   = ListBuffer.empty[LspSymbol]
    val inputUnit = CompilationUnit.fromWvletString(content)
    try
      inputUnit.unresolvedPlan = ParserPhase.parseOnly(inputUnit)
    catch
      case _: Throwable =>
      // Ignore parse errors — extract whatever symbols we can

    val sourceFile = inputUnit.sourceFile
    sourceFile.ensureLoaded

    def toSymbol(name: String, kind: Int, span: wvlet.lang.api.Span): LspSymbol =
      if span.exists then
        LspSymbol(
          name = name,
          kind = kind,
          startLine = sourceFile.offsetToLine(span.start) + 1,
          startColumn = sourceFile.offsetToColumn(span.start),
          endLine = sourceFile.offsetToLine(span.end) + 1,
          endColumn = sourceFile.offsetToColumn(span.end)
        )
      else
        LspSymbol(
          name = name,
          kind = kind,
          startLine = 1,
          startColumn = 1,
          endLine = 1,
          endColumn = 1
        )

    def extractSymbols(plan: LogicalPlan): Unit =
      plan match
        case pkg: PackageDef =>
          pkg.statements.foreach(extractSymbols)
        case m: ModelDef =>
          symbols += toSymbol(m.name.fullName, LspSymbolKind.Class, m.span)
        case t: TypeDef =>
          symbols += toSymbol(t.name.name, LspSymbolKind.Struct, t.span)
        case f: TopLevelFunctionDef =>
          symbols += toSymbol(f.functionDef.name.name, LspSymbolKind.Function, f.span)
        case v: ValDef =>
          symbols += toSymbol(v.name.name, LspSymbolKind.Variable, v.span)
        case p: PartialQueryDef =>
          symbols += toSymbol(p.name.name, LspSymbolKind.Function, p.span)
        case fl: FlowDef =>
          symbols += toSymbol(fl.name.name, LspSymbolKind.Function, fl.span)
        case _ =>
        // Skip other plan types

    extractSymbols(inputUnit.unresolvedPlan)

    MessageCodec.of[List[LspSymbol]].toJson(symbols.toList)

  end getDocumentSymbols

end WvletJS

/**
  * Compilation options
  */
case class CompileOptions(target: Option[String] = None, profile: Option[String] = None)

/**
  * LSP diagnostic representation for JSON serialization
  */
case class LspDiagnostic(
    line: Int,
    column: Int,
    endLine: Int,
    endColumn: Int,
    message: String,
    severity: String,
    statusCode: String
)

/**
  * LSP symbol representation for JSON serialization
  */
case class LspSymbol(
    name: String,
    kind: Int,
    startLine: Int,
    startColumn: Int,
    endLine: Int,
    endColumn: Int
)

/**
  * LSP SymbolKind constants (subset from the LSP specification)
  */
object LspSymbolKind:
  val Class: Int    = 5
  val Function: Int = 12
  val Variable: Int = 13
  val Struct: Int   = 23
