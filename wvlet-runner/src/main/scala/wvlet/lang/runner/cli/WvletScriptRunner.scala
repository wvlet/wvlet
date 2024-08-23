package wvlet.lang.runner.cli

import wvlet.lang.WvletLangException
import wvlet.lang.compiler.{CompilationUnit, CompileResult, Compiler, CompilerOptions}
import wvlet.lang.runner.*
import org.jline.terminal.Terminal
import wvlet.airframe.control.{Control, Shell}
import wvlet.log.LogSupport

import java.io.{BufferedWriter, FilterOutputStream, OutputStreamWriter}

case class WvletScriptRunnerConfig(
    workingFolder: String = ".",
    interactive: Boolean,
    resultLimit: Int = 40,
    maxColWidth: Int = 150,
    catalog: Option[String],
    schema: Option[String]
)

case class LastOutput(line: String, output: String, result: QueryResult) {}

class WvletScriptRunner(config: WvletScriptRunnerConfig, queryExecutor: QueryExecutor)
    extends AutoCloseable
    with LogSupport:
  private var units: List[CompilationUnit] = Nil

  private var resultRowLimits: Int   = config.resultLimit
  private var resultMaxColWidth: Int = config.maxColWidth

  def setResultRowLimit(limit: Int): Unit = resultRowLimits = limit
  def setMaxColWidth(size: Int): Unit     = resultMaxColWidth = size

  override def close(): Unit = queryExecutor.close()

  private val compiler =
    val c = Compiler(
      CompilerOptions(
        sourceFolders = List(config.workingFolder),
        workingFolder = config.workingFolder,
        catalog = config.catalog,
        schema = config.schema
      )
    )

    // Set the default catalog given in the configuration
    config
      .catalog
      .foreach { catalog =>
        c.setDefaultCatalog(
          queryExecutor.getDBConnector.getCatalog(catalog, config.schema.getOrElse("main"))
        )
      }
    config
      .schema
      .foreach { schema =>
        c.setDefaultSchema(schema)
      }

    // Pre-compile files in the source paths
    c.compileSourcePaths(None)
    c

  def runStatement(line: String, terminal: Terminal): LastOutput =
    val newUnit = CompilationUnit.fromString(line)
    units = newUnit :: units

    try
      val compileResult = compiler.compileSingleUnit(contextUnit = newUnit)
      val ctx           = compileResult.context.global.getContextOf(newUnit)
      val queryResult   = queryExecutor.executeSingle(newUnit, ctx, limit = resultRowLimits)
      trace(s"ctx: ${ctx.hashCode()} ${ctx.compilationUnit.knownSymbols}")

      val str = queryResult.toPrettyBox(maxColWidth = resultMaxColWidth)
      if str.nonEmpty then
        val resultMaxWidth = str.split("\n").map(_.size).max
        if !config.interactive || resultMaxWidth <= terminal.getWidth then
          println(
            queryResult
              .toPrettyBox(maxWidth = Some(terminal.getWidth), maxColWidth = resultMaxColWidth)
          )
        else
          // Launch less command to enable scrolling of query results in the terminal
          val proc = ProcessUtil.launchInteractiveProcess("less", "-FXRSn")
          val out =
            new BufferedWriter(
              new OutputStreamWriter(
                // Need to use a FilterOutputStream to accept keyboard events for less command along with the query result string
                new FilterOutputStream(proc.getOutputStream())
              )
            )
          out.write(str)
          out.flush()
          out.close()
          // Blocking
          proc.waitFor()

      LastOutput(line, str, queryResult)
    catch
      case e: WvletLangException if e.statusCode.isUserError =>
        error(s"${e.getMessage}")
        LastOutput(line, e.getMessage, QueryResult.empty)
    end try

  end runStatement

end WvletScriptRunner
