package wvlet.lang.native

import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, Symbol, WorkEnv}
import wvlet.log.{LogLevel, LogSupport, Logger}

object WvcMain extends LogSupport:

  def main(args: Array[String]): Unit =
    var inputQuery: Option[String] = None

    var workFolder                     = "."
    var displayHelp                    = false
    var logLevel: LogLevel             = LogLevel.INFO
    var logLevelPatterns: List[String] = List.empty[String]
    var remainingArgs: List[String]    = Nil

    var parseSuccess: Boolean = false

    // TODO Use generic command line parser (airframe-launcher still doesn't support Scala Native)
    def parseOption(lst: List[String]): Unit =
      lst match
        case h :: tail if h == "-h" || h == "--help" =>
          displayHelp = true
          parseOption(tail)
        case "-w" :: folder :: tail =>
          workFolder = folder
          parseOption(tail)
        case "-q" :: query :: tail =>
          inputQuery = Some(query.toString)
          parseOption(tail)
        case "-l" :: level :: tail =>
          logLevel = LogLevel(level.toString)
          parseOption(tail)
        case "-L" :: pattern :: tail =>
          logLevelPatterns = pattern.toString :: logLevelPatterns
          parseOption(tail)
        case h :: tail if h.startsWith("-") || h.startsWith("--") =>
          warn(s"Unknown option: ${h}")
          parseSuccess = false
        case rest =>
          parseSuccess = true
          remainingArgs = rest

    parseOption(args.toList)

    if !parseSuccess then
      System.exit(1)
    else if displayHelp then
      println("""wvc (Wvlet Native Compiler)
          |  Compile Wvlet files and generate SQL queries
          |
          |[usage]:
          |  wvc [options] -q '(Wvlet query)'
          |  cat query.wv | wvc [options]
          |
          |[options]
          | -h, --help         Display help message
          | -w <folder>        Working folder
          | -q <query>         Query string
          | -l <level>         Log level (info, debug, trace, warn, error)
          | -L <pattern=level> Set log level for a class pattern
          |""".stripMargin)
    else
      // Set log levels
      Logger("wvlet.lang.compiler").setLogLevel(logLevel)
      Logger("wvlet.lang.native").setLogLevel(logLevel)
      logLevelPatterns.foreach { p =>
        p.split("=") match
          case Array(pattern, level) =>
            debug(s"Set the log level for ${pattern} to ${level}")
            Logger.setLogLevel(pattern, LogLevel(level))
          case _ =>
            error(s"Invalid log level pattern: ${p}")
      }

      // Prepare a compiler and input source
      val compiler = Compiler(
        CompilerOptions(workEnv = WorkEnv(path = workFolder), sourceFolders = List(workFolder))
      )
      val query: String =
        inputQuery match
          case Some(q) =>
            q
          case None =>
            import scala.scalanative.posix.unistd
            val connectedToStdin = unistd.isatty(unistd.STDIN_FILENO) == 0
            if connectedToStdin then
              // Read from stdin
              Iterator.continually(scala.io.StdIn.readLine()).takeWhile(_ != null).mkString("\n")
            else
              ""

      if query.trim.isEmpty then
        warn(s"No query is given. Use -q 'query' option or stdin to feed the query")
      else
        // Compile
        val inputUnit     = CompilationUnit.fromString(query)
        val compileResult = compiler.compileSingleUnit(inputUnit)
        compileResult.reportAllErrors
        val ctx = compileResult
          .context
          .withCompilationUnit(inputUnit)
          .withDebugRun(false)
          .newContext(Symbol.NoSymbol)

        val sql = GenSQL.generateSQL(inputUnit, ctx)
        println(sql)

    end if

  end main

end WvcMain
