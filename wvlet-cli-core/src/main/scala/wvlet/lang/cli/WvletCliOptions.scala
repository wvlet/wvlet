package wvlet.lang.cli

import wvlet.lang.BuildInfo
import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.DBType
import wvlet.lang.runner.ModelBuildPlan
import wvlet.uni.cli.launcher.argument
import wvlet.uni.cli.launcher.option
import wvlet.uni.io.IO
import wvlet.uni.log.LogLevel
import wvlet.uni.log.LogSupport
import wvlet.uni.log.Logger

/**
  * Global options of every `wvlet` distribution (npm, Native, and the JVM CLI) and of the `wv` REPL
  * launcher. Constructing it applies the log levels; `--version` prints the version and ends the
  * command with [[StatusCode.EXIT_SUCCESSFULLY]].
  */
case class WvletCliGlobalOption(
    @option(prefix = "--version", description = "Display the version")
    displayVersion: Boolean = false,
    @option(prefix = "--debug", description = "Enable debug log")
    debugMode: Boolean = false,
    @option(prefix = "-l,--loglevel", description = "Log level (trace, debug, info, warn, error)")
    logLevelName: Option[String] = None,
    @option(prefix = "-L", description = "Log level for a class pattern (pattern=level)")
    logLevelPatterns: List[String] = List.empty
) extends LogSupport:

  /** The log level of query runs: `--debug` wins over `-l` */
  def logLevel: LogLevel =
    if debugMode then
      LogLevel.DEBUG
    else
      logLevelName.map(LogLevel(_)).getOrElse(LogLevel.INFO)

  def withDisplayVersion(display: Boolean): WvletCliGlobalOption = copy(displayVersion = display)
  def withDebugMode(debug: Boolean): WvletCliGlobalOption        = copy(debugMode = debug)
  def withLogLevelName(level: String): WvletCliGlobalOption      = copy(logLevelName = Some(level))
  def noLogLevelName(): WvletCliGlobalOption                     = copy(logLevelName = None)
  def withLogLevelPatterns(patterns: List[String]): WvletCliGlobalOption = copy(logLevelPatterns =
    patterns
  )

  def versionString = s"wvlet version: ${BuildInfo.version} (Built at: ${BuildInfo.builtAtString})"

  Logger("wvlet.lang.runner").setLogLevel(logLevel)

  if displayVersion then
    println(versionString)
    throw StatusCode.EXIT_SUCCESSFULLY.newException("exit successfully")

  logLevelPatterns.foreach { p =>
    p.split("=") match
      case Array(pattern, level) =>
        debug(s"Set the log level for ${pattern} to ${level}")
        Logger.setLogLevel(pattern, LogLevel(level))
      case _ =>
        error(s"Invalid log level pattern: ${p}")
  }

end WvletCliGlobalOption

/**
  * Input and profile options shared by `compile`, `to_wvlet`, and `run`.
  */
case class WvletCliCompileOption(
    @option(prefix = "-w", description = "Working folder")
    workFolder: String = ".",
    @option(prefix = "-f,--file", description = "Read a query from the given .wv file")
    file: Option[String] = None,
    @argument(description = "query")
    query: Option[String] = None,
    @option(
      prefix = "-t,--target",
      description =
        "Target engine: wvlet (in-process engine, the default for run), duckdb (the default for compile), trino, ..."
    )
    targetDBType: Option[String] = None,
    @option(prefix = "-p,--profile", description = "Profile name from ~/.wvlet/profiles.json")
    profile: Option[String] = None,
    @option(prefix = "--catalog", description = "Context database catalog to use")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Context database schema to use")
    schema: Option[String] = None,
    @option(prefix = "--strict", description = "Fail compilation on typing errors")
    strict: Boolean = false
):
  def withWorkFolder(folder: String): WvletCliCompileOption   = copy(workFolder = folder)
  def withFile(file: String): WvletCliCompileOption           = copy(file = Some(file))
  def noFile(): WvletCliCompileOption                         = copy(file = None)
  def withQuery(query: String): WvletCliCompileOption         = copy(query = Some(query))
  def noQuery(): WvletCliCompileOption                        = copy(query = None)
  def withTargetDBType(dbType: String): WvletCliCompileOption = copy(targetDBType = Some(dbType))
  def noTargetDBType(): WvletCliCompileOption                 = copy(targetDBType = None)
  def withProfile(profile: String): WvletCliCompileOption     = copy(profile = Some(profile))
  def noProfile(): WvletCliCompileOption                      = copy(profile = None)
  def withCatalog(catalog: String): WvletCliCompileOption     = copy(catalog = Some(catalog))
  def noCatalog(): WvletCliCompileOption                      = copy(catalog = None)
  def withSchema(schema: String): WvletCliCompileOption       = copy(schema = Some(schema))
  def noSchema(): WvletCliCompileOption                       = copy(schema = None)
  def withStrict(strict: Boolean): WvletCliCompileOption      = copy(strict = strict)

  /**
    * The profile to compile and run with:
    *   - `-p` loads a profile from ~/.wvlet/profiles.json, and `-t` alone uses the built-in profile
    *     of that engine. Without either, `default` applies
    *   - `-t` with `-p` replaces the type of the profile's default engine and keeps its other
    *     settings (e.g., the host of a remote wvlet server)
    *   - `--catalog` and `--schema` override the default engine's
    */
  def resolveProfile(default: => Profile): Profile =
    val base =
      (profile, targetDBType) match
        case (Some(_), Some(target)) =>
          Profile
            .getProfile(profile, default = default)
            .updateDefaultEngine(_.withType(target.toLowerCase))
        case (None, Some(target)) =>
          Profile.defaultProfileFor(DBType.fromString(target))
        case _ =>
          Profile.getProfile(profile, default = default)
    base.withEngineOverrides(catalog, schema)

end WvletCliCompileOption

/**
  * Output and connection options of `run`. Credentials (passwords, server tokens) come from the
  * profile only, with `${ENV}` interpolation, and never from these flags.
  */
case class WvletCliRunOption(
    @option(prefix = "--format", description = "Output format: box (default), csv")
    format: String = "box",
    @option(
      prefix = "--host",
      description = "Host of the Trino coordinator, or of a wvlet server with -t wvlet"
    )
    host: Option[String] = None,
    @option(
      prefix = "--port",
      description =
        "Port of --host (default: 443 with --https, else 8080 for Trino and 9090 for a wvlet server)"
    )
    port: Option[Int] = None,
    @option(prefix = "--user", description = "User name for Trino")
    user: Option[String] = None,
    @option(prefix = "--https", description = "Use HTTPS to reach --host")
    useHttps: Boolean = false
):
  def withFormat(format: String): WvletCliRunOption      = copy(format = format)
  def withHost(host: String): WvletCliRunOption          = copy(host = Some(host))
  def noHost(): WvletCliRunOption                        = copy(host = None)
  def withPort(port: Int): WvletCliRunOption             = copy(port = Some(port))
  def noPort(): WvletCliRunOption                        = copy(port = None)
  def withUser(user: String): WvletCliRunOption          = copy(user = Some(user))
  def noUser(): WvletCliRunOption                        = copy(user = None)
  def withUseHttps(useHttps: Boolean): WvletCliRunOption = copy(useHttps = useHttps)

  /**
    * Apply the connection flags to the profile's default engine. `--https` is a Boolean flag that
    * can't distinguish "false" from "not given", so the profile's setting applies unless it is set.
    */
  def applyTo(profile: Profile): Profile =
    if host.isEmpty && port.isEmpty && user.isEmpty && !useHttps then
      profile
    else
      profile.updateDefaultEngine { engine =>
        engine.copy(
          host = host.orElse(engine.host),
          port = port.orElse(engine.port),
          user = user.orElse(engine.user),
          useHttps =
            if useHttps then
              Some(true)
            else
              engine.useHttps
        )
      }

end WvletCliRunOption

/**
  * Options of the REPL (`wvlet repl`, bare `wvlet` on a terminal, and the `wv` alias)
  */
case class WvletREPLOption(
    @option(prefix = "-p,--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(
      prefix = "-t,--target",
      description = "Engine to use without a profile (wvlet (default), duckdb, ...)"
    )
    targetDBType: Option[String] = None,
    @option(prefix = "-c", description = "Run a command and exit")
    commands: List[String] = Nil,
    @option(prefix = "-f,--file", description = "Run commands in a file and exit")
    inputFile: Option[String] = None,
    @option(prefix = "-w", description = "Working folder")
    workFolder: String = ".",
    @option(prefix = "--catalog", description = "Context database catalog to use")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Context database schema to use")
    schema: Option[String] = None
):
  def withProfile(profile: String): WvletREPLOption         = copy(profile = Some(profile))
  def noProfile(): WvletREPLOption                          = copy(profile = None)
  def withTargetDBType(dbType: String): WvletREPLOption     = copy(targetDBType = Some(dbType))
  def noTargetDBType(): WvletREPLOption                     = copy(targetDBType = None)
  def withCommands(commands: List[String]): WvletREPLOption = copy(commands = commands)
  def withInputFile(file: String): WvletREPLOption          = copy(inputFile = Some(file))
  def noInputFile(): WvletREPLOption                        = copy(inputFile = None)
  def withWorkFolder(folder: String): WvletREPLOption       = copy(workFolder = folder)
  def withCatalog(catalog: String): WvletREPLOption         = copy(catalog = Some(catalog))
  def noCatalog(): WvletREPLOption                          = copy(catalog = None)
  def withSchema(schema: String): WvletREPLOption           = copy(schema = Some(schema))
  def noSchema(): WvletREPLOption                           = copy(schema = None)

  /**
    * The profile the REPL runs with: `-t` alone picks the engine's default profile, and without
    * either `-t` or `-p`, the REPL runs on the in-process wvlet engine
    */
  def resolveProfile: Profile =
    targetDBType match
      case Some(target) =>
        if profile.isDefined then
          throw StatusCode
            .INVALID_ARGUMENT
            .newException("Specify either -t (--target) or --profile")
        Profile.defaultProfileFor(DBType.fromString(target)).withEngineOverrides(catalog, schema)
      case None =>
        Profile.getProfile(profile, catalog, schema, Profile.defaultWvletProfile)

  /** Commands given with `-c`, then the content of the `-f` file */
  def inputScripts: List[String] =
    val file = inputFile.map { f =>
      val path = IO.path(workFolder, f)
      if !IO.exists(path) then
        throw StatusCode.FILE_NOT_FOUND.newException(s"File not found: ${path.path}")
      IO.readString(path)
    }
    commands ++ file

end WvletREPLOption

/**
  * Options of `wvlet build`, the same on every distribution. `--run-store` applies to the JVM CLI
  * only, which records build runs like flow runs
  */
case class WvletBuildOption(
    @option(prefix = "-w", description = "Working folder containing .wv files")
    workFolder: String = ".",
    @option(
      prefix = "-s,--select",
      description = "Models to build (comma-separated): m, m+ (with downstream), +m (with upstream)"
    )
    select: Option[String] = None,
    @option(prefix = "--dry-run", description = "Print the build order and SQL without running")
    dryRun: Boolean = false,
    @option(prefix = "-p,--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(
      prefix = "-t,--target",
      description = "Engine to use without a profile (wvlet (default), duckdb, ...)"
    )
    targetDBType: Option[String] = None,
    @option(prefix = "--catalog", description = "Context database catalog to use")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Context database schema to use")
    schema: Option[String] = None,
    @option(
      prefix = "--run-store",
      description =
        "JVM CLI only: run store type: file (default), sqlite, or postgres (connection via WVLET_FLOW_STORE_PG_* env)"
    )
    runStore: Option[String] = None
):
  def withWorkFolder(folder: String): WvletBuildOption   = copy(workFolder = folder)
  def withSelect(select: String): WvletBuildOption       = copy(select = Some(select))
  def noSelect(): WvletBuildOption                       = copy(select = None)
  def withDryRun(dryRun: Boolean): WvletBuildOption      = copy(dryRun = dryRun)
  def withProfile(profile: String): WvletBuildOption     = copy(profile = Some(profile))
  def noProfile(): WvletBuildOption                      = copy(profile = None)
  def withTargetDBType(dbType: String): WvletBuildOption = copy(targetDBType = Some(dbType))
  def noTargetDBType(): WvletBuildOption                 = copy(targetDBType = None)
  def withCatalog(catalog: String): WvletBuildOption     = copy(catalog = Some(catalog))
  def noCatalog(): WvletBuildOption                      = copy(catalog = None)
  def withSchema(schema: String): WvletBuildOption       = copy(schema = Some(schema))
  def noSchema(): WvletBuildOption                       = copy(schema = None)
  def withRunStore(storeType: String): WvletBuildOption  = copy(runStore = Some(storeType))
  def noRunStore(): WvletBuildOption                     = copy(runStore = None)

  /**
    * The profile of the build: `-p`, `-t`, or the in-process wvlet engine, whose tables persist in
    * the work folder's `target/warehouse`
    */
  def resolveProfile: Profile = ModelBuildPlan.resolveProfile(
    workFolder,
    profile,
    targetDBType,
    catalog,
    schema
  )

  /** The selectors of `-s` (comma-split by the model graph) */
  def selectors: List[String] = select.toList

end WvletBuildOption
