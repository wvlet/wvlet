import scala.scalanative.build.BuildTarget
import scala.scalanative.build.GC
import scala.scalanative.build.Mode
import scala.scalanative.build.NativeConfig
// sbt 2.x auto-caches task results. File / Seq[File] outputs need @transient task keys to opt out;
// the keys live in project/Keys.scala so the annotation is respected by the sbt macro.
import scala.language.implicitConversions
import WvletBuildKeys.*

val UNI_VERSION = "2026.1.15"

val TRINO_VERSION          = "476"
val AWS_SDK_VERSION        = "2.20.146"
val SCALAJS_DOM_VERSION    = "2.8.1"
val DUCKDB_JDBC_VERSION    = "1.5.4.0"
val SNOWFLAKE_JDBC_VERSION = "4.3.1"
val CAFFEINE_VERSION       = "3.2.4"

val SCALA_3 = IO.read(file("SCALA_VERSION")).trim
// ThisBuild / resolvers ++= Resolver.sonatypeOssRepos("snapshots")

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / scalaVersion         := SCALA_3

val buildSettings = Seq[Setting[?]](
  organization      := "wvlet.lang",
  description       := "wvlet: A flow-style query language",
  crossPaths        := true,
  publishMavenStyle := true,
  scalacOptions ++= Seq("-deprecation", "-feature"),
  // Tell the runtime that we are running tests in SBT
  Test / testOptions += Tests.Setup(() => sys.props("wvlet.sbt.testing") = "true"),
  Test / javaOptions += "-Dwvlet.sbt.testing=true",
  Test / parallelExecution := false,
  Test / logBuffered       := false,
  libraryDependencies +=
    // scala3-library is not published for Scala Native 0.5 (no _native0.5_3 artifact); the
    // Scala.js workaround in the linked thread only applies to JS/JVM, so keep it out of the
    // shared cross-project deps.
    "org.wvlet.uni" %% "uni-test" % UNI_VERSION % Test,
  testFrameworks += new TestFramework("wvlet.uni.test.Framework"),
  // Don't use pipelining as it tends to slowdown the build
  usePipelining := false
)

// uni-core's JS compat$ references java.security.SecureRandom, so any Scala.js module that
// links uni's randomness or ULID code paths needs this polyfill on the classpath. The artifact
// name is hard-coded with the sjs1_2.13 classifier because the polyfill is only published
// against the Scala 2.13 binary ABI on Scala.js 1.x — `%%%` would not resolve under Scala 3.
val scalajsJavaSecureRandom: ModuleID =
  "org.scala-js" % "scalajs-java-securerandom_sjs1_2.13" % "1.0.0"

// uni-native ships `uni_curl_shim.c` in its resources, which is unconditionally compiled into
// every downstream Scala Native binary and calls `curl_easy_setopt` / `curl_easy_getinfo`.
// Scala Native's `@link("curl")` on `CurlBindings` only fires when the Extern object is
// reachable through DCE — test binaries that don't touch the HTTP client would otherwise link
// the shim without `-lcurl` and fail with `undefined reference to curl_easy_setopt`.
val uniNativeCurlLinking =
  nativeConfig ~= { c =>
    c.withLinkingOptions(c.linkingOptions :+ "-lcurl")
  }

lazy val jvmProjects: Seq[ProjectReference] = Seq(
  api.jvm,
  httpServer,
  server,
  lang.jvm,
  connector,
  runner,
  client.jvm,
  spec,
  cli,
  cliCore.jvm,
  testUtil
)

lazy val jsProjects: Seq[ProjectReference] = Seq(
  api.js,
  client.js,
  lang.js,
  ui,
  uiMain,
  playground,
  sdkJs,
  cliCore.js
)

lazy val nativeProjects: Seq[ProjectReference] = Seq(
  api.native,
  lang.native,
  cliCore.native,
  wvc,
  wvcLib
)

val noPublish = Seq(
  publishArtifact := false,
  publish         := {},
  publishLocal    := {},
  publish / skip  := true,
  // Aggregated project should not be a part of pipelining
  usePipelining := false,
  // Skip importing aggregated projects in IntelliJ IDEA
  ideSkipProject := true
  // Use a stable coverage directory name without containing scala version
  // coverageDataDir := target.value
)

Global / excludeLintKeys += ideSkipProject

lazy val projectJVM    = project.settings(noPublish).aggregate(jvmProjects: _*)
lazy val projectJS     = project.settings(noPublish).aggregate(jsProjects: _*)
lazy val projectNative = project.settings(noPublish).aggregate(nativeProjects: _*)

lazy val api = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .crossType(CrossType.Pure)
  .in(file("wvlet-api"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildSettings,
    name          := "wvlet-api",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoOptions += BuildInfoOption.BuildTime,
    buildInfoPackage                       := "wvlet.lang",
    libraryDependencies += "org.wvlet.uni" %% "uni" % UNI_VERSION
  )
  .jsSettings(libraryDependencies += scalajsJavaSecureRandom)
  .nativeSettings(uniNativeCurlLinking)

def generateWvletLib(path: File, packageName: String, className: String): String = {
  val srcDir      = path
  val wvFiles     = (srcDir ** "*.wv").get()
  val methodNames = Seq.newBuilder[String]

  def resourceDefs: String = wvFiles
    .map { f =>
      // Use replace instead of replaceAll to handle both Unix and Windows path separators
      val name = f
        .relativeTo(srcDir)
        .get
        .getPath
        .stripSuffix(".wv")
        .replace("/", "__")
        .replace("\\", "__")

      val methodName = name.replaceAll("-", "_")
      methodNames += methodName
      val content = IO
        .read(f)
        .replaceAll("""\$""", """\$\$""")
        .replaceAll("""\"\"\"""", """\${"\\"\\"\\""}""")
      s"""|  def _${methodName}: String = s\"\"\"${content}\"\"\"
          |""".stripMargin
    }
    .mkString("\n")

  def allFiles: String = {
    val allMethods = methodNames.result().sorted
    s"""  def allFiles: ListMap[String, String] = ListMap(
       |    ${allMethods
        .map(m => s""""${m.replaceAll("__", "/")}.wv"-> _${m}""")
        .mkString(",\n    ")}
       |  )
       |""".stripMargin
  }

  def body =
    s"""package ${packageName}
       |import scala.collection.immutable.ListMap
       |
       |object ${className}:
       |${resourceDefs}
       |${allFiles}
       |end ${className}
       |""".stripMargin

  body
}

lazy val lang = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .crossType(CrossType.Pure)
  .in(file("wvlet-lang"))
  .settings(
    buildSettings,
    name := "wvlet-lang",
    // Embed the standard library in the jar
    Compile / unmanagedResourceDirectories += (ThisBuild / baseDirectory).value / "wvlet-stdlib",
    libraryDependencies ++=
      Seq(
        "org.wvlet.uni" %% "uni" % UNI_VERSION,
        // For resolving parquet file schema
        "org.duckdb" % "duckdb_jdbc" % DUCKDB_JDBC_VERSION
      ),
    stdlibGen := {
      val libDir     = (ThisBuild / baseDirectory).value / "wvlet-stdlib" / "module"
      val targetFile = (Compile / sourceManaged).value / "stdlib.scala"
      val body       = generateWvletLib(libDir, "wvlet.lang.stdlib", "StdLib")
      state.value.log.debug(s"Generating stdlib.scala:\n${body}")
      IO.write(targetFile, body)
      Seq(targetFile)
    },
    Compile / sourceGenerators += stdlibGen.taskValue,
    // Watch changes of example .wv files upon testing. In sbt 2, Seq[File] outputs are
    // rejected by the caching layer, so wrap the value in Def.uncached (same trick uni
    // uses for JSEnv).
    Test / watchSources ++=
      Def.uncached(
        ((ThisBuild / baseDirectory).value / "spec" ** "*.wv").get() ++
          ((ThisBuild / baseDirectory).value / "wvlet-stdlib" ** "*.wv").get()
      )
  )
  .jvmSettings(
    // scala3-compiler is JVM-only; kept out of the shared crossProject deps so `%%` on
    // Scala.js/Native doesn't try to resolve non-existent _sjs1_3 / _native0.5_3 artifacts.
    libraryDependencies += "org.scala-lang" %% "scala3-compiler" % SCALA_3 % Test
  )
  .jsSettings(
    libraryDependencies += scalajsJavaSecureRandom,
    // wvlet-lang.js targets Node.js — uni.io.FileSystemJS uses @JSImport for `os` / `fs` /
    // `path` / `zlib`, which the linker can only emit under a real ModuleKind. CommonJSModule
    // is the most compatible option for Node-side test runs and downstream consumers.
    scalaJSLinkerConfig ~= {
      _.withModuleKind(ModuleKind.CommonJSModule)
    }
  )
  .nativeSettings(uniNativeCurlLinking)
  .dependsOn(api)

val specRunnerSettings = Seq(
  // Fork JVM to enable JVM options for Trino
  Test / fork := true,
  // When forking, the base directory should be set to the root directory
  Test / baseDirectory := (ThisBuild / baseDirectory).value,
  // Watch changes of example .wv files upon testing. Def.uncached wraps the Seq[File]
  // value so sbt 2's cached-task validator does not reject a non-serializable File output.
  Test / watchSources ++=
    Def.uncached(
      ((ThisBuild / baseDirectory).value / "spec" ** "*.wv").get() ++
        ((ThisBuild / baseDirectory).value / "wvlet-lang" ** "*.wv").get()
    )
)

lazy val wvc = project
  .enablePlugins(ScalaNativePlugin)
  .in(file("wvc"))
  .settings(buildSettings, name := "wvc")
  .dependsOn(lang.native, cliCore.native)

lazy val wvcLib = project
  .in(file("wvc-lib"))
  .enablePlugins(ScalaNativePlugin)
  .settings(
    buildSettings,
    name := "wvc-lib",
    nativeConfig ~= { c =>
      val baseConfig = c
        .withBuildTarget(BuildTarget.libraryDynamic)
        // Generates libwvlet.so, libwvlet.dylib, libwvlet.dll
        .withBaseName("wvlet")
        .withSourceLevelDebuggingConfig(_.enableAll) // enable generation of debug information
        // Boehm GC's non-moving behavior helps avoid Segmentation Fault in DLLs
        .withGC(GC.boehm)
      // Allow overriding target triple via environment variable for cross-compilation
      sys.env.get("SCALANATIVE_TARGET_TRIPLE") match {
        case Some(triple) =>
          baseConfig.withTargetTriple(triple)
        case None =>
          baseConfig
      }
    }
  )
  .dependsOn(wvc)

lazy val wvcLibStatic = project
  .in(file("wvc-lib"))
  .enablePlugins(ScalaNativePlugin)
  .settings(
    buildSettings,
    name   := "wvc-lib",
    target := Def.uncached(target.value / "static"),
    nativeConfig ~= { c =>
      c.withBuildTarget(BuildTarget.libraryStatic).withBaseName("wvlet")
    }
  )
  .dependsOn(wvc)

/**
  * @param name
  * @param llvmTriple
  *   https://clang.llvm.org/docs/CrossCompilation.html
  * @return
  */
def nativeCrossProject(
    name: String,
    llvmTriple: String,
    compileOptions: Seq[String] = Seq.empty,
    linkerOptions: Seq[String] = Seq.empty
) = {
  val id = s"wvc-${name}"
  Project(id = id, file(s"wvlet-native-cli"))
    .enablePlugins(ScalaNativePlugin)
    .settings(noPublish)
    .settings(
      target := Def.uncached((ThisBuild / baseDirectory).value / id / "target"),
      nativeConfig ~= { c =>
        c.withTargetTriple(llvmTriple)
          .withCompileOptions(c.compileOptions ++ compileOptions)
          .withLinkingOptions(c.linkingOptions ++ linkerOptions)
          .withBuildTarget(BuildTarget.libraryDynamic)
      }
    )
    .dependsOn(wvcLib)
}

// Cross compile for different platforms
// Native libraries (include headers in C) will be necessary for nativeLink,
// So we may need to use https://github.com/dockcross/dockcross to cross build native libraries
// DEBUG: temporarily commented out for sbt 2 migration
// lazy val nativeCliMacArm = nativeCrossProject(...)
// lazy val nativeCliLinuxIntel = ...
// lazy val nativeCliLinuxArm = ...
// lazy val nativeCliWindowsArm = ...
// lazy val nativeCliWindowsIntel = ...

val packQuick = taskKey[Unit]("Run pack task quickly for faster development")

lazy val cli = project
  .in(file("wvlet-cli"))
  .enablePlugins(PackPlugin)
  .settings(
    buildSettings,
    name := "wvlet-cli",
    // Need to fork a JVM to avoid DuckDB crash while running runner/cli test simultaneously
    Test / fork          := true,
    Test / baseDirectory := (ThisBuild / baseDirectory).value,
    // Build the web UI assets (pnpm + Vite) before packing. In sbt 2, sbt-pack 1.0.0's
    // `pack` no longer lives in the `Runtime` scope, so we can't wrap it via
    // `(Runtime / pack).toTask`. Instead, run the asset build as a `pack`-time dependency
    // through a dedicated task key.
    packUiAssets := {
      // Install pnpm dependencies
      scala
        .sys
        .process
        .Process(List("pnpm", "install", "--silent"), (ThisBuild / baseDirectory).value)
        .!
      // Trigger compilation from Scala.js to JS
      val _ = (uiMain / Compile / fullLinkJS).value
      // Package the web assets using Vite.js
      scala
        .sys
        .process
        .Process(List("pnpm", "run", "--silent", "build-ui"), (ThisBuild / baseDirectory).value)
        .!
    },
    // Non-UI pack for faster development iteration
    packQuick := Def.uncached(pack.value),
    pack      := Def.uncached((pack dependsOn packUiAssets).value),
    packMain  :=
      Map(
        // Wvlet REPL launcher
        "wv" -> "wvlet.lang.cli.WvletREPLMain",
        // wvlet compiler/run/ui server command launcher
        "wvlet" -> "wvlet.lang.cli.WvletMain"
      ),
    packJvmVersionSpecificOpts := {
      val jvmOpts = Seq(
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED"
      )
      Map("wv" -> Map(25 -> jvmOpts), "wvlet" -> Map(25 -> jvmOpts))
    },
    packResourceDir ++= Map(file("wvlet-ui-main/dist") -> "web")
  )
  .dependsOn(server)

/**
  * Shared test helpers that wrap uni-test (e.g., AirSpec-DI-style `initDesign` + per-test
  * dependency injection).
  */
lazy val testUtil = project
  .in(file("wvlet-test-util"))
  .settings(
    buildSettings,
    noPublish,
    ideSkipProject := false,
    name           := "wvlet-test-util",
    libraryDependencies ++=
      Seq("org.wvlet.uni" %% "uni" % UNI_VERSION, "org.wvlet.uni" %% "uni-test" % UNI_VERSION)
  )

// Capability-based Connector traits, the factory registry, and the built-in engine connectors
// (DuckDB, Trino, Snowflake) extracted from wvlet-runner. One module for now; packages are kept
// per-engine (wvlet.lang.connector.{duckdb,trino,snowflake}) so future service connectors with
// heavy dependencies can split into their own modules mechanically.
// See https://github.com/wvlet/wvlet/issues/1861
lazy val connector = project
  .in(file("wvlet-connector"))
  .settings(
    buildSettings,
    name        := "wvlet-connector",
    description := "Connector interface and built-in engine connectors for wvlet",
    libraryDependencies ++=
      Seq(
        // ConnectorCatalog's table-schema cache
        "com.github.ben-manes.caffeine" % "caffeine"       % CAFFEINE_VERSION,
        "org.duckdb"                    % "duckdb_jdbc"    % DUCKDB_JDBC_VERSION,
        "net.snowflake"                 % "snowflake-jdbc" % SNOWFLAKE_JDBC_VERSION
      )
  )
  .dependsOn(lang.jvm)

lazy val runner = project
  .in(file("wvlet-runner"))
  .settings(
    buildSettings,
    specRunnerSettings,
    name        := "wvlet-runner",
    description := "wvlet query executor using trino, duckdb, etc.",
    Test / javaOptions ++= Seq("--enable-native-access=ALL-UNNAMED"),
    libraryDependencies ++=
      Seq(
        "org.jline"        % "jline"        % "4.3.1",
        "org.apache.arrow" % "arrow-vector" % "19.0.0",
        // SQLite-backed flow run store (cross-process cancellation and concurrency claims)
        "org.xerial" % "sqlite-jdbc" % "3.53.2.0",
        // trino-jdbc removed in PR-D: TrinoConnector now talks the Trino REST protocol via uni's
        // HttpSyncClient (see wvlet-lang's TrinoSqlConnector). trino-testing stays in test scope
        // for the in-process TestingTrinoServer — that artifact doesn't pull in trino-jdbc.
        // exclude() and jar() are necessary to avoid https://github.com/sbt/sbt/issues/7407
        // tpc-h connector neesd to download GB's of jar, so excluding it
        "io.trino" % "trino-testing" % TRINO_VERSION % Test exclude ("io.trino", "trino-tpch"),
        // Trino uses trino-plugin packaging name in pom.xml, so we need to specify jar() package explicitly
        ("io.trino" % "trino-delta-lake" % TRINO_VERSION % Test)
          .exclude("io.trino", "trino-tpch")
          .exclude("io.trino", "trino-hive")
          .jar(),
        // hive and hdfs are necessary for accessing delta lake tables
        ("io.trino" % "trino-hive" % TRINO_VERSION % Test).exclude("io.trino", "trino-tpch").jar(),
        ("io.trino" % "trino-hdfs" % TRINO_VERSION % Test).jar(),
        ("io.trino" % "trino-memory" % TRINO_VERSION % Test).exclude("io.trino", "trino-tpch").jar()
        //        // Add Spark as a reference impl (Scala 2)
        //        "org.apache.spark" %% "spark-sql" % "3.5.1" % Test excludeAll (
        //          // exclude sbt-parser-combinators as it conflicts with Scala 3
        //          ExclusionRule(organization = "org.scala-lang.modules", name = "scala-parser-combinators_2.13")
        //        ) cross (CrossVersion.for3Use2_13)
      )
  )
  .dependsOn(lang.jvm, connector, testUtil % Test)

lazy val spec = project
  .in(file("wvlet-spec"))
  .settings(buildSettings, specRunnerSettings, noPublish, name := "wvlet-spec")
  .dependsOn(runner, testUtil % Test)

lazy val sdkJs = project
  .in(file("wvlet-sdk-js"))
  .enablePlugins(ScalaJSPlugin)
  .settings(
    buildSettings,
    name := "wvlet-sdk-js",
    // Configure Scala.js output as ES module
    scalaJSLinkerConfig ~= {
      _.withModuleKind(ModuleKind.ESModule)
    },
    // Configure output directory
    Compile / fastLinkJS / scalaJSLinkerOutputDirectory :=
      (ThisBuild / baseDirectory).value / "sdks" / "typescript" / "lib",
    Compile / fullLinkJS / scalaJSLinkerOutputDirectory :=
      (ThisBuild / baseDirectory).value / "sdks" / "typescript" / "lib"
  )
  .dependsOn(lang.js)

// Cross-platform wvlet CLI surface. Same `version` / `compile` / `to_wvlet` commands across
// JVM, Node.js, and Scala Native. Compile-only — does not pull in wvlet-runner / wvlet-server,
// so JS and Native bundles stay slim. The full-featured `wvlet` JVM command (run / ui / REPL)
// is still produced by wvlet-cli, which can layer on top of this.
lazy val cliCore = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .crossType(CrossType.Pure)
  .in(file("wvlet-cli-core"))
  .settings(buildSettings, noPublish, name := "wvlet-cli-core")
  .jsSettings(
    libraryDependencies += scalajsJavaSecureRandom,
    scalaJSUseMainModuleInitializer := true,
    Compile / mainClass             := Some("wvlet.lang.cli.WvletCliMain"),
    scalaJSLinkerConfig ~= {
      // The repo package.json sets "type": "module", so the linked .js must be an ES module.
      _.withModuleKind(ModuleKind.ESModule)
    },
    // Link directly into the npm package layout. `bin/wvlet.js` imports `lib/main.js`, so
    // `pnpm run build` (which calls `./sbt cliCoreJS/fullLinkJS`) drops the bundle straight
    // into the publishable folder.
    Compile / fastLinkJS / scalaJSLinkerOutputDirectory :=
      (ThisBuild / baseDirectory).value / "sdks" / "cli-node" / "lib",
    Compile / fullLinkJS / scalaJSLinkerOutputDirectory :=
      (ThisBuild / baseDirectory).value / "sdks" / "cli-node" / "lib"
  )
  .nativeSettings(
    // The C wrappers in `lang.native`'s `src/main/resources/scala-native/` (duckdb helpers)
    // and uni-native's resources (`uni_curl_shim.c`) are unconditionally compiled and linked
    // into every downstream Native binary, so their `duckdb_*` / `curl_easy_*` references
    // need `-lduckdb` / `-lcurl` available at the final link step. Scala Native's
    // `@link("duckdb")` / `@link("curl")` annotations only trigger when their extern objects
    // are reachable, and cli-core tests / binaries typically don't reach those code paths.
    // Add the flags explicitly here. (Local macOS linker tolerates the missing flags because
    // the dylibs are already on the search path; GNU ld on Linux is strict.)
    uniNativeCurlLinking,
    nativeConfig ~= { c =>
      c.withLinkingOptions(c.linkingOptions :+ "-lduckdb")
    }
  )
  .dependsOn(lang)

// JVM-only host for HTTP server bits that wvlet maintains. Hosts the few pieces uni doesn't
// ship (e.g. StaticContent) and re-exports uni-netty as a transitive runtime for downstream
// modules.
lazy val httpServer = project
  .in(file("wvlet-http-server"))
  .settings(
    buildSettings,
    name := "wvlet-http-server",
    libraryDependencies ++=
      Seq("org.wvlet.uni" %% "uni" % UNI_VERSION, "org.wvlet.uni" %% "uni-netty" % UNI_VERSION)
  )

lazy val server = project
  .in(file("wvlet-server"))
  .settings(
    buildSettings,
    name := "wvlet-server",
    // Route SLF4J calls (e.g. from JDBC drivers) into java.util.logging so they share the
    // same handler as wvlet.uni.log.
    libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "2.0.18",
    uniRestart / baseDirectory        := (ThisBuild / baseDirectory).value
  )
  .dependsOn(api.jvm, client.jvm, runner, httpServer, testUtil % Test)

// Hand-written uni-RPC clients live in wvlet-client/{shared,jvm,js}/src/main/scala. The
// FrontendRPC shim aggregates the per-service clients so consumers see a single surface.
lazy val client = crossProject(JVMPlatform, JSPlatform)
  .in(file("wvlet-client"))
  .settings(buildSettings)
  .jsSettings(libraryDependencies += scalajsJavaSecureRandom)
  .dependsOn(api)

import org.scalajs.linker.interface.OutputPatterns
import org.scalajs.linker.interface.StandardConfig
import org.scalajs.linker.interface.ModuleKind
import org.scalajs.linker.interface.ModuleSplitStyle

lazy val ui = project
  .enablePlugins(ScalaJSPlugin)
  .in(file("wvlet-ui"))
  .settings(
    buildSettings,
    name        := "wvlet-ui",
    description := "UI components that can be testable with Node.js",
    // Run UI tests in headless Chromium via Playwright — gives real DOM APIs plus ES-module
    // support, unlike jsdom. Def.uncached wraps the JSEnv because it isn't JSON-serializable
    // and sbt 2 caches setting values by default.
    Test / jsEnv :=
      Def.uncached(
        new wvlet.uni.jsenv.playwright.PlaywrightJSEnv(browserName = "chromium", headless = true)
      ),
    libraryDependencies ++=
      Seq(
        "org.wvlet.uni" %% "uni"         % UNI_VERSION,
        "org.scala-js"  %% "scalajs-dom" % SCALAJS_DOM_VERSION
      )
  )
  .dependsOn(api.js, client.js)

lazy val uiMain = project
  .enablePlugins(ScalaJSPlugin)
  .in(file("wvlet-ui-main"))
  .settings(
    buildSettings,
    uiSettings,
    name        := "wvlet-ui-main",
    description := "UI main code for wvlet ui command"
  )
  .dependsOn(ui, lang.js)

lazy val playground = project
  .enablePlugins(ScalaJSPlugin)
  .in(file("wvlet-ui-playground"))
  .settings(
    buildSettings,
    uiSettings,
    name           := "wvlet-ui-playground",
    description    := "Online playground for wvlet",
    sampleQueryGen := {
      val libDir     = baseDirectory.value / "src/main/wvlet/Examples"
      val targetFile = (Compile / sourceManaged).value /
        "wvlet/lang/ui/playground/SampleQuery.scala"
      val body = generateWvletLib(libDir, "wvlet.lang.ui.playground", "SampleQuery")
      state.value.log.debug(s"Generating SampleQuery.scala:\n${body}")
      IO.write(targetFile, body)
      Seq(targetFile)
    },
    Compile / sourceGenerators += sampleQueryGen.taskValue
  )
  .dependsOn(uiMain, lang.js)

def uiSettings: Seq[Setting[?]] = Seq(
  Test / jsEnv                    := Def.uncached(new org.scalajs.jsenv.nodejs.NodeJSEnv()),
  scalaJSUseMainModuleInitializer := true,
  scalaJSLinkerConfig ~= {
    linkerConfig(_)
  }
)

def linkerConfig(config: StandardConfig): StandardConfig = {
  config
    // Check IR works properly since Scala.js 1.22.0 https://github.com/scala-js/scala-js/pull/4867
    .withCheckIR(true)
    .withSourceMap(true)
    .withModuleKind(ModuleKind.ESModule)
    .withModuleSplitStyle(ModuleSplitStyle.SmallModulesFor(List("wvlet.lang.ui")))
}

// For experimental projects
lazy val labs = project
  .in(file("wvlet-labs"))
  .settings(
    buildSettings,
    noPublish,
    // Override noPublish setting to allow IDEA import
    ideSkipProject                     := false,
    name                               := "wvlet-labs",
    libraryDependencies += "org.duckdb" % "duckdb_jdbc" % DUCKDB_JDBC_VERSION
  )
  .dependsOn(lang.jvm)
