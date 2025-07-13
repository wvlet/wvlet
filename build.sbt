import scala.scalanative.build.{BuildTarget, GC, Mode}

val AIRFRAME_VERSION    = "2025.1.14"
val AIRSPEC_VERSION     = "2025.1.14"
val TRINO_VERSION       = "476"
val AWS_SDK_VERSION     = "2.20.146"
val SCALAJS_DOM_VERSION = "2.8.0"

val SCALA_3 = IO.read(file("SCALA_VERSION")).trim
ThisBuild / resolvers ++= Resolver.sonatypeOssRepos("snapshots")

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / scalaVersion         := SCALA_3

val buildSettings = Seq[Setting[?]](
  organization      := "wvlet.lang",
  description       := "wvlet: A flow-style query language",
  crossPaths        := true,
  publishMavenStyle := true,
  // Tell the runtime that we are running tests in SBT
  Test / testOptions += Tests.Setup(_ => sys.props("wvlet.sbt.testing") = "true"),
  Test / javaOptions += "-Dwvlet.sbt.testing=true",
  Test / parallelExecution := true,
  Test / logBuffered       := false,
  libraryDependencies ++= Seq("org.wvlet.airframe" %%% "airspec" % AIRSPEC_VERSION % Test),
  testFrameworks += new TestFramework("wvlet.airspec.Framework"),
  // Don't use pipelining as it tends to slowdown the build
  usePipelining := false
)

lazy val jvmProjects: Seq[ProjectReference] = Seq(
  api.jvm,
  server,
  lang.jvm,
  runner,
  client.jvm,
  spec,
  cli
)

lazy val jsProjects: Seq[ProjectReference] = Seq(
  api.js,
  client.js,
  lang.js,
  ui,
  uiMain,
  playground,
  sdkJs
)

lazy val nativeProjects: Seq[ProjectReference] = Seq(api.native, lang.native, wvc, wvcLib)

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
    buildInfoPackage := "wvlet.lang",
    libraryDependencies ++=
      Seq(
        "org.wvlet.airframe" %%% "airframe-http"    % AIRFRAME_VERSION,
        "org.wvlet.airframe" %%% "airframe-metrics" % AIRFRAME_VERSION
      )
  )

def generateWvletLib(path: File, packageName: String, className: String): String = {
  val srcDir      = path
  val wvFiles     = (srcDir ** "*.wv").get
  val methodNames = Seq.newBuilder[String]

  def resourceDefs: String = wvFiles
    .map { f =>
      val name = f.relativeTo(srcDir).get.getPath.stripSuffix(".wv").replaceAll("/", "__")

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
    Compile / unmanagedResourceDirectories +=
      (ThisBuild / baseDirectory).value / "wvlet-stdlib",
    libraryDependencies ++=
      Seq(
        "org.wvlet.airframe" %% "airframe" % AIRFRAME_VERSION,
        // For reading profile
        "org.wvlet.airframe" %% "airframe-config" % AIRFRAME_VERSION,
        "org.wvlet.airframe" %% "airframe-ulid"   % AIRFRAME_VERSION,
        // For resolving parquet file schema
        "org.duckdb" % "duckdb_jdbc" % "1.3.2.0",
        // Add a reference implementation of the compiler
        "org.scala-lang" %% "scala3-compiler" % SCALA_3 % Test
      ),
    Compile / sourceGenerators +=
      Def
        .task {
          // Generate a Scala file containing all .wv files in wvlet-stdlib
          val libDir     = (ThisBuild / baseDirectory).value / "wvlet-stdlib" / "module"
          val targetFile = (Compile / sourceManaged).value / "stdlib.scala"
          val body       = generateWvletLib(libDir, "wvlet.lang.stdlib", "StdLib")
          state.value.log.debug(s"Generating stdlib.scala:\n${body}")
          IO.write(targetFile, body)
          Seq(targetFile)
        }
        .taskValue,
    // Watch changes of example .wv files upon testing
    Test / watchSources ++=
      ((ThisBuild / baseDirectory).value / "spec" ** "*.wv").get ++
        ((ThisBuild / baseDirectory).value / "wvlet-stdlib" ** "*.wv").get
  )
  .dependsOn(api)

val specRunnerSettings = Seq(
  // Fork JVM to enable JVM options for Trino
  Test / fork := true,
  // When forking, the base directory should be set to the root directory
  Test / baseDirectory :=
    (ThisBuild / baseDirectory).value,
  // Watch changes of example .wv files upon testing
  Test / watchSources ++=
    ((ThisBuild / baseDirectory).value / "spec" ** "*.wv").get ++
      ((ThisBuild / baseDirectory).value / "wvlet-lang" ** "*.wv").get
)

lazy val wvc = project
  .enablePlugins(ScalaNativePlugin)
  .in(file("wvc"))
  .settings(buildSettings, name := "wvc")
  .dependsOn(lang.native)

lazy val wvcLib = project
  .in(file("wvc-lib"))
  .enablePlugins(ScalaNativePlugin)
  .settings(
    buildSettings,
    name := "wvc-lib",
    nativeConfig ~= { c =>
      c.withBuildTarget(BuildTarget.libraryDynamic)
        // Generates libwvlet.so, libwvlet.dylib, libwvlet.dll
        .withBaseName("wvlet")
        .withSourceLevelDebuggingConfig(_.enableAll) // enable generation of debug information
        // Boehm GC's non-moving behavior helps avoid Segmentation Fault in DLLs
        .withGC(GC.boehm)
    }
  )
  .dependsOn(wvc)

lazy val wvcLibStatic = project
  .in(file("wvc-lib"))
  .enablePlugins(ScalaNativePlugin)
  .settings(
    buildSettings,
    name   := "wvc-lib",
    target := target.value / "static",
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
      target := (ThisBuild / baseDirectory).value / id / "target",
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
lazy val nativeCliMacArm = nativeCrossProject(
  "mac-arm64",
  "arm64-apple-darwin",
  // Need to use LLD linker as the default linker never understands cross-build target
  linkerOptions = Seq("-fuse-ld=ld64.lld")
)

lazy val nativeCliMacIntel = nativeCrossProject(
  "mac-x64",
  "x86_64-apple-darwin",
  linkerOptions = Seq("-fuse-ld=ld64.lld")
)

lazy val nativeCliLinuxIntel = nativeCrossProject(
  "linux-x64",
  "x86_64-unknown-linux-gnu",
  linkerOptions = Seq("-fuse-ld=ld.lld")
)

val commonClangOptions = Seq(
  "--sysroot=/usr/xcc/aarch64-unknown-linux-gnu/aarch64-unknown-linux-gnu/sysroot"
)

lazy val nativeCliLinuxArm = nativeCrossProject(
  "linux-arm64",
  "aarch64-unknown-linux-gnu",
  compileOptions = commonClangOptions ++ Seq("-I/usr/xcc/aarch64-unknown-linux-gnu/include/"),
  linkerOptions =
    commonClangOptions ++
      Seq(
        "-fuse-ld=/usr/xcc/aarch64-unknown-linux-gnu/bin/aarch64-unknown-linux-gnu-ld",
        "-L/usr/xcc/aarch64-unknown-linux-gnu/lib",
        "-L/usr/xcc/aarch64-unknown-linux-gnu/aarch64-unknown-linux-gnu/sysroot",
        "-L/usr/lib/aarch64-linux-gnu"
      )
)

lazy val nativeCliWindowsArm   = nativeCrossProject("windows-arm64", "arm64-w64-windows-gnu")
lazy val nativeCliWindowsIntel = nativeCrossProject("windows-x64", "x86_64-w64-windows-gnu")

val packQuick = taskKey[Unit]("Run pack task quickly for faster development")

lazy val cli = project
  .in(file("wvlet-cli"))
  .enablePlugins(PackPlugin)
  .settings(
    buildSettings,
    name := "wvlet-cli",
    // Need to fork a JVM to avoid DuckDB crash while running runner/cli test simultaneously
    Test / fork := true,
    Test / baseDirectory :=
      (ThisBuild / baseDirectory).value,
    packQuick :=
      // Run the default pack task
      (Runtime / pack).value,
    pack :=
      Def
        .sequential(
          Def.task[Unit] {
            // Install NPM dependencies
            scala
              .sys
              .process
              .Process(
                List("npm", "install", "--silent", "--no-audit", "--no-fund"),
                (ThisBuild / baseDirectory).value
              )
              .!
            // Trigger compilation from Scala.js to JS
            val assetFiles = (uiMain / Compile / fullLinkJS).value
            // Packaging the web assets using Vite.js
            scala
              .sys
              .process
              .Process(
                List("npm", "run", "build-ui", "--silent", "--no-audit", "--no-fund"),
                (ThisBuild / baseDirectory).value
              )
              .!
          },
          // Run the default pack task
          (Runtime / pack).toTask
        )
        .value,
    packMain :=
      Map(
        // Wvlet REPL launcher
        "wv" -> "wvlet.lang.cli.WvletREPLMain",
        // wvlet compiler/run/ui server command launcher
        "wvlet" -> "wvlet.lang.cli.WvletMain"
      ),
    packResourceDir ++= Map(file("wvlet-ui-main/dist") -> "web")
  )
  .dependsOn(server)

lazy val runner = project
  .in(file("wvlet-runner"))
  .settings(
    buildSettings,
    specRunnerSettings,
    name        := "wvlet-runner",
    description := "wvlet query executor using trino, duckdb, etc.",
    libraryDependencies ++=
      Seq(
        "org.jline"                     % "jline"             % "3.30.4",
        "org.wvlet.airframe"           %% "airframe-launcher" % AIRFRAME_VERSION,
        "com.github.ben-manes.caffeine" % "caffeine"          % "3.2.2",
        "org.apache.arrow"              % "arrow-vector"      % "18.3.0",
        "org.duckdb"                    % "duckdb_jdbc"       % "1.3.2.0",
        "io.trino"                      % "trino-jdbc"        % TRINO_VERSION,
        // exclude() and jar() are necessary to avoid https://github.com/sbt/sbt/issues/7407
        // tpc-h connector neesd to download GB's of jar, so excluding it
        "io.trino" % "trino-testing" % TRINO_VERSION % Test exclude ("io.trino", "trino-tpch"),
        // Trino uses trino-plugin packaging name in pom.xml, so we need to specify jar() package explicitly
        "io.trino" % "trino-delta-lake" % TRINO_VERSION % Test exclude
          ("io.trino", "trino-tpch") exclude
          ("io.trino", "trino-hive") jar
          (),
        // hive and hdfs are necessary for accessing delta lake tables
        "io.trino" % "trino-hive" % TRINO_VERSION % Test exclude ("io.trino", "trino-tpch") jar (),
        "io.trino" % "trino-hdfs" % TRINO_VERSION % Test jar (),
        "io.trino" % "trino-memory" % TRINO_VERSION % Test exclude ("io.trino", "trino-tpch") jar ()
        //        // Add Spark as a reference impl (Scala 2)
        //        "org.apache.spark" %% "spark-sql" % "3.5.1" % Test excludeAll (
        //          // exclude sbt-parser-combinators as it conflicts with Scala 3
        //          ExclusionRule(organization = "org.scala-lang.modules", name = "scala-parser-combinators_2.13")
        //        ) cross (CrossVersion.for3Use2_13)
      )
  )
  .dependsOn(lang.jvm)

lazy val spec = project
  .in(file("wvlet-spec"))
  .settings(buildSettings, specRunnerSettings, noPublish, name := "wvlet-spec")
  .dependsOn(runner)

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

lazy val server = project
  .in(file("wvlet-server"))
  .settings(
    buildSettings,
    name := "wvlet-server",
    libraryDependencies ++=
      Seq(
        // For redirecting slf4j logs to airframe-log
        "org.slf4j"           % "slf4j-jdk14"         % "2.0.17",
        "org.wvlet.airframe" %% "airframe-launcher"   % AIRFRAME_VERSION,
        "org.wvlet.airframe" %% "airframe-http-netty" % AIRFRAME_VERSION
      ),
    reStart / baseDirectory :=
      (ThisBuild / baseDirectory).value
  )
  .dependsOn(api.jvm, client.jvm, runner)

lazy val client = crossProject(JVMPlatform, JSPlatform)
  .in(file("wvlet-client"))
  .enablePlugins(AirframeHttpPlugin)
  .settings(buildSettings, airframeHttpClients := Seq("wvlet.lang.api.v1.frontend:rpc:FrontendRPC"))
  .dependsOn(api)

import org.scalajs.linker.interface.{OutputPatterns, StandardConfig}
import org.scalajs.linker.interface.{ModuleKind, ModuleSplitStyle}

lazy val ui = project
  .enablePlugins(ScalaJSPlugin)
  .in(file("wvlet-ui"))
  .settings(
    buildSettings,
    name         := "wvlet-ui",
    description  := "UI components that can be testable with Node.js",
    Test / jsEnv := new org.scalajs.jsenv.jsdomnodejs.JSDOMNodeJSEnv(),
    libraryDependencies ++=
      Seq(
        "org.wvlet.airframe" %%% "airframe"         % AIRFRAME_VERSION,
        "org.wvlet.airframe" %%% "airframe-http"    % AIRFRAME_VERSION,
        "org.wvlet.airframe" %%% "airframe-rx-html" % AIRFRAME_VERSION,
        "org.scala-js"       %%% "scalajs-dom"      % SCALAJS_DOM_VERSION
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
    name        := "wvlet-ui-playground",
    description := "Online playground for wvlet",
    Compile / sourceGenerators +=
      Def
        .task {
          // Generate a Scala file containing all .wv files in wvlet-stdlib
          val libDir = baseDirectory.value / "src/main/wvlet/Examples"
          val targetFile = (Compile / sourceManaged).value /
            "wvlet/lang/ui/playground/SampleQuery.scala"
          val body = generateWvletLib(libDir, "wvlet.lang.ui.playground", "SampleQuery")
          state.value.log.debug(s"Generating stdlib.scala:\n${body}")
          IO.write(targetFile, body)
          Seq(targetFile)
        }
        .taskValue
  )
  .dependsOn(uiMain, lang.js)

def uiSettings: Seq[Setting[?]] = Seq(
  Test / jsEnv                    := new org.scalajs.jsenv.nodejs.NodeJSEnv(),
  scalaJSUseMainModuleInitializer := true,
  scalaJSLinkerConfig ~= {
    linkerConfig(_)
  }
)

def linkerConfig(config: StandardConfig): StandardConfig = {
  config
    // Check IR works properly since Scala.js 1.19.0 https://github.com/scala-js/scala-js/pull/4867
    .withCheckIR(true)
    .withSourceMap(true)
    .withModuleKind(ModuleKind.ESModule)
    .withModuleSplitStyle(ModuleSplitStyle.SmallModulesFor(List("wvlet.lang.ui")))
}
