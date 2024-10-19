val AIRFRAME_VERSION    = "24.9.3"
val AIRSPEC_VERSION     = "24.9.3"
val TRINO_VERSION       = "462"
val AWS_SDK_VERSION     = "2.20.146"
val SCALAJS_DOM_VERSION = "2.8.0"

val SCALA_3 = IO.read(file("SCALA_VERSION")).trim
ThisBuild / scalaVersion := SCALA_3
ThisBuild / resolvers ++= Resolver.sonatypeOssRepos("snapshots")

Global / onChangedBuildSource := ReloadOnSourceChanges

import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

val buildSettings = Seq[Setting[?]](
  organization       := "wvlet.lang",
  description        := "wvlet: A flow-style query language",
  crossPaths         := true,
  publishMavenStyle  := true,
  Test / logBuffered := false,
  libraryDependencies ++= Seq("org.wvlet.airframe" %%% "airspec" % AIRSPEC_VERSION % Test),
  testFrameworks += new TestFramework("wvlet.airspec.Framework"),
  // Prevent double trigger due to scalafmt run in IntelliJ by adding a small delay (default is 500ms)
  watchAntiEntropy := FiniteDuration(700, TimeUnit.MILLISECONDS)
)

lazy val jvmProjects: Seq[ProjectReference] = Seq(api.jvm, server, lang, runner, client.jvm, spec)

lazy val jsProjects: Seq[ProjectReference] = Seq(api.js, client.js, ui, uiMain)

val noPublish = Seq(
  publishArtifact := false,
  publish         := {},
  publishLocal    := {},
  publish / skip  := true
)

Global / excludeLintKeys += ideSkipProject

lazy val projectJVM = project
  .settings(noPublish)
  .settings(
    // Skip importing aggregated projects in IntelliJ IDEA
    ideSkipProject :=
      true
      // Use a stable coverage directory name without containing scala version
      // coverageDataDir := target.value
  )
  .aggregate(jvmProjects: _*)

lazy val projectJS = project
  .settings(noPublish)
  .settings(
    // Skip importing aggregated projects in IntelliJ IDEA
    ideSkipProject := true
  )
  .aggregate(jsProjects: _*)

lazy val api = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .in(file("wvlet-api"))
  .enablePlugins(AirframeHttpPlugin, BuildInfoPlugin)
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

lazy val lang = project
//    .enablePlugins(Antlr4Plugin)
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
        "org.duckdb" % "duckdb_jdbc" % "1.1.2",
        // Add a reference implementation of the compiler
        "org.scala-lang" %% "scala3-compiler" % SCALA_3 % Test
      ),
    // Watch changes of example .wv files upon testing
    Test / watchSources ++=
      ((ThisBuild / baseDirectory).value / "spec" ** "*.wv").get ++
        ((ThisBuild / baseDirectory).value / "wvlet-stdlib" ** "*.wv").get
  )
  .dependsOn(api.jvm)

val specRunnerSettings = Seq(
  // To enable JVM options
  Test / fork := true,
  // When forking, the base directory should be set to the root directory
  Test / baseDirectory :=
    (ThisBuild / baseDirectory).value,
  // Watch changes of example .wv files upon testing
  Test / watchSources ++=
    ((ThisBuild / baseDirectory).value / "spec" ** "*.wv").get ++
      ((ThisBuild / baseDirectory).value / "wvlet-lang" ** "*.wv").get
)

lazy val cli = project
  .in(file("wvlet-cli"))
  .enablePlugins(PackPlugin)
  .settings(
    buildSettings,
    name := "wvlet-cli",
    Compile / resourceGenerators +=
      Def
        .task {
          // Trigger compilation from Scala.js to JS
          val assetFiles = (uiMain / Compile / fullLinkJS).value
          // Packaging the web assets using Vite.js
          scala
            .sys
            .process
            .Process(
              List("npm", "run", "build", "--silent", "--no-audit", "--no-fund"),
              (uiMain / baseDirectory).value
            )
            .!
          // Return empty files to avoid recompilation
          Seq.empty[File]
        }
        .taskValue,
    packMain :=
      Map(
        // wvlet compiler
        "wvc" -> "wvlet.lang.cli.WvcMain",
        // Alias for wvlet runner and shell
        "wv" -> "wvlet.lang.cli.WvMain",
        // wvlet command launcher
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
        "org.jline"                     % "jline"             % "3.27.1",
        "org.wvlet.airframe"           %% "airframe-launcher" % AIRFRAME_VERSION,
        "com.github.ben-manes.caffeine" % "caffeine"          % "3.1.8",
        "org.apache.arrow"              % "arrow-vector"      % "17.0.0",
        "org.duckdb"                    % "duckdb_jdbc"       % "1.1.2",
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
  .dependsOn(lang)

lazy val spec = project
  .in(file("wvlet-spec"))
  .settings(buildSettings, specRunnerSettings, noPublish, name := "wvlet-spec")
  .dependsOn(runner)

lazy val server = project
  .in(file("wvlet-server"))
  .settings(
    buildSettings,
    name := "wvlet-server",
    libraryDependencies ++=
      Seq(
        // For redirecting slf4j logs to airframe-log
        "org.slf4j"           % "slf4j-jdk14"         % "2.0.16",
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
    name                   := "wvlet-ui",
    description            := "UI components that can be testable with Node.js",
    Test / jsEnv           := new org.scalajs.jsenv.jsdomnodejs.JSDOMNodeJSEnv(),
    Test / requireJsDomEnv := true,
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
  .enablePlugins(ScalaJSPlugin, ScalablyTypedConverterExternalNpmPlugin)
  .in(file("wvlet-ui-main"))
  .settings(
    buildSettings,
    name                            := "wvlet-ui-main",
    description                     := "UI main code compiled with Vite.js",
    Test / jsEnv                    := new org.scalajs.jsenv.nodejs.NodeJSEnv(),
    scalaJSUseMainModuleInitializer := true,
    scalaJSLinkerConfig ~= {
      linkerConfig(_)
    },
    externalNpm := {
      scala
        .sys
        .process
        .Process(List("npm", "install", "--silent", "--no-audit", "--no-fund"), baseDirectory.value)
        .!
      baseDirectory.value
    }
  )
  .dependsOn(ui)

def linkerConfig(config: StandardConfig): StandardConfig = config
  // Check IR works properly since Scala.js 1.17.0 https://github.com/scala-js/scala-js/pull/4867
  .withCheckIR(true)
  .withSourceMap(true)
  .withModuleKind(ModuleKind.ESModule)
  .withModuleSplitStyle(ModuleSplitStyle.SmallModulesFor(List("wvlet.ui")))
