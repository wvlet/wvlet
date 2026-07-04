// ThisBuild / resolvers ++= Resolver.sonatypeOssRepos("snapshots")

val UNI_VERSION = "2026.1.15"

addSbtPlugin("org.scalameta" % "sbt-scalafmt"  % "2.6.1")
addSbtPlugin("com.eed3si9n"  % "sbt-buildinfo" % "0.13.1")

// For IntelliJ IDEA
addSbtPlugin("org.jetbrains.scala" % "sbt-ide-settings" % "1.1.4")

// For restarting servers with uniRestart sbt command as you edit source files
// (replaces sbt-revolver; not yet available for sbt 2)
addSbtPlugin("org.wvlet.uni" % "sbt-uni" % UNI_VERSION)

// For Scala.js
val SCALAJS_VERSION = sys.env.getOrElse("SCALAJS_VERSION", "1.22.0")
addSbtPlugin("org.scala-js" % "sbt-scalajs" % SCALAJS_VERSION)

// For Scala Native
addSbtPlugin("org.scala-native" % "sbt-scala-native" % "0.5.12")

// Cross-platform (JVM/JS/Native) crossProject support
// (replaces org.portable-scala sbt-scalajs-crossproject & sbt-scala-native-crossproject
// which have no sbt 2 build)
addSbtPlugin("org.wvlet.uni" % "sbt-uni-crossproject" % UNI_VERSION)

// Headless Chromium JSEnv for wvlet-ui tests (replaces scalajs-env-jsdom-nodejs, which is
// only published for Scala 2.13 and conflicts with sbt 2's Scala 3 meta-build).
addSbtPlugin("org.wvlet.uni" % "sbt-uni-playwright" % UNI_VERSION)

addDependencyTreePlugin

// For setting explicit versions for each commit
addSbtPlugin("com.github.sbt" % "sbt-dynver" % "5.1.1")

// For packaging Scala project into a executable folder
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "1.0.0")

scalacOptions ++= Seq("-deprecation", "-feature")
