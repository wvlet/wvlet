// Ignore binary incompatible errors for libraries using scala-xml.
// sbt-scoverage upgraded to scala-xml 2.1.0, but other sbt-plugins and Scala compilier 2.12 uses scala-xml 1.x.x
ThisBuild / libraryDependencySchemes ++=
  Seq(
    "org.scala-lang.modules" %% "scala-xml"                % "always",
    "org.scala-lang.modules" %% "scala-parser-combinators" % "always"
  )

// ThisBuild / resolvers ++= Resolver.sonatypeOssRepos("snapshots")
val AIRFRAME_VERSION = sys.env.getOrElse("AIRFRAME_VERSION", "2025.1.12")

addSbtPlugin("org.scalameta"      % "sbt-scalafmt"  % "2.5.4")
addSbtPlugin("com.eed3si9n"       % "sbt-buildinfo" % "0.13.1")
addSbtPlugin("org.wvlet.airframe" % "sbt-airframe"  % AIRFRAME_VERSION)

// For IntelliJ IDEA
addSbtPlugin("org.jetbrains.scala" % "sbt-ide-settings" % "1.1.2")

// For restarting servers with reStart sbt command as you edit source files
addSbtPlugin("io.spray" % "sbt-revolver" % "0.10.0")

// For Scala.js
val SCALAJS_VERSION                    = sys.env.getOrElse("SCALAJS_VERSION", "1.19.0")
addSbtPlugin("org.scala-js"       % "sbt-scalajs"              % SCALAJS_VERSION)
addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.3.2")

// For Scala Native
addSbtPlugin("org.scala-native"   % "sbt-scala-native"              % "0.5.7")
addSbtPlugin("org.portable-scala" % "sbt-scala-native-crossproject" % "1.3.2")

// For testing Scala.js code with Node.js
libraryDependencies += "org.scala-js" %% "scalajs-env-jsdom-nodejs" % "1.1.0"

addDependencyTreePlugin

// For setting explicit versions for each commit
addSbtPlugin("com.github.sbt" % "sbt-dynver" % "5.1.0")

// For packaging Scala project into a executable folder
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.20")

// For compiling model classes from SQL templates
libraryDependencies += "org.duckdb" % "duckdb_jdbc" % "1.2.2.0"
addSbtPlugin("org.xerial.sbt" % "sbt-sql" % "0.19")

scalacOptions ++= Seq("-deprecation", "-feature")
