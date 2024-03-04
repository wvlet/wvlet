// Ignore binary incompatible errors for libraries using scala-xml.
// sbt-scoverage upgraded to scala-xml 2.1.0, but other sbt-plugins and Scala compilier 2.12 uses scala-xml 1.x.x
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml"                % "always",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "always"
)

// ThisBuild / resolvers ++= Resolver.sonatypeOssRepos("snapshots")
val AIRFRAME_VERSION = sys.env.getOrElse("AIRFRAME_VERSION", "24.3.0")

addSbtPlugin("org.scalameta"      % "sbt-scalafmt"  % "2.5.2")
addSbtPlugin("com.eed3si9n"       % "sbt-buildinfo" % "0.11.0")
addSbtPlugin("org.wvlet.airframe" % "sbt-airframe"  % AIRFRAME_VERSION)

// For generating Lexer/Parser from ANTLR4 grammar (.g4)
addSbtPlugin("com.simplytyped" % "sbt-antlr4" % "0.8.3")

// For IntelliJ IDEA
addSbtPlugin("org.jetbrains.scala" % "sbt-ide-settings" % "1.1.2")

// For restarting servers with reStart sbt command as you edit source files
addSbtPlugin("io.spray" % "sbt-revolver" % "0.10.0")

// For Scala.js
val SCALAJS_VERSION = sys.env.getOrElse("SCALAJS_VERSION", "1.15.0")
addSbtPlugin("org.scala-js"       % "sbt-scalajs"              % SCALAJS_VERSION)
addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.3.2")

// For creating Scala.js facade from JS moduels
addSbtPlugin("org.scalablytyped.converter" % "sbt-converter" % "1.0.0-beta44")

// For testing Scala.js code with Node.js
libraryDependencies += "org.scala-js" %% "scalajs-env-jsdom-nodejs" % "1.1.0"

addDependencyTreePlugin

// For setting explicit versions for each commit
addSbtPlugin("com.github.sbt" % "sbt-dynver" % "5.0.1")

// For packaging Scala project into a executable folder
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.19")

// For compiling model classes from SQL templates
libraryDependencies += "org.duckdb" % "duckdb_jdbc" % "0.10.0"
addSbtPlugin("org.xerial.sbt"       % "sbt-sql"     % "0.19")

scalacOptions ++= Seq("-deprecation", "-feature")
