package wvlet.lang.runner

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, DBType, WorkEnv}
import wvlet.lang.compiler.parser.{SqlParser, WvletParser}
import wvlet.lang.compiler.codegen.{CodeFormatterConfig, SqlGenerator}

/**
  * Test parsing and SQL generation for Hive without execution
  */
class HiveParseSpec extends AirSpec:

  private val workEnv = WorkEnv(path = "spec/sql/hive-parseable")

  test("parse and generate Hive SQL from Wvlet queries") {
    CompilationUnit
      .fromPath("spec/sql/hive-parseable/wvlet-to-hive.wv")
      .foreach { unit =>
        test(s"parse ${unit.sourceFile.fileName}") {
          debug(s"Source:\n${unit.sourceFile.getContentAsString}")

          // Parse as Wvlet
          val plan = WvletParser(unit).parse()
          debug(s"Parsed plan: ${plan.pp}")

          // Generate Hive SQL (without transformations)
          val hiveGen = SqlGenerator(CodeFormatterConfig(sqlDBType = DBType.Hive))
          val hiveSql = hiveGen.print(plan)
          debug(s"Generated Hive SQL:\n${hiveSql}")

          // Check for Hive-specific syntax
          // Note: Function transformations require full compilation pipeline
          hiveSql shouldContain "ARRAY["                  // Array syntax
          hiveSql shouldContain "{name: 'John', age: 30}" // Struct syntax
        }
      }
  }

  test("parse SQL queries") {
    val sqlFiles = Seq("basic-queries.sql", "values-syntax.sql")

    sqlFiles.foreach { fileName =>
      CompilationUnit
        .fromPath(s"spec/sql/hive-parseable/${fileName}")
        .foreach { unit =>
          test(s"parse ${fileName}") {
            debug(s"Source:\n${unit.sourceFile.getContentAsString}")

            // Parse as SQL
            val plan = SqlParser(unit).parse()
            debug(s"Parsed plan: ${plan.pp}")

            // Generate Hive SQL
            val hiveGen = SqlGenerator(CodeFormatterConfig(sqlDBType = DBType.Hive))
            val hiveSql = hiveGen.print(plan)
            debug(s"Generated Hive SQL:\n${hiveSql}")

            // Verify VALUES syntax for Hive
            if fileName == "values-syntax.sql" then
              hiveSql shouldContain "values"
              hiveSql shouldNotContain "(values"
          }
        }
    }
  }

end HiveParseSpec
