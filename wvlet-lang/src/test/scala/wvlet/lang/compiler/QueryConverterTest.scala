// In: wvlet-lang/src/test/scala/wvlet/lang/compiler/QueryConverterTest.scala
package wvlet.lang.compiler

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.codegen.GenSQL
// import wvlet.lang.compiler.codegen.GenWvlet // Not yet implemented
// import wvlet.lang.parser.SQLParser // Not yet implemented
import wvlet.log.LogLevel
import wvlet.log.io.IOUtil

/**
  * TPC-H queries for testing wvlet <=> SQL translation
  */
class QueryConverterTest extends AirSpec {

  // Create a new WorkEnv instance for the test environment
  private val workEnv = new WorkEnv(
    path = "target/query_converter_test",
    logLevel = LogLevel.INFO
  )
  private val compiler = Compiler(CompilerOptions(dbType = DBType.Trino, workEnv = workEnv))

  // Path to TPC-H queries (adjust to your project structure)
  private val tpchWvletQueryPath = "spec/sql/tpc-h"
  private val tpchSQLQueryPath   = "spec/tpch"

  // Test TPC-H queries from 1 to 22
  for (i <- 1 to 22) {
    val queryName = f"q${i}%02d"
    test(s"translate ${queryName}: wvlet -> SQL") {
      val wvletQuery = IOUtil.readAsString(s"${tpchWvletQueryPath}/${queryName}.wv")
      val sqlQuery   = IOUtil.readAsString(s"${tpchSQLQueryPath}/${queryName}.sql")

      // Create a CompilationUnit from the wvlet query string
      val unit = CompilationUnit.fromWvletString(wvletQuery)
      // Use compileSingleUnit to compile a given CompilationUnit
      val result = compiler.compileSingleUnit(unit)

      // LogicalPlan -> SQL
      // Pass the unit directly to GenSQL.generateSQL (similar to WvletJS.scala)
      val generatedSQL = GenSQL.generateSQL(unit)(using result.context)

      info(s"=== Query: ${queryName} ===")
      info(s"Expected SQL:\n${sqlQuery.trim}")
      info(s"Generated SQL:\n${generatedSQL.trim}")

      // Compare the generated SQL with the expected SQL (normalization would be better to absorb formatting differences)
      // Simple comparison here
      generatedSQL.trim shouldBe sqlQuery.trim
    }

    // Test for SQL -> wvlet (enable after implementing SQLParser and GenWvlet)
    /*
    test(s"translate ${queryName}: SQL -> wvlet") {
      val wvletQuery = IOUtil.readAsString(s"${tpchWvletQueryPath}/${queryName}.wv")
      val sqlQuery   = IOUtil.readAsString(s"${tpchSQLQueryPath}/${queryName}.sql")

      // TODO: Implement SQLParser
      // SQL -> LogicalPlan
      val plan = SQLParser.parse(sqlQuery, compiler.defaultContext)

      //　TODO: Implement GenWvlet
      // LogicalPlan -> wvlet
      val generatedWvlet = GenWvlet.generateWvlet(plan)

      // Compare the generated wvlet with the original wvlet
      generatedWvlet.trim shouldBe wvletQuery.trim
    }
    */
  }
}