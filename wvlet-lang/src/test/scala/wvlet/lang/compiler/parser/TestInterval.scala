package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.parser.SqlParser

class TestInterval extends AirSpec:
  test("current INTERVAL behavior") {
    val tests = List(
      "SELECT INTERVAL '1' DAY",
      "SELECT INTERVAL '2' HOUR", 
      "SELECT INTERVAL '30' MINUTE",
      "SELECT INTERVAL + '1' DAY",
      "SELECT INTERVAL - '1' DAY",
      "SELECT INTERVAL '1' DAY TO HOUR"
    )
    
    tests.foreach { sql =>
      try
        val stmt = SqlParser(CompilationUnit.fromSqlString(sql)).parse()
        info(s"✓ PASS: $sql")
        info(s"    AST: ${stmt.pp}")
      catch
        case e: Exception =>
          info(s"✗ FAIL: $sql")
          info(s"    Error: ${e.getMessage}")
    }
  }