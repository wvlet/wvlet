package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.parser.SqlParser
import wvlet.lang.model.expr.*

class IntervalLiteralParserTest extends AirSpec:
  
  test("parse basic INTERVAL literals") {
    val tests = List(
      ("SELECT INTERVAL '1' DAY", "day"),
      ("SELECT INTERVAL '2' HOUR", "hour"), 
      ("SELECT INTERVAL '30' MINUTE", "minute"),
      ("SELECT INTERVAL '45' SECOND", "second"),
      ("SELECT INTERVAL '1' YEAR", "year"),
      ("SELECT INTERVAL '6' MONTH", "month")
    )
    
    tests.foreach { case (sql, expectedField) =>
      val stmt = SqlParser(CompilationUnit.fromSqlString(sql)).parse()
      debug(s"Parsed: $sql")
      debug(s"AST: ${stmt.pp}")
      
      // Verify the AST contains an IntervalLiteral
      var found = false
      stmt.traverseExpressions {
        case interval: IntervalLiteral =>
          found = true
          interval.startField.name shouldBe expectedField
          interval.sign shouldBe Sign.NoSign
          interval.end shouldBe None
      }
      found shouldBe true
    }
  }

  test("parse signed INTERVAL literals") {
    val tests = List(
      ("SELECT INTERVAL + '1' DAY", Sign.Positive),
      ("SELECT INTERVAL - '1' DAY", Sign.Negative)
    )
    
    tests.foreach { case (sql, expectedSign) =>
      val stmt = SqlParser(CompilationUnit.fromSqlString(sql)).parse()
      debug(s"Parsed: $sql")
      
      var found = false
      stmt.traverseExpressions {
        case interval: IntervalLiteral =>
          found = true
          interval.sign shouldBe expectedSign
          interval.startField.name shouldBe "day"
      }
      found shouldBe true
    }
  }

  test("parse range INTERVAL literals") {
    val tests = List(
      ("SELECT INTERVAL '1' DAY TO HOUR", "day", "hour"),
      ("SELECT INTERVAL '1' HOUR TO MINUTE", "hour", "minute"),
      ("SELECT INTERVAL '1' MINUTE TO SECOND", "minute", "second"),
      ("SELECT INTERVAL '1' YEAR TO MONTH", "year", "month")
    )
    
    tests.foreach { case (sql, startField, endField) =>
      val stmt = SqlParser(CompilationUnit.fromSqlString(sql)).parse()
      debug(s"Parsed: $sql")
      
      var found = false
      stmt.traverseExpressions {
        case interval: IntervalLiteral =>
          found = true
          interval.startField.name shouldBe startField
          interval.end shouldBe defined
          interval.end.get.name shouldBe endField
      }
      found shouldBe true
    }
  }

  test("case-insensitive interval fields") {
    val tests = List(
      "SELECT INTERVAL '1' day",     // lowercase
      "SELECT INTERVAL '1' DAY",     // uppercase  
      "SELECT INTERVAL '1' Day",     // mixed case
      "SELECT INTERVAL '1' DaY"      // mixed case
    )
    
    tests.foreach { sql =>
      val stmt = SqlParser(CompilationUnit.fromSqlString(sql)).parse()
      debug(s"Parsed: $sql")
      
      var found = false
      stmt.traverseExpressions {
        case interval: IntervalLiteral =>
          found = true
          interval.startField.name shouldBe "day"
      }
      found shouldBe true
    }
  }

  test("complex interval value expressions") {
    val tests = List(
      "SELECT INTERVAL '1 2:03:04' DAY",   // day-time format
      "SELECT INTERVAL '123' SECOND",       // numeric string
      "SELECT INTERVAL '1.5' HOUR"         // decimal string
    )
    
    tests.foreach { sql =>
      val stmt = SqlParser(CompilationUnit.fromSqlString(sql)).parse()
      debug(s"Parsed: $sql")
      
      var found = false
      stmt.traverseExpressions {
        case interval: IntervalLiteral =>
          found = true
          // Just verify it parses correctly
      }
      found shouldBe true
    }
  }

  test("interval literal SQL generation") {
    val tests = List(
      ("SELECT INTERVAL '1' DAY", "interval '1' day"),
      ("SELECT INTERVAL + '2' HOUR", "interval + '2' hour"),
      ("SELECT INTERVAL - '30' MINUTE", "interval - '30' minute"),
      ("SELECT INTERVAL '1' DAY TO HOUR", "interval  between '1' day and hour")
    )
    
    tests.foreach { case (sql, expectedSql) =>
      val stmt = SqlParser(CompilationUnit.fromSqlString(sql)).parse()
      debug(s"Parsed: $sql")
      
      var found = false
      stmt.traverseExpressions {
        case interval: IntervalLiteral =>
          found = true
          interval.sqlExpr shouldBe expectedSql
      }
      found shouldBe true
    }
  }