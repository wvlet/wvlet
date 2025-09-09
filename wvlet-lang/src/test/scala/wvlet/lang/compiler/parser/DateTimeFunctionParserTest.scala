package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.expr.*

class DateTimeFunctionParserTest extends AirSpec:

  def parseExpression(sql: String): Expression =
    val unit   = CompilationUnit.fromSqlString(sql)
    val parser = SqlParser(unit)
    parser.expression()

  test("should parse date function calls correctly") {
    val expr = parseExpression("date('2025-08-17')")
    expr shouldMatch { case FunctionApply(UnquotedIdentifier("date", _), _, _, _) =>
    }
  }

  test("should parse time function calls correctly") {
    val expr = parseExpression("time('10:30:00')")
    expr shouldMatch { case FunctionApply(UnquotedIdentifier("time", _), _, _, _) =>
    }
  }

  test("should parse timestamp function calls correctly") {
    val expr = parseExpression("timestamp('2025-08-17 10:30:00')")
    expr shouldMatch { case FunctionApply(UnquotedIdentifier("timestamp", _), _, _, _) =>
    }
  }

  test("should parse DATE literals correctly") {
    val expr = parseExpression("DATE '2025-08-17'")
    expr shouldMatch { case _: GenericLiteral =>
    }
  }

  test("should parse TIME literals correctly") {
    val expr = parseExpression("TIME '10:30:00'")
    expr shouldMatch { case _: GenericLiteral =>
    }
  }

  test("should parse TIMESTAMP literals correctly") {
    val expr = parseExpression("TIMESTAMP '2025-08-17 10:30:00'")
    expr shouldMatch { case _: GenericLiteral =>
    }
  }

  test("should handle the original problematic pattern") {
    // This should parse without throwing an exception
    val expr = parseExpression("date(cast(1755446400 as varchar))")
    expr shouldMatch { case FunctionApply(UnquotedIdentifier("date", _), _, _, _) =>
    }
  }

end DateTimeFunctionParserTest
