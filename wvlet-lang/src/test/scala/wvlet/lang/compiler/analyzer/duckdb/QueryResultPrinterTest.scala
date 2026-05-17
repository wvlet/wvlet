package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.compiler.Name
import wvlet.lang.compiler.connector.QueryResult
import wvlet.lang.compiler.connector.QueryResultRow
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.uni.test.UniTest

/**
  * Cross-platform tests for [[QueryResultPrinter]]. Pure functions over a fixture —
  * platform-agnostic by design.
  */
class QueryResultPrinterTest extends UniTest:

  private def col(name: String, dt: DataType): NamedType = NamedType(Name.termName(name), dt)

  private val sample = QueryResult(
    columns = List(col("id", DataType.LongType), col("name", DataType.StringType)),
    rows = List(
      QueryResultRow(List(Some("1"), Some("alice"))),
      QueryResultRow(List(Some("2"), None)),
      QueryResultRow(List(Some("3"), Some("clark")))
    )
  )

  test("toCsv emits header + RFC-4180-style rows, NULL as empty field") {
    val csv = QueryResultPrinter.toCsv(sample)
    csv shouldBe """id,name
        |1,alice
        |2,
        |3,clark
        |""".stripMargin
  }

  test("toCsv quotes fields containing comma, quote, or newline") {
    val r = QueryResult(
      columns = List(col("text", DataType.StringType)),
      rows = List(
        QueryResultRow(List(Some("plain"))),
        QueryResultRow(List(Some("has,comma"))),
        QueryResultRow(List(Some("has \"quote\""))),
        QueryResultRow(List(Some("has\nnewline")))
      )
    )
    val csv      = QueryResultPrinter.toCsv(r)
    val expected =
      "text\n" + "plain\n" + "\"has,comma\"\n" + "\"has \"\"quote\"\"\"\n" + "\"has\nnewline\"\n"
    csv shouldBe expected
  }

  test("toBox emits a unicode-bordered table sized to its content") {
    val box = QueryResultPrinter.toBox(sample)
    // Sanity: 3 row borders (top, header-divider, bottom) + 1 header + 3 data rows = 7 lines
    box.split('\n').length shouldBe 7
    // Header and one data value should be present
    box shouldContain "id"
    box shouldContain "name"
    box shouldContain "alice"
    box shouldContain "clark"
    // NULL renders as the literal `null`
    box shouldContain "null"
    // Unicode box-drawing markers used
    box shouldContain "┌"
    box shouldContain "┘"
    box shouldContain "│"
  }

end QueryResultPrinterTest
