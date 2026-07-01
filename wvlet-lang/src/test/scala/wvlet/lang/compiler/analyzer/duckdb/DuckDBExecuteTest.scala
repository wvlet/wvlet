package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.model.DataType
import wvlet.uni.test.UniTest

/**
  * Cross-platform tests for `DuckDB.execute(sql)` — the row-iteration counterpart to `schemaOf`.
  * Same DuckDB engine on all three platforms (JDBC on JVM, koffi-FFI on JS, `@extern @link` on
  * Native), so the same SQL should produce identical results.
  *
  * Auto-skips on Scala.js if libduckdb isn't loadable (e.g. browser, or Node without the shared lib
  * installed). `DuckDB.isAvailable` gates the run.
  */
class DuckDBExecuteTest extends UniTest:

  private def skipIfUnavailable(): Unit =
    if !DuckDB.canExecute then
      ignore("DuckDB.execute not yet wired on this platform")

  test("execute returns a single-row scalar result") {
    skipIfUnavailable()
    val r = DuckDB.execute("select 1 + 1 as two")
    r.columnCount shouldBe 1
    r.rowCount shouldBe 1
    r.columns.head.name.name shouldBe "two"
    r.rows.head.values.head shouldBe Some("2")
  }

  test("execute returns multiple typed columns") {
    skipIfUnavailable()
    val r = DuckDB.execute(
      "select 42::bigint as n, 'alice' as name, 3.14::double as pi, true as flag"
    )
    r.columnCount shouldBe 4
    r.rowCount shouldBe 1
    r.columns.map(_.name.name) shouldBe List("n", "name", "pi", "flag")
    r.columns.map(_.dataType) shouldBe
      List(DataType.LongType, DataType.StringType, DataType.DoubleType, DataType.BooleanType)
    r.rows.head.values shouldBe List(Some("42"), Some("alice"), Some("3.14"), Some("true"))
  }

  test("execute returns multiple rows in order") {
    skipIfUnavailable()
    val r = DuckDB.execute(
      "select * from (values (1, 'a'), (2, 'b'), (3, 'c')) t(id, label) order by id"
    )
    r.rowCount shouldBe 3
    r.rows.map(_.values.head) shouldBe List(Some("1"), Some("2"), Some("3"))
    r.rows.map(_.values(1)) shouldBe List(Some("a"), Some("b"), Some("c"))
  }

  test("execute surfaces SQL NULLs as None") {
    skipIfUnavailable()
    val r = DuckDB.execute("select NULL::varchar as x, 'present' as y")
    r.rows.head.values shouldBe List(None, Some("present"))
  }

  test("execute reports copy ... to statements as a Count row") {
    skipIfUnavailable()
    // COPY produces no regular ResultSet; every platform must surface DuckDB's `Count`
    // result (JDBC update count on JVM, C-API result on JS/Native) with the same shape.
    val r = DuckDB.execute(
      "copy (select * from (values (1), (2), (3)) t(id)) to 'target/duckdb-execute-copy-test.parquet'"
    )
    r.columnCount shouldBe 1
    r.columns.head.name.name shouldBe "Count"
    r.rowCount shouldBe 1
    r.rows.head.values.head shouldBe Some("3")
  }

  test("execute compiles a query against a parquet fixture") {
    skipIfUnavailable()
    val r = DuckDB.execute(
      "select count(*) as n from 'spec/cdp_behavior/data/weblog_users_first_purchased_at/part-00000-90048009-4be7-472e-ad1a-019aed6bd6fa-c000.snappy.parquet'"
    )
    r.rowCount shouldBe 1
    // Just assert we got a numeric count back; the fixture row count itself may evolve.
    r.rows.head.values.head.exists(_.toLongOption.isDefined) shouldBe true
  }

end DuckDBExecuteTest
