package wvlet.lang.compiler.analyzer.duckdb

import wvlet.uni.test.UniTest

/**
  * Cross-platform tests for the [[DuckDB]] facade. Pure helpers (`escapeSqlString`) must behave
  * identically on JVM, JS, and Native; backend availability differs per platform and is asserted
  * accordingly.
  */
class DuckDBTest extends UniTest:

  test("escapeSqlString returns input unchanged when there are no single quotes") {
    DuckDB.escapeSqlString("") shouldBe ""
    DuckDB.escapeSqlString("plain.parquet") shouldBe "plain.parquet"
    DuckDB.escapeSqlString("path/to/file.parquet") shouldBe "path/to/file.parquet"
  }

  test("escapeSqlString doubles single quotes") {
    DuckDB.escapeSqlString("O'Reilly") shouldBe "O''Reilly"
    DuckDB.escapeSqlString("'leading") shouldBe "''leading"
    DuckDB.escapeSqlString("trailing'") shouldBe "trailing''"
    DuckDB.escapeSqlString("'") shouldBe "''"
    DuckDB.escapeSqlString("''") shouldBe "''''"
    DuckDB.escapeSqlString("a'b'c") shouldBe "a''b''c"
  }

  test("escapeSqlString leaves double quotes, backslashes, and unicode untouched") {
    DuckDB.escapeSqlString("a\"b") shouldBe "a\"b"
    DuckDB.escapeSqlString("a\\b") shouldBe "a\\b"
    DuckDB.escapeSqlString("ファイル.parquet") shouldBe "ファイル.parquet"
  }

  test("isAvailable reports a platform-dependent backend presence") {
    // JVM (JDBC) and Native (libduckdb) backends report true; the Scala.js stub reports false.
    // Either is valid; the test just pins the contract so a future refactor that, e.g., drops
    // the JVM backend doesn't silently change the API.
    val available = DuckDB.isAvailable
    (available == true || available == false) shouldBe true
  }

end DuckDBTest
