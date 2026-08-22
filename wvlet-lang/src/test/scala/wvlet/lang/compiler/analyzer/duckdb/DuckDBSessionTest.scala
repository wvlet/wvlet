package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.uni.test.UniTest

/**
  * Cross-platform tests for [[DuckDB.newSession]] — the persistent-connection counterpart to the
  * one-shot `DuckDB.execute`. The same session semantics must hold on all three platforms (JDBC on
  * JVM, koffi-FFI on JS, C API on Native): state created by one `execute` is visible to the next.
  *
  * Auto-skips on Scala.js if libduckdb isn't loadable. `DuckDB.canExecute` gates the run.
  */
class DuckDBSessionTest extends UniTest:

  private def skipIfUnavailable(): Unit =
    if !DuckDB.canExecute then
      ignore("DuckDB.execute not yet wired on this platform")

  test("session preserves tables across execute calls") {
    skipIfUnavailable()
    val session = DuckDB.newSession()
    try
      session.execute("create table t(id integer, msg varchar)")
      session.execute("insert into t values (1, 'hello'), (2, 'session')")
      val r = session.execute("select * from t order by id")
      r.rowCount shouldBe 2
      r.rows.map(_.values) shouldBe
        List(List(Some("1"), Some("hello")), List(Some("2"), Some("session")))
    finally
      session.close()
  }

  test("separate sessions are isolated") {
    skipIfUnavailable()
    val s1 = DuckDB.newSession()
    val s2 = DuckDB.newSession()
    try
      s1.execute("create table only_in_s1(id integer)")
      val err = intercept[Exception] {
        s2.execute("select * from only_in_s1")
      }
      err.getMessage shouldContain "only_in_s1"
    finally
      s1.close()
      s2.close()
  }

  test("session rejects execute after close") {
    skipIfUnavailable()
    val session = DuckDB.newSession()
    session.execute("select 1")
    session.close()
    // close() must be idempotent
    session.close()
    intercept[IllegalStateException] {
      session.execute("select 1")
    }
  }

  test("file-backed session persists data across sessions") {
    skipIfUnavailable()
    val path = s"target/duckdb-session-test-${System.currentTimeMillis()}.db"
    val s1   = DuckDB.newSession(Some(path))
    try
      s1.execute("create table persisted(id integer)")
      s1.execute("insert into persisted values (42)")
    finally
      s1.close()
    val s2 = DuckDB.newSession(Some(path))
    try
      val r = s2.execute("select id from persisted")
      r.rows.map(_.values.head) shouldBe List(Some("42"))
    finally
      s2.close()
  }

  test("session-backed DuckDBSqlConnector keeps state across submits") {
    skipIfUnavailable()
    given QueryProgressMonitor = QueryProgressMonitor.noOp
    val connector              = DuckDBSqlConnector.withNewSession()
    try
      connector.execute("create table c(id integer)")
      connector.execute("insert into c values (7)")
      val r = connector.execute("select id from c")
      r.rows.map(_.values.head) shouldBe List(Some("7"))
    finally
      connector.close()
  }

  test("sessionless DuckDBSqlConnector stays one-shot") {
    skipIfUnavailable()
    given QueryProgressMonitor = QueryProgressMonitor.noOp
    val connector              = DuckDBSqlConnector()
    try
      connector.execute("create table gone(id integer)")
      val err = intercept[Exception] {
        connector.execute("select * from gone")
      }
      err.getMessage shouldContain "gone"
    finally
      connector.close()
  }

end DuckDBSessionTest
