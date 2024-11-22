package wvlet.lang.ui.playground

import wvlet.airframe.rx.Rx
import wvlet.lang.api.{StatusCode, WvletLangException}
import wvlet.log.LogSupport

import scala.concurrent.Future
import scala.scalajs.js
import scala.scalajs.js.annotation.{
  JSBracketCall,
  JSExport,
  JSExportTopLevel,
  JSGlobal,
  JSGlobalScope,
  JSImport
}

/**
  * A facade for accessing DuckDB.ts
  */
@js.native
@JSGlobal("duckdb")
class DuckDBWasm() extends js.Object:
  def query(sql: String): js.Promise[String] = js.native
  def close(): Unit                          = js.native
end DuckDBWasm

class DuckDB() extends AutoCloseable with LogSupport:

  private val db = new DuckDBWasm()

  def query(sql: String): Rx[String] =
    import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits.*
    val p = scala.concurrent.Promise[String]()
    db.query(sql)
      .`then` { result =>
        p.success(result)
      }
      .`catch` { e =>
        p.failure(StatusCode.QUERY_EXECUTION_FAILURE.newException(s"Query failed:\n${e}"))
      }
    Rx.future(p.future)

  override def close(): Unit = db.close()

end DuckDB
