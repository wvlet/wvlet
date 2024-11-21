package wvlet.lang.ui.duckdb

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
  * Facade interface for acceessing duckdb.js
  */
@js.native
@JSGlobal("duckdb")
object DuckDBWasm extends js.Object:
  def hello(): Unit                          = js.native
  def query(sql: String): js.Promise[js.Any] = js.native
end DuckDBWasm
