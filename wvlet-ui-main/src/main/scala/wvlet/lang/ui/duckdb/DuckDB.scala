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
import scala.language.dynamics

@js.native
@JSImport("@duckdb/duckdb-wasm", JSImport.Namespace)
object DuckDB extends js.Object:
// def getJsDelivrBundles(): js.Any = js.native
end DuckDB

object DuckDBUtil extends LogSupport:
  @JSExportTopLevel("duck")
  def duck(): Unit = info("Duck Duck")

//  def init(): Unit =
//    val bundles = DuckDB.getJSDelivrBundles()
//    val bundle  = DuckDB.selectBundle(bundles)
