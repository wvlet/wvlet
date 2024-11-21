package wvlet.lang.ui.duckdb

import scala.concurrent.Future
import scala.scalajs.js
import scala.scalajs.js.annotation.{JSGlobal, JSGlobalScope, JSImport}

@js.native
@JSImport("@duckdb/duckdb-wasm", JSImport.Namespace)
object DuckDB extends js.Object:
  def getJsDelivrBundles(): js.Any = js.native

@js.native
@JSGlobal("DuckDBUtil")
object DuckDBUtil extends js.Object:
  def hello(): String = js.native
