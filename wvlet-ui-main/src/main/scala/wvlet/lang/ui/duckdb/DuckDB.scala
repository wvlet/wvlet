package wvlet.lang.ui.duckdb

import scala.concurrent.Future
import scala.scalajs.js
import scala.scalajs.js.annotation.{JSGlobalScope, JSImport}

@js.native
@JSImport("@duckdb/duckdb-wasm", JSImport.Namespace)
object DuckDB extends js.Object:
  def getJsDelivrBundles(): js.Any = js.native
