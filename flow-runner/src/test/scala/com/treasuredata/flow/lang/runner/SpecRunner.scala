package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.FlowLangException
import com.treasuredata.flow.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.airspec.AirSpec

import java.io.File
import java.sql.SQLException

trait SpecRunner(specPath: String, prepareTPCH: Boolean = false) extends AirSpec:
  private val duckDB          = QueryExecutor(DuckDBConnector(prepareTPCH = prepareTPCH))
  override def afterAll: Unit = duckDB.close()
  private val specFiles       = new File(specPath).listFiles().filter(_.getName.endsWith(".flow"))

  for file <- specFiles do
    test(file.getName) {
      try
        val result = duckDB.execute(specPath, file.getName)
        debug(result)
      catch
        case e: FlowLangException if e.statusCode.isUserError =>
          fail(e.getMessage)
    }

class BasicSpec  extends SpecRunner("spec/basic")
class Model1Spec extends SpecRunner("spec/model1")
class TPCHSpec   extends SpecRunner("spec/tpch", prepareTPCH = true)
class DuckDBSpec extends SpecRunner("spec/duckdb")
