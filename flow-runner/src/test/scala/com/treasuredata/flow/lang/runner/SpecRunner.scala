package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.FlowLangException
import com.treasuredata.flow.lang.compiler.{Compiler, CompilerOptions}
import com.treasuredata.flow.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.airspec.AirSpec

import java.io.File
import java.sql.SQLException

trait SpecRunner(
    specPath: String,
    ignoredSpec: Map[String, String] = Map.empty,
    prepareTPCH: Boolean = false
) extends AirSpec:
  private val duckDB          = QueryExecutor(DuckDBConnector(prepareTPCH = prepareTPCH))
  override def afterAll: Unit = duckDB.close()

  private val compiler = Compiler(
    CompilerOptions(sourceFolders = List(specPath), workingFolder = specPath)
  )

  // Compile all files in the source paths first
  for unit <- compiler.localCompilationUnits do
    test(unit.sourceFile.fileName) {
      ignoredSpec.get(unit.sourceFile.fileName).foreach(reason => ignore(reason))

      try
        val compileResult = compiler.compileSingleUnit(unit)
        val result        = duckDB.executeSingle(unit, compileResult.context)
        debug(result.toPrettyBox(maxWidth = Some(120)))
      catch
        case e: FlowLangException if e.statusCode.isUserError =>
          fail(e.getMessage)
    }

class BasicSpec
    extends SpecRunner(
      "spec/basic",
      ignoredSpec = Map("values.flow" -> "Need to support triple quotes")
    )

class Model1Spec extends SpecRunner("spec/model1")
class TPCHSpec   extends SpecRunner("spec/tpch", prepareTPCH = true)
class DuckDBSpec extends SpecRunner("spec/duckdb")
