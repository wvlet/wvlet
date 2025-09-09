package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.plan.*

trait SqlParserSpec(specPath: String, ignoredSpec: Map[String, String] = Map.empty) extends AirSpec:
  for unit <- CompilationUnit.fromPath(specPath) do
    // Rename spec path / to : for test name
    test(unit.sourceFile.relativeFilePath.replaceAll("/", ":")) {
      val parser = SqlParser(unit, isContextUnit = true)
      val stmt   = parser.parse()
      debug(stmt.pp)
    }

class SqlParserBasicSpec extends SqlParserSpec("spec/sql/basic")
class SqlParserTPCHSpec  extends SqlParserSpec("spec/sql/tpc-h")
class SqlParserTPCDSSpec extends SqlParserSpec("spec/sql/tpc-ds")
