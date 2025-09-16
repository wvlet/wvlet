package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit

trait ParserSpec(specPath: String, ignoredSpec: Map[String, String] = Map.empty) extends AirSpec:
  for unit <- CompilationUnit.fromPath(specPath) do
    val specName = unit.sourceFile.relativeFilePath.replaceAll("/", ":")
    // Rename spec path's / (slash) to : to enable filtering by test names
    test(s"parse ${specName}") {
      // If the file matches to the ignoredSpec, ignore the test
      ignoredSpec.get(unit.sourceFile.fileName).foreach(reason => ignore(reason))

      val plan = ParserPhase.parseOnly(unit)
      debug(plan.pp)
    }

// Basic Wvlet queries
class ParserSpecBasic extends ParserSpec("spec/basic")
// TD CDP simple queries written in Wvlet
class ParserSpecCDPSimple extends ParserSpec("spec/cdp_simple")
// TD CDP behavior queries written in Wvlet
class ParserSpecCDPBehavior extends ParserSpec("spec/cdp_behavior")

// TPC-H queries translated into Wvlet
class ParserSpecTPCH extends ParserSpec("spec/tpch")
// Trino queries written in Wvlet
class ParserSpecTrino extends ParserSpec("spec/trino")
// TD-Trino specific queries
class ParserSpecTDTrino extends ParserSpec("spec/td-trino")

// Basic SQL queries
class ParserSpecSqlBasic extends ParserSpec("spec/sql/basic")
// TPC-H SQL
class ParserSpecSqlTPCH extends ParserSpec("spec/sql/tpc-h")
// TPC-DS SQL
class ParserSpecSqlTPCDS extends ParserSpec("spec/sql/tpc-ds")

// Trino SQL
class ParserSpecSqlTrino extends ParserSpec("spec/sql/trino")

// Hive SQL
class ParserSpecSqlHive
    extends ParserSpec(
      "spec/sql/hive",
      Map(
        "hive-data-types.sql" -> "Temporarily ignored - complex Hive data types not yet supported"
      )
    )

// Update SQL
class ParserSpecSqlUpdate extends ParserSpec("spec/sql/update")
