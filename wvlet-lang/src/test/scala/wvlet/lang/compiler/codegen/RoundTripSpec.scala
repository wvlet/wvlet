package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.{CodeFormatterConfig, SqlGenerator, WvletGenerator}
import wvlet.lang.compiler.transform.RewriteExpr

/**
  * Given a spec file, this test performs the following tests:
  *   - parse a query to generate a LogicalPlan
  *   - print the query using the LogicalPlan
  *   - parse the printed query again
  */
abstract class RoundTripSpec(path: String, ignoredSpec: Map[String, String] = Map.empty)
    extends AirSpec:

  for unit <- CompilationUnit.fromPath(path) do
    val specName = unit.relativeFilePath.replaceAll("/", ":")
    test(s"Roundtrip ${specName}") {
      // If the file matches to the ignoredSpec, ignore the test
      ignoredSpec.get(unit.fileName).foreach(reason => ignore(reason))

      val unresolvedPlan = ParserPhase.parseOnly(unit)
      // Run basic expression rewrites
      val rewrittenPlan = RewriteExpr.rewriteOnly(unresolvedPlan)
      debug(rewrittenPlan.pp)

      val config = CodeFormatterConfig()
      val g      =
        if unit.isSQL then
          SqlGenerator(config)
        else
          WvletGenerator(config)
      val str = g.print(rewrittenPlan)
      debug(s"[formatted ${unit.fileName}]\n${str}")

      // Test parsing the generated Wvlet/SQL query
      {
        val reformattedUnit =
          if unit.isSQL then
            CompilationUnit.fromSqlString(str)
          else
            CompilationUnit.fromWvletString(str)
        val newPlan = ParserPhase.parseOnly(reformattedUnit)
        trace(newPlan.pp)
      }
    }

  end for

end RoundTripSpec

class RoundTripSpecBasic extends RoundTripSpec("spec/basic")
class RoundTripSpecTPCH  extends RoundTripSpec("spec/tpch")
//class WvletRoundTripSpecSqlBasic
//    extends RoundTripSpec(
//      "spec/sql/basic",
//      ignoredSpec = Map(
//        "decimal-literals.sql" -> "Need to decide how to support DECIMAL type in Wvlet",
//        "show-create-view.sql" -> "SHOW CREATE VIEW syntax not yet supported in Wvlet"
//      )
//    )

class RoundTripSpecSqlBasic
    extends RoundTripSpec(
      "spec/sql/basic",
      ignoredSpec = Map("row-map-types.sql" -> "Need to parse `type_name`[]")
    )

class RoundTripSpecSqlTPCH extends RoundTripSpec("spec/sql/tpc-h")
class RoundTripSpecSqlTPCD extends RoundTripSpec("spec/sql/tpc-ds")
