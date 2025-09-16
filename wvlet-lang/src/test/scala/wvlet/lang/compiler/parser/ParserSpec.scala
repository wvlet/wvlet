package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit

trait ParserSpec(specPath: String, ignoredSpec: Map[String, String] = Map.empty) extends AirSpec:
  for unit <- CompilationUnit.fromPath(specPath) do
    // Rename spec path's / (slash) to : to enable filtering by test names
    test(unit.sourceFile.relativeFilePath.replaceAll("/", ":")) {
      ignoredSpec.get(unit.sourceFile.fileName).foreach(reason => ignore(reason))
      val plan = ParserPhase.parseOnly(unit)
      debug(plan.pp)
    }
