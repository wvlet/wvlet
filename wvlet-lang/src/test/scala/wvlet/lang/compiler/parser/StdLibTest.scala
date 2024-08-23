package wvlet.lang.compiler.parser

import wvlet.lang.compiler.CompilationUnit
import wvlet.airspec.AirSpec

class StdLibTest extends AirSpec:
  test("parse stdlib") {
    val units = CompilationUnit.fromPath("wvlet-stdlib/module/standard")
    units.foreach { u =>
      test(s"Parse ${u.sourceFile.fileName}") {
        val plan = WvletParser(u).parse()
        debug(plan.pp)
      }
    }
  }
