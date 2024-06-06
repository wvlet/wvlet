package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.CompilationUnit
import wvlet.airspec.AirSpec

class ParserTest extends AirSpec:
  test("parse package") {
    val p = Parsers(CompilationUnit.fromString("""package mypackage.example
        |from A
        |""".stripMargin))
    val plan = p.parse()
    debug(plan)
  }
