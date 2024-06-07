package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.CompilationUnit
import com.treasuredata.flow.lang.model.plan.{PackageDef, TableRef, Query}
import wvlet.airspec.AirSpec

class ParserTest extends AirSpec:
  test("parse package") {
    val p = Parsers(CompilationUnit.fromString("""package org.app.example
        |from A
        |""".stripMargin))
    val plan = p.parse()
    debug(plan)
    plan shouldMatch { case p: PackageDef =>
      p.statements shouldMatch { case List(Query(t: TableRef, _)) =>
        t.name.fullName shouldBe "A"
      }
    }
  }
