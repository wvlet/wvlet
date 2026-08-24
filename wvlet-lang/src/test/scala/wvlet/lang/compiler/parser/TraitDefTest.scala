/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.compiler.parser

import wvlet.uni.test.UniTest
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.plan.LogicalPlan
import wvlet.lang.model.plan.TypeDef

/**
  * `trait` declarations (#2001): a method interface attached to a type. Traits share the TypeDef
  * machinery (isTrait = true) but never describe storage — column fields and `<catalog>.<schema>`
  * bindings are rejected at parse time
  */
class TraitDefTest extends UniTest:

  private def parse(wvlet: String): LogicalPlan = ParserPhase.parseOnly(
    CompilationUnit.fromWvletString(wvlet)
  )

  private def firstTypeDef(plan: LogicalPlan): TypeDef =
    var found: Option[TypeDef] = None
    plan.traverse { case t: TypeDef =>
      if found.isEmpty then
        found = Some(t)
    }
    found.get

  test("parse a trait with def members, a dialect scope, and a parent type") {
    val t = firstTypeDef(parse("""trait ip_address in duckdb extends string = {
        |  def country_name: string = sql"'N/A'"
        |}
        |""".stripMargin))
    t.isTrait shouldBe true
    t.isTableDef shouldBe false
    t.name.name shouldBe "ip_address"
    t.defContexts.map(_.contextType.fullName) shouldBe List("duckdb")
    t.parents.map(_.fullName) shouldBe List("string")
  }

  test("parse a trait with type parameters") {
    val t = firstTypeDef(parse("""trait array[A] = {
        |  def size: int = sql"array_length(${this})"
        |}
        |""".stripMargin))
    t.isTrait shouldBe true
    t.params.size shouldBe 1
  }

  test("parse a comma-separated mixin parent list (#2012)") {
    val t = firstTypeDef(parse("""trait auditable extends recent, labeled = {
        |  def tag: string = sql"'a'"
        |}
        |""".stripMargin))
    t.isTrait shouldBe true
    t.parents.map(_.fullName) shouldBe List("recent", "labeled")
  }

  test("parse mixin parents on a table declaration (#2012)") {
    val t = firstTypeDef(parse("""table events extends timestamped, auditable = {
        |  id: int
        |}
        |""".stripMargin))
    t.isTableDef shouldBe true
    t.parents.map(_.fullName) shouldBe List("timestamped", "auditable")
  }

  test("reject combining extends with like on a table declaration") {
    val e = intercept[WvletLangException] {
      parse("""table events extends timestamped like users
          |""".stripMargin)
    }
    e.getMessage shouldContain "cannot combine 'extends' with 'like'"
  }

  test("reject column fields in a trait body") {
    val e = intercept[WvletLangException] {
      parse("""trait users = {
          |  id: int
          |}
          |""".stripMargin)
    }
    e.getMessage shouldContain "cannot declare columns"
    e.getMessage shouldContain "table users"
  }

  test("reject a catalog.schema binding on a trait") {
    val e = intercept[WvletLangException] {
      parse("""trait orders in mydb.sales = {
          |  def is_open: boolean = sql"true"
          |}
          |""".stripMargin)
    }
    e.getMessage shouldContain "cannot bind to a table location"
    e.getMessage shouldContain "table orders in mydb.sales"
  }

end TraitDefTest
