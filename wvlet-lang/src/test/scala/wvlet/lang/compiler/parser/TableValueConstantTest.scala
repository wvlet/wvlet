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

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.plan.{PackageDef, Query, ValDef}
import wvlet.lang.model.DataType

class TableValueConstantTest extends AirSpec:

  private def parse(input: String): PackageDef =
    val unit   = CompilationUnit.fromWvletString(input)
    val parser = WvletParser(unit)
    parser.parse().asInstanceOf[PackageDef]

  test("parse table value constant with column names") {
    val pkg = parse("""
      val t1(id, name) = [[1, "Alice"], [2, "Bob"]]
    """)

    val valDef = pkg.statements.head.asInstanceOf[ValDef]

    valDef.name.name shouldBe "t1"
    valDef.tableColumns shouldBe defined
    valDef.tableColumns.get.size shouldBe 2
    valDef.tableColumns.get(0).name.name shouldBe "id"
    valDef.tableColumns.get(1).name.name shouldBe "name"
  }

  test("parse table value constant with column types") {
    val pkg = parse("""
      val t2(id:int, name:string) = [[1, "Alice"], [2, "Bob"]]
    """)

    val valDef = pkg.statements.head.asInstanceOf[ValDef]

    valDef.name.name shouldBe "t2"
    valDef.tableColumns shouldBe defined
    valDef.tableColumns.get.size shouldBe 2
    valDef.tableColumns.get(0).name.name shouldBe "id"
    valDef.tableColumns.get(0).dataType shouldBe DataType.IntType
    valDef.tableColumns.get(1).name.name shouldBe "name"
    valDef.tableColumns.get(1).dataType shouldBe DataType.StringType
  }

  test("parse table value constant with trailing comma") {
    val pkg = parse("""
      val t3(id, name) = [
        [1, "Alice"],
        [2, "Bob"],
      ]
    """)

    val valDef = pkg.statements.head.asInstanceOf[ValDef]

    valDef.name.name shouldBe "t3"
    valDef.tableColumns shouldBe defined
    valDef.tableColumns.get.size shouldBe 2
  }

  test("parse table value constant with trailing comma in rows") {
    val pkg = parse("""
      val t4(id, name) = [
        [1, "Alice",],
        [2, "Bob",],
      ]
    """)

    val valDef = pkg.statements.head.asInstanceOf[ValDef]

    valDef.name.name shouldBe "t4"
    valDef.tableColumns shouldBe defined
    valDef.tableColumns.get.size shouldBe 2
  }

  test("use table value constant in query") {
    val pkg = parse("""
      val t1(id, name) = [[1, "Alice"], [2, "Bob"]]
      from t1
    """)

    pkg.statements.size shouldBe 2

    val valDef = pkg.statements(0).asInstanceOf[ValDef]
    valDef.name.name shouldBe "t1"
    valDef.tableColumns shouldBe defined

    val query = pkg.statements(1).asInstanceOf[Query]
    query shouldNotBe null
  }

  test("regular val definition should not have table columns") {
    val pkg = parse("""
      val x = 42
    """)

    val valDef = pkg.statements.head.asInstanceOf[ValDef]

    valDef.name.name shouldBe "x"
    valDef.tableColumns shouldBe None
  }

end TableValueConstantTest
