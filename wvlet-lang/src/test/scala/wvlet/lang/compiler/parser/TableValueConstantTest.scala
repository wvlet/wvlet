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
    valDef.dataType shouldMatch { case schemaType: DataType.SchemaType =>
      schemaType.columnTypes.size shouldBe 2
      schemaType.columnTypes(0).name.name shouldBe "id"
      schemaType.columnTypes(1).name.name shouldBe "name"
    }
  }

  test("parse table value constant with column types") {
    val pkg = parse("""
      val t2(id:int, name:string) = [[1, "Alice"], [2, "Bob"]]
    """)

    val valDef = pkg.statements.head.asInstanceOf[ValDef]

    valDef.name.name shouldBe "t2"
    valDef.dataType shouldMatch { case schemaType: DataType.SchemaType =>
      schemaType.columnTypes.size shouldBe 2
      schemaType.columnTypes(0).name.name shouldBe "id"
      schemaType.columnTypes(0).dataType shouldBe DataType.IntType
      schemaType.columnTypes(1).name.name shouldBe "name"
      schemaType.columnTypes(1).dataType shouldBe DataType.StringType
    }
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
    valDef.dataType shouldMatch { case schemaType: DataType.SchemaType =>
      schemaType.columnTypes.size shouldBe 2
    }
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
    valDef.dataType shouldMatch { case schemaType: DataType.SchemaType =>
      schemaType.columnTypes.size shouldBe 2
    }
  }

  test("use table value constant in query") {
    val pkg = parse("""
      val t1(id, name) = [[1, "Alice"], [2, "Bob"]]
      from t1
    """)

    pkg.statements.size shouldBe 2

    val valDef = pkg.statements(0).asInstanceOf[ValDef]
    valDef.name.name shouldBe "t1"
    valDef.dataType shouldMatch { case _: DataType.SchemaType =>
    }

    val query = pkg.statements(1).asInstanceOf[Query]
    query shouldNotBe null
  }

  test("regular val definition should not have table columns") {
    val pkg = parse("""
      val x = 42
    """)

    val valDef = pkg.statements.head.asInstanceOf[ValDef]

    valDef.name.name shouldBe "x"
    valDef.dataType match
      case _: DataType.SchemaType =>
        fail("Regular val should not be SchemaType")
      case _ => // ok
  }

  test("parse empty table value constant") {
    val pkg = parse("""
      val empty_table(id, name) = []
    """)

    val valDef = pkg.statements.head.asInstanceOf[ValDef]

    valDef.name.name shouldBe "empty_table"
    valDef.dataType shouldMatch { case schemaType: DataType.SchemaType =>
      schemaType.columnTypes.size shouldBe 2
    }
  }

  test("parse table value constant with single row") {
    val pkg = parse("""
      val single_row(x, y, z) = [[1, 2, 3]]
    """)

    val valDef = pkg.statements.head.asInstanceOf[ValDef]

    valDef.name.name shouldBe "single_row"
    valDef.dataType shouldMatch { case schemaType: DataType.SchemaType =>
      schemaType.columnTypes.size shouldBe 3
    }
  }

end TableValueConstantTest
