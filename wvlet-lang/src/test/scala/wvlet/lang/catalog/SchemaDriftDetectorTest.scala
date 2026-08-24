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
package wvlet.lang.catalog

import wvlet.uni.test.UniTest
import wvlet.lang.catalog.Catalog.TableColumn
import wvlet.lang.compiler.DBType
import wvlet.lang.model.DataType

class SchemaDriftDetectorTest extends UniTest:

  private def declared(cols: (String, String)*): Seq[(String, DataType)] = cols.map {
    case (name, tpe) =>
      name -> DataType.parse(tpe)
  }

  private def actual(cols: (String, String)*): Seq[TableColumn] = cols.map { case (name, tpe) =>
    TableColumn(name, DataType.parse(tpe))
  }

  private def diff(d: Seq[(String, DataType)], a: Seq[TableColumn]) = SchemaDriftDetector.diff(
    "users",
    d,
    a,
    DBType.DuckDB
  )

  test("report no drift when the declaration matches the catalog") {
    val drift = diff(
      declared("id" -> "int", "name"    -> "string"),
      actual("id"   -> "bigint", "name" -> "varchar")
    )
    drift.hasDrift shouldBe false
  }

  test("normalize engine-widened types when comparing") {
    // Writes materialize int as bigint and string as varchar; declared any matches any type
    val drift = diff(
      declared("a" -> "long", "b"   -> "string", "c"  -> "integer[]", "d" -> "any"),
      actual("a"   -> "bigint", "b" -> "varchar", "c" -> "bigint[]", "d"  -> "json")
    )
    drift.hasDrift shouldBe false
  }

  test("match column names case-insensitively") {
    val drift = diff(declared("User_ID" -> "int"), actual("user_id" -> "bigint"))
    drift.hasDrift shouldBe false
  }

  test("detect added and removed columns") {
    val drift = diff(
      declared("id" -> "int", "created_at"     -> "timestamp"),
      actual("id"   -> "bigint", "legacy_flag" -> "boolean")
    )
    drift.hasDrift shouldBe true
    drift.addColumns.map(_._1) shouldBe List("created_at")
    drift.excludeColumns shouldBe List("legacy_flag")
    val rendered = SchemaDriftDetector.render(drift)
    rendered shouldContain "table users has drifted from its declaration. To migrate, run:"
    rendered shouldContain "reshape users {"
    rendered shouldContain "add created_at: timestamp"
    rendered shouldContain "exclude legacy_flag"
    // A paired exclude/add may actually be a rename, which a diff cannot infer
    rendered shouldContain "rename <old> as <new>"
  }

  test("skip the rename hint when no exclude/add pair exists") {
    val drift = diff(declared("id" -> "int", "created_at" -> "timestamp"), actual("id" -> "bigint"))
    val rendered = SchemaDriftDetector.render(drift)
    rendered shouldContain "add created_at: timestamp"
    rendered shouldNotContain "rename"
  }

  test("report type changes as cast operations inside the reshape block") {
    val drift = diff(
      declared("id" -> "int", "status"    -> "string", "extra" -> "int"),
      actual("id"   -> "bigint", "status" -> "double")
    )
    drift.typeChanges.map(_.column) shouldBe List("status")
    val rendered = SchemaDriftDetector.render(drift)
    rendered shouldContain "add extra: int"
    rendered shouldContain "cast status as string -- the catalog has double"
  }

  test("render type-only drift as a reshape block with cast operations") {
    val drift = diff(declared("id" -> "string"), actual("id" -> "bigint"))
    drift.hasDrift shouldBe true
    val rendered = SchemaDriftDetector.render(drift)
    rendered shouldContain "table users has drifted from its declaration. To migrate, run:"
    rendered shouldContain "reshape users {"
    rendered shouldContain "cast id as string -- the catalog has long"
  }

  test("detect precision changes of parameterized types") {
    val drift = diff(declared("price" -> "decimal(10,2)"), actual("price" -> "decimal(12,2)"))
    drift.typeChanges.map(_.column) shouldBe List("price")
  }

  test("backquote column names that need quoting") {
    val drift = diff(declared("count" -> "int"), actual())
    SchemaDriftDetector.render(drift) shouldContain "add `count`: int"
  }

  test("qualify the reshape target only when bound outside the context catalog/schema") {
    SchemaDriftDetector.reshapeTarget("users", None, "memory", "main") shouldBe "users"
    SchemaDriftDetector.reshapeTarget("users", Some(("memory", "Main")), "memory", "main") shouldBe
      "users"
    SchemaDriftDetector.reshapeTarget("users", Some(("mydb", "sales")), "memory", "main") shouldBe
      "mydb.sales.users"
  }

end SchemaDriftDetectorTest
