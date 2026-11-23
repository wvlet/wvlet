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
package wvlet.lang.engine.catalog

import wvlet.lang.engine.vector.*
import wvlet.lang.model.DataType
import wvlet.uni.test.UniTest

/** Converting rows of other engines and source connectors into engine tables */
class ExternalRowsTest extends UniTest:

  test("should type text rows by their source column types") {
    val schema = StringRows.schemaOf(
      Seq(
        "id"   -> DataType.LongType,
        "d"    -> DataType.DateType,
        "amt"  -> DataType.parse("decimal(10,2)"),
        "ok"   -> DataType.BooleanType,
        "tags" -> DataType.parse("array(varchar)")
      )
    )
    schema.types shouldBe
      IndexedSeq(PType.Int64, PType.Date, PType.Decimal(10, 2), PType.Bool, PType.Str)
    val b = StringRows.toBatch(
      schema,
      Seq(
        Seq(Some("1"), Some("2026-01-02"), Some("3.50"), Some("true"), Some("[a, b]")),
        Seq(Some("2"), None, None, Some("false"), None)
      )
    )
    b.size shouldBe 2
    b.column(0).get(1) shouldBe 2L
    b.column(1).get(0) shouldBe ValueOps.parseDate("2026-01-02")
    b.column(1).isNull(1) shouldBe true
    ValueOps.toBigDecimal(b.column(2).get(0)).toPlainString shouldBe "3.50"
    b.column(3).get(1) shouldBe false
    b.column(4).get(0) shouldBe "[a, b]"
  }

  test("should keep a time-zoned timestamp as text") {
    val schema = StringRows.schemaOf(
      Seq("ts" -> DataType.TimestampType(DataType.TimestampField.TIMESTAMP, withTimeZone = true))
    )
    schema.types shouldBe IndexedSeq(PType.Str)
  }

  test("should build a table of JSON rows typed by a schema") {
    val schema = Schema(
      IndexedSeq(Field("name", PType.Str), Field("n", PType.Int64), Field("day", PType.Date))
    )
    val t = JsonRows.toMemTable(
      "t",
      Iterator("""{"name":"a","n":1,"day":"2026-01-02","extra":true}""", """{"n":2}"""),
      Some(schema)
    )
    t.schema.names shouldBe IndexedSeq("name", "n", "day")
    val b = t.scan(ScanSpec()).next()
    b.size shouldBe 2
    b.column(0).get(0) shouldBe "a"
    b.column(0).isNull(1) shouldBe true
    b.column(1).get(1) shouldBe 2L
    b.column(2).get(0) shouldBe ValueOps.parseDate("2026-01-02")
  }

  test("should infer columns of JSON rows without a schema") {
    val t = JsonRows.toMemTable("t", Iterator("""{"a":1,"b":"x"}""", """{"a":2.5}"""))
    t.schema.names shouldBe IndexedSeq("a", "b")
    t.schema.types shouldBe IndexedSeq(PType.Float64, PType.Str)
  }

  test("should build an empty table with the schema's columns") {
    val schema = Schema(IndexedSeq(Field("x", PType.Int64)))
    val t      = JsonRows.toMemTable("t", Iterator.empty, Some(schema))
    t.schema.names shouldBe IndexedSeq("x")
    t.estimatedRows shouldBe Some(0L)
  }

end ExternalRowsTest
