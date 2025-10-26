package wvlet.lang.catalog

import wvlet.airspec.AirSpec
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.model.expr.DotRef
import wvlet.lang.model.expr.NameExpr

class TableNameTest extends AirSpec:
  test("create a table name with catalog and schema") {
    val tableName = TableName("my_catalog.my_schema.my_table")
    tableName.catalog shouldBe Some("my_catalog")
    tableName.schema shouldBe Some("my_schema")
    tableName.name shouldBe "my_table"
  }

  test("create a table name without catalog and schema") {
    val tableName = TableName("my_table")
    tableName.catalog shouldBe None
    tableName.schema shouldBe None
    tableName.name shouldBe "my_table"
  }

  test("reject invalid table names") {
    intercept[IllegalArgumentException] {
      TableName(Some("my_catalog"), None, "my_table")
    }
  }

  test("convert to Expression") {
    val tableName = TableName("my_catalog.my_schema.my_table")
    val expr      = tableName.toExpr
    expr shouldMatch { case DotRef(c: NameExpr, DotRef(s: NameExpr, t: NameExpr, _, _), _, _) =>
      c.fullName shouldBe "my_catalog"
      s.fullName shouldBe "my_schema"
      t.fullName shouldBe "my_table"
    }
  }

end TableNameTest
