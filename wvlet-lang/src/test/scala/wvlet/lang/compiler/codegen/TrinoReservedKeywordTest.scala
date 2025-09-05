package wvlet.lang.compiler.codegen

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.{Context, DBType}
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.lang.model.{DataType, RelationType}
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.compiler.Name

class TrinoReservedKeywordTest extends AirSpec:
  test("Trino reserved keywords in aggregated labels should not have nested quotes") {
    given Context = Context.NoContext.withDBType(DBType.Trino)
    
    // Create a test relation with reserved keyword column "table"
    val fields = List(
      DataType.NamedType(Name.termName("id"), DataType.IntType),
      DataType.NamedType(Name.termName("table"), DataType.StringType) // "table" is a reserved keyword
    )
    val relationType = RelationType(fields)
    
    // Create test data similar to VALUES [[1, "a"], [2, "b"]]
    val values = Values(
      List(
        ArrayConstructor(List(
          IntLiteral(1, NoSpan),
          StringLiteral.fromString("a", NoSpan)
        ), NoSpan),
        ArrayConstructor(List(
          IntLiteral(2, NoSpan), 
          StringLiteral.fromString("b", NoSpan)
        ), NoSpan)
      ),
      relationType,
      NoSpan
    )
    
    // Create a GROUP BY operation
    val groupBy = GroupBy(
      values,
      List(UnresolvedGroupingKey(NameExpr.fromString("id"), NoSpan)),
      NoSpan
    )
    
    // Generate SQL for Trino
    val config = CodeFormatterConfig().copy(sqlDBType = DBType.Trino)
    val generator = SqlGenerator(config)
    val sql = generator.print(groupBy)
    
    trace(s"Generated SQL:\n${sql}")
    
    // The SQL should contain the properly quoted function argument
    sql shouldContain """arbitrary("table")"""
    
    // But should NOT contain nested quotes in the alias
    sql should not contain """"arbitrary("table")""""
    
    // Instead it should contain the clean alias name  
    sql shouldContain """"arbitrary(table)""""
  }
  
  test("DuckDB should still work correctly with reserved keywords") {
    given Context = Context.NoContext.withDBType(DBType.DuckDB)
    
    // Same test setup as above but for DuckDB
    val fields = List(
      DataType.NamedType(Name.termName("id"), DataType.IntType),
      DataType.NamedType(Name.termName("table"), DataType.StringType)
    )
    val relationType = RelationType(fields)
    
    val values = Values(
      List(
        ArrayConstructor(List(
          IntLiteral(1, NoSpan),
          StringLiteral.fromString("a", NoSpan)
        ), NoSpan)
      ),
      relationType,
      NoSpan
    )
    
    val groupBy = GroupBy(
      values,
      List(UnresolvedGroupingKey(NameExpr.fromString("id"), NoSpan)),
      NoSpan
    )
    
    // Generate SQL for DuckDB
    val config = CodeFormatterConfig().copy(sqlDBType = DBType.DuckDB)
    val generator = SqlGenerator(config)
    val sql = generator.print(groupBy)
    
    trace(s"DuckDB Generated SQL:\n${sql}")
    
    // DuckDB should still work and use EmptyName for aliases (human-friendly names)
    sql shouldContain """arbitrary("table")"""
  }