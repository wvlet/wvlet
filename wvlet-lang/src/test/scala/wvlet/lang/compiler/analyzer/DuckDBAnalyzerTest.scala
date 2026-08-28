package wvlet.lang.compiler.analyzer

import wvlet.lang.compiler.analyzer.duckdb.DuckDB
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.uni.test.UniTest

/**
  * Cross-platform tests for [[DuckDBAnalyzer]] — the file-schema-inference dispatcher used by
  * `RelationRefResolver.resolveDataFileRef`.
  *
  *   - JSON cases exercise the [[JSONAnalyzer]] dispatch path, which is pure-Scala and works
  *     identically on JVM, JS, and Native.
  *   - Parquet cases exercise the [[DuckDB]] backend; they are skipped on platforms without a
  *     DuckDB backend (Scala.js today).
  *
  * Test data lives in the `spec` folder. The working directory at test time is the repo root for
  * all three platform test runners.
  */
class DuckDBAnalyzerTest extends UniTest:

  private def columnNamesOf(path: String): List[String] =
    DuckDBAnalyzer.guessSchema(path) match
      case SchemaType(_, _, cols) =>
        cols.collect { case n: NamedType =>
          n.name.name
        }
      case other =>
        fail(s"unexpected relation type for ${path}: ${other}")

  test("guess JSON schema dispatches to JSONAnalyzer (cross-platform)") {
    val rel = DuckDBAnalyzer.guessSchema("spec/basic/person.json")
    rel shouldMatch { case SchemaType(_, _, cols) =>
      val byName =
        cols
          .collect { case n: NamedType =>
            n.name.name -> n.dataType
          }
          .toMap
      // JSONAnalyzer infers types from the most-frequent observation per field. person.json has
      // {id, name, age} as primitives, so all three should resolve.
      byName.keySet shouldBe Set("id", "name", "age")
      byName("id") shouldBe DataType.LongType
      byName("name") shouldBe DataType.StringType
      byName("age") shouldBe DataType.LongType
    }
  }

  test("guess JSONL schema dispatches to JSONAnalyzer, including gzip-compressed files") {
    for path <- Seq("spec/basic/person.jsonl", "spec/basic/person.jsonl.gz") do
      columnNamesOf(path) shouldBe List("id", "name", "age")
  }

  test("guess gzip-compressed CSV and TSV schema goes through the DuckDB backend") {
    if !DuckDB.isAvailable then
      ignore("DuckDB backend not available on this platform (Scala.js stub)")
    else
      for path <- Seq("spec/basic/people.csv.gz", "spec/basic/people.tsv") do
        columnNamesOf(path) shouldBe List("id", "name", "age", "salary")
  }

  test("guess parquet schema goes through the DuckDB backend") {
    if !DuckDB.isAvailable then
      ignore("DuckDB backend not available on this platform (Scala.js stub)")
    else
      val rel = DuckDBAnalyzer.guessSchema(
        "spec/cdp_behavior/data/weblog_users_first_purchased_at/part-00000-90048009-4be7-472e-ad1a-019aed6bd6fa-c000.snappy.parquet"
      )
      rel shouldMatch { case SchemaType(_, _, cols) =>
        // We don't pin specific column names since the fixture's schema may evolve; we just
        // assert the backend actually opened the file and produced at least one named column.
        cols.size shouldNotBe 0
      }
  }

end DuckDBAnalyzerTest
