package wvlet.lang.compiler.analyzer

import wvlet.lang.compiler.analyzer.DataFilePath.Compression
import wvlet.lang.compiler.analyzer.DataFilePath.Format
import wvlet.uni.test.UniTest

class DataFilePathTest extends UniTest:

  test("should recognize plain data file extensions") {
    DataFilePath.parse("data.json") shouldBe Some(DataFilePath(Format.JSON, None))
    DataFilePath.parse("data.jsonl") shouldBe Some(DataFilePath(Format.JSONL, None))
    DataFilePath.parse("data.ndjson") shouldBe Some(DataFilePath(Format.NDJSON, None))
    DataFilePath.parse("data.csv") shouldBe Some(DataFilePath(Format.CSV, None))
    DataFilePath.parse("data.tsv") shouldBe Some(DataFilePath(Format.TSV, None))
    DataFilePath.parse("dir/data.parquet") shouldBe Some(DataFilePath(Format.PARQUET, None))
  }

  test("should recognize compressed data files") {
    DataFilePath.parse("data.json.gz") shouldBe
      Some(DataFilePath(Format.JSON, Some(Compression.GZ)))
    DataFilePath.parse("data.jsonl.gz") shouldBe
      Some(DataFilePath(Format.JSONL, Some(Compression.GZ)))
    DataFilePath.parse("data.csv.zst") shouldBe
      Some(DataFilePath(Format.CSV, Some(Compression.ZST)))
    DataFilePath.parse("s3://bucket/x/data.tsv.gz") shouldBe
      Some(DataFilePath(Format.TSV, Some(Compression.GZ)))
  }

  test("should be case-insensitive") {
    DataFilePath.parse("DATA.JSONL.GZ") shouldBe
      Some(DataFilePath(Format.JSONL, Some(Compression.GZ)))
  }

  test("should reject non-data files") {
    DataFilePath.parse("query.wv") shouldBe None
    DataFilePath.parse("query.sql") shouldBe None
    DataFilePath.parse("archive.gz") shouldBe None
    DataFilePath.parse("noext") shouldBe None
    DataFilePath.parse(".json") shouldBe None
    // Parquet carries its own compression; DuckDB cannot read gzip-wrapped parquet
    DataFilePath.parse("data.parquet.gz") shouldBe None
  }

  test("should ignore URL query strings and fragments") {
    DataFilePath.parse("https://host/data.parquet?X-Amz-Signature=abc") shouldBe
      Some(DataFilePath(Format.PARQUET, None))
    DataFilePath.parse("https://host/data.csv.gz#part") shouldBe
      Some(DataFilePath(Format.CSV, Some(Compression.GZ)))
  }

  test("should leave remote JSON files to the query engine") {
    val jsonl = DataFilePath.parse("events.jsonl").get
    DuckDBAnalyzer.usesJsonAnalyzer("events.jsonl", jsonl) shouldBe true
    DuckDBAnalyzer.usesJsonAnalyzer("https://host/events.jsonl", jsonl) shouldBe false
    DuckDBAnalyzer.usesJsonAnalyzer("s3://bucket/events.jsonl", jsonl) shouldBe false
  }

  test("should route JSON family to JSONAnalyzer unless zstd-compressed") {
    DataFilePath.parse("a.json").get.canUseJsonAnalyzer shouldBe true
    DataFilePath.parse("a.ndjson").get.canUseJsonAnalyzer shouldBe true
    DataFilePath.parse("a.jsonl.gz").get.canUseJsonAnalyzer shouldBe true
    DataFilePath.parse("a.jsonl.zst").get.canUseJsonAnalyzer shouldBe false
    DataFilePath.parse("a.csv").get.canUseJsonAnalyzer shouldBe false
  }

end DataFilePathTest
