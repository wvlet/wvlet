package wvlet.lang.compiler.analyzer

import wvlet.lang.compiler.SourceIO
import wvlet.lang.model.DataType
import wvlet.lang.model.RelationType
import wvlet.uni.test.UniTest

/**
  * Cross-platform tests for [[JSONAnalyzer]] record sampling. The sampler is pure Scala (uni's
  * JSONScanner), so the same expectations hold on JVM, JS, and Native.
  */
class JSONAnalyzerTest extends UniTest:

  private def columnTypes(rel: RelationType): Map[String, DataType] =
    rel.fields.map(f => f.name.name -> f.dataType).toMap

  private def records(n: Int): String = (0 until n)
    .map(i => s"""{"id": ${i}, "name": "u${i}"}""")
    .mkString("[", ",\n", "]")

  test("infer schema from a top-level array") {
    val types = columnTypes(
      JSONAnalyzer.analyzeJSONContent(records(3), isJsonLines = false, sampleSize = 10)
    )
    types shouldBe Map("id" -> DataType.LongType, "name" -> DataType.StringType)
  }

  test("infer schema from a single top-level object") {
    val types = columnTypes(
      JSONAnalyzer.analyzeJSONContent(
        """{"id": 1, "score": 1.5, "ok": true}""",
        isJsonLines = false,
        sampleSize = 10
      )
    )
    types shouldBe
      Map("id" -> DataType.LongType, "score" -> DataType.DoubleType, "ok" -> DataType.BooleanType)
  }

  test("stop scanning after the sample size is reached") {
    // A field that first appears past the sample limit must not be part of the schema, and the
    // sampled records must be the first ones (id values 0..limit-1)
    val json = (0 until 100)
      .map { i =>
        if i < 50 then
          s"""{"id": ${i}}"""
        else
          s"""{"id": ${i}, "late": "x"}"""
      }
      .mkString("[", ",", "]")
    val types = columnTypes(
      JSONAnalyzer.analyzeJSONContent(json, isJsonLines = false, sampleSize = 50)
    )
    types shouldBe Map("id" -> DataType.LongType)

    // Without the limit the late field is visible
    val all = columnTypes(
      JSONAnalyzer.analyzeJSONContent(json, isJsonLines = false, sampleSize = 1000)
    )
    all shouldBe Map("id" -> DataType.LongType, "late" -> DataType.StringType)
  }

  test("keep nested arrays intact while sampling") {
    val json  = """[{"id": 1, "tags": ["a", "b", "c", "d"]}, {"id": 2, "tags": []}]"""
    val types = columnTypes(
      JSONAnalyzer.analyzeJSONContent(json, isJsonLines = false, sampleSize = 1)
    )
    types shouldBe Map("id" -> DataType.LongType, "tags" -> DataType.StringType)
  }

  test("analyze a JSON file from the spec folder") {
    val types = columnTypes(JSONAnalyzer.analyzeJSONFile("spec/basic/person.json"))
    types shouldBe
      Map("id" -> DataType.LongType, "name" -> DataType.StringType, "age" -> DataType.LongType)
  }

  test("sample a JSON file with more records than the sample size") {
    val path = "target/json-analyzer-test/large.json"
    SourceIO.writeString(path, records(200))
    try
      val types = columnTypes(
        JSONAnalyzer.analyzeJSONFile(
          path,
          DataFilePath(DataFilePath.Format.JSON, None),
          sampleSize = 20
        )
      )
      types shouldBe Map("id" -> DataType.LongType, "name" -> DataType.StringType)
    finally
      SourceIO.deleteFile(path)
  }

  test("sample JSON Lines records") {
    val jsonl = (0 until 100)
      .map { i =>
        if i < 10 then
          s"""{"id": ${i}}"""
        else
          s"""{"id": ${i}, "late": true}"""
      }
      .mkString("\n")
    val types = columnTypes(
      JSONAnalyzer.analyzeJSONContent(jsonl, isJsonLines = true, sampleSize = 10)
    )
    types shouldBe Map("id" -> DataType.LongType)
  }

end JSONAnalyzerTest
