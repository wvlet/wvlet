package wvlet.lang.compiler.analyzer

import wvlet.lang.compiler.Name
import wvlet.lang.compiler.SourceIO
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.RelationType
import wvlet.uni.test.UniTest

class FileSchemaCacheTest extends UniTest:

  private def schema(col: String): RelationType = SchemaType(
    None,
    Name.typeName(RelationType.newRelationTypeName),
    List(NamedType(Name.termName(col), DataType.LongType))
  )

  private def withTempFile[A](name: String, content: String)(body: String => A): A =
    val path = s"target/file-schema-cache-test/${name}"
    SourceIO.writeString(path, content)
    try body(path)
    finally SourceIO.deleteFile(path)

  test("reuse the inferred schema while the file is unchanged") {
    withTempFile("unchanged.json", "[]") { path =>
      val cache = FileSchemaCache()
      var calls = 0
      val first =
        cache.getOrElseUpdate(path) {
          calls += 1
          schema("a")
        }
      val second =
        cache.getOrElseUpdate(path) {
          calls += 1
          schema("b")
        }
      calls shouldBe 1
      second shouldBeTheSameInstanceAs first
      cache.size shouldBe 1
    }
  }

  test("re-infer after the file is modified") {
    withTempFile("modified.json", "[]") { path =>
      val cache  = FileSchemaCache()
      val before = SourceIO.lastUpdatedAt(path)
      cache.getOrElseUpdate(path)(schema("a"))

      // Rewrite until the mtime visibly changes (filesystem timestamp granularity varies)
      val deadline = System.currentTimeMillis() + 5000
      while SourceIO.lastUpdatedAt(path) == before && System.currentTimeMillis() < deadline do
        SourceIO.writeString(path, "[{}]")
      SourceIO.lastUpdatedAt(path) shouldNotBe before

      var called = false
      cache.getOrElseUpdate(path) {
        called = true
        schema("b")
      }
      called shouldBe true
      cache.size shouldBe 1
    }
  }

  test("never cache a missing file") {
    val cache = FileSchemaCache()
    var calls = 0
    cache.getOrElseUpdate("target/file-schema-cache-test/missing.json") {
      calls += 1
      schema("a")
    }
    cache.getOrElseUpdate("target/file-schema-cache-test/missing.json") {
      calls += 1
      schema("a")
    }
    calls shouldBe 2
    cache.size shouldBe 0
  }

  test("cache remote paths for the lifetime of the compiler") {
    val cache = FileSchemaCache()
    var calls = 0
    Seq("s3://bucket/data.parquet", "https://example.com/data.parquet").foreach { url =>
      cache.getOrElseUpdate(url) {
        calls += 1
        schema("a")
      }
      cache.getOrElseUpdate(url) {
        calls += 1
        schema("b")
      }
    }
    calls shouldBe 2
    cache.size shouldBe 2
  }

end FileSchemaCacheTest
