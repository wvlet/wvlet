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
package wvlet.lang.engine

import wvlet.lang.compiler.*
import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

/**
  * The `parquet_footer_cache` optimization: a parsed footer is reused while the file's size and
  * last-modified time stay the same, a changed or just-written file is parsed again, and with the
  * optimization off every open parses the footer as before
  */
class ParquetFooterCacheTest extends UniTest:
  private val dir = Paths.get("target/engine-test/footer-cache").toAbsolutePath
  Files.createDirectories(dir)

  private val nation = Paths.get("spec/tpch/data/nation.parquet")
  private val region = Paths.get("spec/tpch/data/region.parquet")

  private val minuteAgo = System.currentTimeMillis() - 60000

  /** A copy of `from` at `name`, last modified at `mtime` (None: just now) */
  private def copy(from: Path, name: String, mtime: Option[Long] = Some(minuteAgo)): String =
    val p = dir.resolve(name)
    Files.copy(from, p, StandardCopyOption.REPLACE_EXISTING)
    mtime.foreach(t => p.toFile.setLastModified(t))
    p.toString

  private def open(path: String): ParquetFooterCache.Opened =
    val o = ParquetFooterCache.open(path)
    o.file.close()
    o

  test("should reuse the parsed footer and statistics of an unchanged file") {
    val path  = copy(nation, "hit.parquet")
    val first = open(path)
    first.hit shouldBe false
    ParquetFooterCache.contains(path) shouldBe true
    val stats = first.columnStatistics

    val second = open(path)
    second.hit shouldBe true
    second.file.metadata shouldBeTheSameInstanceAs first.file.metadata
    second.columnStatistics shouldBeTheSameInstanceAs stats
    second.file.numRows shouldBe 25L
  }

  test("should parse again when the file size changes") {
    val path = copy(nation, "size.parquet")
    open(path).hit shouldBe false
    copy(region, "size.parquet")
    val o = open(path)
    o.hit shouldBe false
    o.file.numRows shouldBe 5L
    open(path).hit shouldBe true
  }

  test("should parse again when the last-modified time changes") {
    val path = copy(nation, "mtime.parquet")
    open(path).hit shouldBe false
    Paths.get(path).toFile.setLastModified(minuteAgo - 60000)
    open(path).hit shouldBe false
    open(path).hit shouldBe true
  }

  test("should not cache the footer of a file modified within the last seconds") {
    val path = copy(nation, "recent.parquet", mtime = None)
    open(path).hit shouldBe false
    ParquetFooterCache.contains(path) shouldBe false
    open(path).hit shouldBe false
  }

  test("should not cache footers without the flag") {
    val path = copy(nation, "off.parquet")
    val src  = ParquetSource("off", path)
    src.schema.fields.size shouldBe 4
    src.estimatedRows shouldBe Some(25L)
    src.scan(ScanSpec()).map(_.size).sum shouldBe 25
    ParquetFooterCache.contains(path) shouldBe false
  }

  test("should cache footers when the source and scans have the flag") {
    val path = copy(nation, "on.parquet")
    val src  = ParquetSource("on", path, footerCache = true)
    src.schema.fields.size shouldBe 4
    ParquetFooterCache.contains(path) shouldBe true
    src.scan(ScanSpec(footerCache = true)).map(_.size).sum shouldBe 25
  }

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def queryOf(e: ExecutionPlan): Option[Relation] =
    e match
      case ExecuteQuery(q: Relation) =>
        Some(q)
      case ExecuteTasks(ts) =>
        ts.iterator.flatMap(queryOf).nextOption()
      case _ =>
        None

  private def count(engine: WvletEngine, path: String): Any =
    val unit           = CompilationUnit.fromWvletString(s"from '${path}'\nagg _.count")
    val r              = compiler.compileSingleUnit(unit)
    given ctx: Context = r.context
    engine.execute(queryOf(unit.executionPlan).get).rows.next().head

  test("should follow the engine's parquet_footer_cache toggle") {
    val on = copy(nation, "engine-on.parquet")
    String.valueOf(count(WvletEngine(), on)) shouldBe "25"
    ParquetFooterCache.contains(on) shouldBe true

    val off       = copy(nation, "engine-off.parquet")
    val engineOff = WvletEngine(optimizer =
      OptimizerOptions().withDisabled(Optimization.ParquetFooterCache)
    )
    String.valueOf(count(engineOff, off)) shouldBe "25"
    ParquetFooterCache.contains(off) shouldBe false
  }

end ParquetFooterCacheTest
