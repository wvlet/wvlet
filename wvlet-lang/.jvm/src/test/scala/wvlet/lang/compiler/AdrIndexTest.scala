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
package wvlet.lang.compiler

import wvlet.uni.test.UniTest

import java.io.File
import java.nio.file.Files

/**
  * Keeps the ADR index in `adr/README.md` newest first (same-date entries by file name), with
  * exactly one entry per record file in `adr/` and no entry for a missing file.
  */
class AdrIndexTest extends UniTest:
  private val adrDir = File("adr")
  private val entry  = """^- \[.*\]\(([^)]+\.md)\)""".r.unanchored

  private def indexedFiles: List[String] =
    val text  = Files.readString(File(adrDir, "README.md").toPath)
    val index = text.split("(?m)^## Index$", 2)
    if index.length < 2 then
      fail("adr/README.md has no '## Index' section")
    index(1)
      .linesIterator
      .collect {
        case line @ entry(file) if line.startsWith("- [") =>
          file
      }
      .toList

  test("should list every ADR exactly once") {
    val records =
      adrDir
        .listFiles()
        .toList
        .map(_.getName)
        .filter(n => n.endsWith(".md") && n != "README.md")
        .toSet
    val indexed    = indexedFiles
    val duplicates = indexed.diff(indexed.distinct).distinct
    val missing    = records -- indexed
    val dangling   = indexed.toSet -- records
    if duplicates.nonEmpty || missing.nonEmpty || dangling.nonEmpty then
      fail(s"""adr/README.md index is out of sync with adr/:
           |  listed more than once: ${duplicates.mkString(", ")}
           |  not listed: ${missing.toList.sorted.mkString(", ")}
           |  listed but missing: ${dangling.toList.sorted.mkString(", ")}""".stripMargin)
  }

  test("should keep the index newest first, same-date entries by file name") {
    val indexed  = indexedFiles
    val expected = indexed.sorted.sortBy(_.take(10))(using Ordering[String].reverse)
    if indexed != expected then
      val firstWrong = indexed.zip(expected).indexWhere((a, b) => a != b)
      fail(s"""adr/README.md index is not newest first at entry ${firstWrong + 1}:
           |  found ${indexed(firstWrong)}, expected ${expected(firstWrong)}
           |Add new records at the top; same-date entries are ordered by file name.""".stripMargin)
  }

end AdrIndexTest
