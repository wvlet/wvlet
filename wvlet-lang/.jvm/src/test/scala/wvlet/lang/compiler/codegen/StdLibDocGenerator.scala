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
package wvlet.lang.compiler.codegen

import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.lsp.StdLibFunctionDoc
import wvlet.lang.compiler.lsp.StdLibIndex
import wvlet.uni.log.LogSupport

import scala.collection.mutable

/**
  * Generate the standard-library reference page (website/docs/syntax/stdlib-reference.md) from the
  * hand-written stdlib .wv sources, so the documented function set can never drift from the
  * implementation. Descriptions come from the `--` comments above each definition; engine coverage
  * comes from the `in <dialect>` scopes.
  *
  * Run with: ./sbt "langJVM/Test/runMain wvlet.lang.compiler.codegen.StdLibDocGenerator"
  */
object StdLibDocGenerator extends LogSupport:
  /** Path of the generated reference page, relative to the repository root */
  val targetPath = "website/docs/syntax/stdlib-reference.md"

  /** Regeneration command shown in the page header and in freshness-check failures */
  val regenCommand = "./sbt \"langJVM/Test/runMain wvlet.lang.compiler.codegen.StdLibDocGenerator\""

  /** Section order and human titles of the reference page */
  private val typeSections: List[(String, String, String)] = List(
    ("any", "Any Values", "Available on values of every type."),
    (
      "numeric",
      "Numeric Values",
      "Shared by all numeric types (int, long, float, real, double, decimal)."
    ),
    ("int", "Int Values", ""),
    ("long", "Long Values", ""),
    ("float", "Float Values", ""),
    ("real", "Real Values", ""),
    ("double", "Double Values", ""),
    ("decimal", "Decimal Values", ""),
    ("boolean", "Boolean Values", ""),
    ("string", "String Values", ""),
    ("date", "Date Values", ""),
    ("timestamp", "Timestamp Values", ""),
    ("json", "JSON Values", ""),
    ("null", "Null Values", ""),
    (
      "array",
      "Array Values",
      "A column reference after `group by` also has an array type, so aggregation functions apply to it with the same syntax."
    ),
    ("map", "Map Values", "")
  )

  def generateMarkdown(): String =
    val memberDefs   = StdLibIndex.allFunctions.filter(_.ownerType.nonEmpty)
    val topLevelDefs = StdLibIndex.allFunctions.filter(_.ownerType.isEmpty)

    val sb = StringBuilder()
    sb ++= s"""---
         |sidebar_label: Stdlib Reference
         |---
         |
         |# Standard Library Reference
         |
         |<!-- Generated from wvlet-stdlib/module/standard/*.wv. DO NOT EDIT.
         |     Regenerate with: ${regenCommand} -->
         |
         |Complete listing of the functions defined in the Wvlet standard library, generated from
         |its sources. Call these with dot syntax on a value of the listed type (e.g. `name.upper`,
         |`price.round(2)`). See [Standard Library Functions](stdlib.md) for a guided tour with
         |examples.
         |
         |The **Engines** column lists the engines a definition is specialized for; **all** means a
         |single dialect-neutral definition serves every supported engine (DuckDB, Trino, Hive,
         |Snowflake, and BigQuery). Engine-specific SQL is selected automatically for the compile
         |target.
         |""".stripMargin

    def renderRows(entries: List[StdLibFunctionDoc]): Unit =
      sb ++= "\n| Function | Returns | Engines | Description |\n"
      sb ++= "|----------|---------|---------|-------------|\n"
      // Merge same-signature dialect variants into one row, keeping source order
      val seen = mutable.LinkedHashMap.empty[String, mutable.ListBuffer[StdLibFunctionDoc]]
      entries.foreach(e => seen.getOrElseUpdate(e.signature, mutable.ListBuffer.empty) += e)
      def cell(s: String): String = s.replace("|", "\\|")
      seen.foreach { case (signature, defs) =>
        val returns = defs.map(_.returnType).find(_.nonEmpty).getOrElse("")
        val engines =
          if defs.exists(_.dialects.isEmpty) then
            "all"
          else
            defs.flatMap(_.dialects).distinct.mkString(", ")
        // A dialect variant's comment explains that engine's quirk, not the function: when a
        // dialect-neutral definition exists, only its comment may serve as the description
        val hasNeutral = defs.exists(_.dialects.isEmpty)
        val descPool   =
          if hasNeutral then
            defs.filter(_.dialects.isEmpty)
          else
            defs
        val desc = descPool.map(_.description).find(_.nonEmpty).getOrElse("")
        sb ++= s"| `${cell(signature)}` | ${cell(returns)} | ${engines} | ${cell(desc)} |\n"
      }

    typeSections.foreach { case (typeName, title, intro) =>
      val entries = memberDefs.filter(_.ownerType == typeName)
      if entries.nonEmpty then
        sb ++= s"\n## ${title}\n"
        if intro.nonEmpty then
          sb ++= s"\n${intro}\n"
        renderRows(entries)
    }

    // Types not covered by the curated section list (e.g. dialect-only extension types)
    val knownSections = typeSections.map(_._1).toSet
    val leftoverTypes = memberDefs.map(_.ownerType).distinct.filterNot(knownSections.contains)
    leftoverTypes.foreach { typeName =>
      val entries = memberDefs.filter(_.ownerType == typeName)
      sb ++= s"\n## ${typeName} Values\n"
      renderRows(entries)
    }

    if topLevelDefs.nonEmpty then
      sb ++= "\n## Top-Level Functions\n"
      sb ++= "\nCalled without a receiver value (window functions require an `over(...)` clause).\n"
      renderRows(topLevelDefs)

    sb.result()
  end generateMarkdown

  def main(args: Array[String]): Unit =
    val md = generateMarkdown()
    SourceIO.writeString(targetPath, md)
    info(s"Wrote ${targetPath}")

end StdLibDocGenerator
