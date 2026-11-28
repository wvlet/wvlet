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

import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.lsp.StdLibReference
import wvlet.uni.log.LogSupport

/**
  * Generate the standard-library reference page (website/docs/syntax/stdlib-reference.md) from the
  * hand-written stdlib .wv sources, so the documented function set can never drift from the
  * implementation. Functions are grouped by category
  * ([[wvlet.lang.compiler.lsp.StdLibCategories]]); descriptions come from the `--` comments above
  * each definition, engine coverage from the `in <dialect>` scopes, and example results from
  * running each `-- @example` on the engine.
  *
  * Run with: ./sbt "engineJVM/Test/runMain wvlet.lang.engine.StdLibDocGenerator"
  */
object StdLibDocGenerator extends LogSupport:
  /** Path of the generated reference page, relative to the repository root */
  val targetPath = "website/docs/syntax/stdlib-reference.md"

  /** Regeneration command shown in the page header and in freshness-check failures */
  val regenCommand = "./sbt \"engineJVM/Test/runMain wvlet.lang.engine.StdLibDocGenerator\""

  // A table row is one line: fold the line breaks of multi-line values (e.g. JSON)
  private def cell(s: String): String = s.replaceAll("\\s*\n\\s*", " ").replace("|", "\\|")
  // Outside code spans, MDX reads `<` as a JSX tag and `{` as an expression (e.g., `stop <= start`)
  private def proseCell(s: String): String = cell(s).replace("<", "\\<").replace("{", "\\{")
  // In a code span, a backtick would end the span early
  private def codeCell(s: String): String =
    if s.contains("`") then
      s"`` ${cell(s)} ``"
    else
      s"`${cell(s)}`"

  /**
    * The page text. Throws when an example fails to run, so the freshness test also checks that
    * every example works
    */
  def generateMarkdown(examples: StdLibExamples = StdLibExamples()): String =
    val failures                          = List.newBuilder[String]
    def resultOf(example: String): String =
      examples.run(example) match
        case Right(r) =>
          r
        case Left(err) =>
          failures += s"${example}: ${err.linesIterator.nextOption().getOrElse("")}"
          ""

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
         |its sources and grouped by purpose. Call a function with dot syntax on a value of its
         |receiver type (`string.upper` is called as `name.upper`, `numeric.round(decimal: int)` as
         |`price.round(2)`). See [Standard Library Functions](stdlib.md) for a guided tour.
         |
         |A function defined on several types shows its receiver as `T` and lists the types
         |(`T.abs: T` with T: int, long, ...). Example results are computed by running each example
         |on the [Wvlet engine](../engines/wvlet-engine.md); try them in the
         |[Playground](https://wvlet.org/wvlet/playground/). A function marked with engines is
         |specialized for those engines only; the others have a single dialect-neutral definition
         |that serves every supported engine (DuckDB, Trino, Hive, Snowflake, and BigQuery).
         |
         || Category | Functions |
         ||----------|-----------|
         |""".stripMargin
    val sections = StdLibReference.sections
    sections.foreach { sec =>
      sb ++= s"| [${sec.category.title}](#${sec.category.id}) | ${sec.entries.size} |\n"
    }

    sections.foreach { sec =>
      sb ++= s"\n## ${sec.category.title} {#${sec.category.id}}\n"
      sb ++= s"\n${sec.category.intro}\n"
      sb ++= "\n| Function | Description | Example | Result |\n"
      sb ++= "|----------|-------------|---------|--------|\n"
      sec
        .entries
        .foreach { e =>
          val fn   = codeCell(e.signature) + e.typeParam.map(t => s"<br/>T: ${t}").getOrElse("")
          val desc = List(
            proseCell(e.description),
            Option.when(e.engines.nonEmpty)(s"Engines: ${proseCell(e.engines)}.").getOrElse("")
          ).filter(_.nonEmpty).mkString(" ")
          val ex  = e.examples.map(codeCell).mkString("<br/>")
          val res = e.examples.map(x => codeCell(resultOf(x))).mkString("<br/>")
          sb ++= s"| ${fn} | ${desc} | ${ex} | ${res} |\n"
        }
    }

    val failed = failures.result()
    if failed.nonEmpty then
      throw IllegalStateException(
        s"${failed.size} stdlib examples failed on the engine:\n  ${failed.mkString("\n  ")}"
      )
    sb.result()

  end generateMarkdown

  def main(args: Array[String]): Unit =
    val md = generateMarkdown()
    SourceIO.writeString(targetPath, md)
    info(s"Wrote ${targetPath}")

end StdLibDocGenerator
