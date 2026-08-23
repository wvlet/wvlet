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

import wvlet.lang.catalog.StaticCatalogExporter
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.VarArgType
import wvlet.lang.model.plan.*
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

  /** One function definition collected from the stdlib sources */
  private case class DefEntry(
      typeName: String,
      fn: FunctionDef,
      // dialect scopes of the enclosing type block (e.g. `type any in duckdb`)
      typeContexts: List[String],
      description: String
  ):
    /** Dialect names this definition is scoped to; empty for a dialect-neutral definition */
    def dialects: List[String] =
      val own = fn.defContexts.map(_.contextType.leafName.toLowerCase)
      (typeContexts ++ own).distinct

    /** Signature key: same name and arity merge into one documented row */
    def signature: String =
      if fn.args.isEmpty then
        fn.name.name
      else
        s"${fn.name.name}(${fn
            .args
            .map(a => s"${a.name.name}: ${typeStr(a.givenDataType)}")
            .mkString(", ")})"

  private def typeStr(dt: DataType): String =
    dt match
      case VarArgType(elem) =>
        s"${typeStr(elem)}*"
      case _: DataType.DecimalType =>
        // Parsed `decimal` carries default precision/scale parameters; render the plain name
        "decimal"
      case _ if dt.typeParams.isEmpty =>
        dt.typeName.name
      case _ =>
        s"${dt.typeName.name}[${dt.typeParams.map(typeStr).mkString(",")}]"

  /**
    * The doc comment of a definition: the run of `--` comment lines immediately above it. Comment
    * lines separated by a blank line (file headers, section notes) are not part of the doc
    */
  private def commentText(
      sf: wvlet.lang.compiler.SourceFile,
      t: wvlet.lang.model.SyntaxTreeNode
  ): String =
    var expected = sf.offsetToLine(t.span.start) - 1
    val adjacent = List.newBuilder[String]
    // comments are stored innermost-last; walk upward from the definition line
    t.comments
      .foreach { c =>
        val line = sf.offsetToLine(c.offset)
        if line == expected then
          adjacent += c.str.trim.stripPrefix("--").trim
          expected = line - 1
      }
    adjacent.result().reverse.filter(_.nonEmpty).mkString(" ")

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
    val handWritten = CompilationUnit
      .stdLib
      .filterNot(
        _.sourceFile.getContentAsString.contains(StaticCatalogExporter.generatedFileHeader)
      )
      .sortBy(_.sourceFile.fileName)

    val memberDefs   = mutable.ListBuffer.empty[DefEntry]
    val topLevelDefs = mutable.ListBuffer.empty[DefEntry]

    handWritten.foreach { unit =>
      val sf = unit.sourceFile
      ParserPhase.parseOnly(unit) match
        case p: PackageDef =>
          p.statements
            .foreach {
              case t: TypeDef =>
                val typeContexts = t.defContexts.map(_.contextType.leafName.toLowerCase)
                t.elems
                  .foreach {
                    case f: FunctionDef =>
                      memberDefs += DefEntry(t.name.name, f, typeContexts, commentText(sf, f))
                    case _ =>
                  }
              case t: TopLevelFunctionDef =>
                val desc =
                  commentText(sf, t) match
                    case "" =>
                      commentText(sf, t.functionDef)
                    case s =>
                      s
                topLevelDefs += DefEntry("", t.functionDef, Nil, desc)
              case _ =>
            }
        case _ =>
    }

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

    def renderRows(entries: List[DefEntry]): Unit =
      sb ++= "\n| Function | Returns | Engines | Description |\n"
      sb ++= "|----------|---------|---------|-------------|\n"
      // Merge same-signature dialect variants into one row, keeping source order
      val seen = mutable.LinkedHashMap.empty[String, mutable.ListBuffer[DefEntry]]
      entries.foreach(e => seen.getOrElseUpdate(e.signature, mutable.ListBuffer.empty) += e)
      def cell(s: String): String = s.replace("|", "\\|")
      seen.foreach { case (signature, defs) =>
        val returns = defs.flatMap(_.fn.retType).headOption.map(typeStr).getOrElse("")
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
      val entries = memberDefs.filter(_.typeName == typeName).toList
      if entries.nonEmpty then
        sb ++= s"\n## ${title}\n"
        if intro.nonEmpty then
          sb ++= s"\n${intro}\n"
        renderRows(entries)
    }

    // Types not covered by the curated section list (e.g. dialect-only extension types)
    val knownSections = typeSections.map(_._1).toSet
    val leftoverTypes = memberDefs.map(_.typeName).distinct.filterNot(knownSections.contains)
    leftoverTypes.foreach { typeName =>
      val entries = memberDefs.filter(_.typeName == typeName).toList
      sb ++= s"\n## ${typeName} Values\n"
      renderRows(entries)
    }

    if topLevelDefs.nonEmpty then
      sb ++= "\n## Top-Level Functions\n"
      sb ++= "\nCalled without a receiver value (window functions require an `over(...)` clause).\n"
      renderRows(topLevelDefs.toList)

    sb.result()
  end generateMarkdown

  def main(args: Array[String]): Unit =
    val md = generateMarkdown()
    SourceIO.writeString(targetPath, md)
    info(s"Wrote ${targetPath}")

end StdLibDocGenerator
