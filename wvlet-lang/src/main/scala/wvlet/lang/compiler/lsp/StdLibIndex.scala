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
package wvlet.lang.compiler.lsp

import wvlet.lang.catalog.StaticCatalogExporter
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.SourceFile
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.VarArgType
import wvlet.lang.model.SyntaxTreeNode
import wvlet.lang.model.plan.*

/**
  * One function definition of the hand-written standard library, extracted for tooling (editor
  * completion, hover docs, generated reference docs).
  *
  * @param name
  *   The function name (e.g. "upper")
  * @param ownerType
  *   The type the function is a member of (e.g. "string"), or empty for a top-level function
  * @param signature
  *   The rendered signature: the name plus its typed argument list, e.g. `substring(start: int)`
  * @param returnType
  *   The rendered return type (e.g. "string"), or empty when the definition declares none
  * @param dialects
  *   Engine dialects this definition is scoped to (e.g. List("duckdb")); empty for a
  *   dialect-neutral definition serving every engine
  * @param description
  *   The `--` doc comment lines directly above the definition, joined into one line
  */
case class StdLibFunctionDoc(
    name: String,
    ownerType: String,
    signature: String,
    returnType: String,
    dialects: List[String],
    description: String
)

/**
  * An index of the hand-written standard library function definitions, built once by parsing the
  * embedded stdlib sources (generated engine catalogs excluded). Cross-platform so both the JVM
  * language server and the Scala.js editor integrations can serve signatures and docs from it.
  */
object StdLibIndex:

  /** Render a data type for signatures: `array[A]`, `any*`, plain `decimal` */
  def typeStr(dt: DataType): String =
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

  /** Render the signature of a function definition: its name plus the typed argument list */
  def signatureOf(fn: FunctionDef): String =
    if fn.args.isEmpty then
      fn.name.name
    else
      s"${fn.name.name}(${fn
          .args
          .map(a => s"${a.name.name}: ${typeStr(a.givenDataType)}")
          .mkString(", ")})"

  /**
    * The doc comment of a definition: the run of `--` comment lines immediately above it. Comment
    * lines separated by a blank line (file headers, section notes) are not part of the doc
    */
  def commentText(sf: SourceFile, t: SyntaxTreeNode): String =
    var expected = sf.offsetToLine(t.span.start) - 1
    val adjacent = List.newBuilder[String]
    // comments are stored innermost-first; walk upward from the definition line
    t.comments
      .foreach { c =>
        val line = sf.offsetToLine(c.offset)
        if line == expected then
          adjacent += c.str.trim.stripPrefix("--").trim
          expected = line - 1
      }
    adjacent.result().reverse.filter(_.nonEmpty).mkString(" ")

  private def entryOf(
      sf: SourceFile,
      ownerType: String,
      typeContexts: List[String],
      fn: FunctionDef,
      description: String
  ): StdLibFunctionDoc =
    val own = fn.defContexts.map(_.contextType.leafName.toLowerCase)
    StdLibFunctionDoc(
      name = fn.name.name,
      ownerType = ownerType,
      signature = signatureOf(fn),
      returnType = fn.retType.map(typeStr).getOrElse(""),
      dialects = (typeContexts ++ own).distinct,
      description = description
    )

  /**
    * All function definitions of the hand-written stdlib, in source-file order. Member functions
    * carry their owner type name; top-level functions have an empty owner
    */
  lazy val allFunctions: List[StdLibFunctionDoc] =
    val handWritten = CompilationUnit
      .stdLib
      .filterNot(
        _.sourceFile.getContentAsString.contains(StaticCatalogExporter.generatedFileHeader)
      )
      .sortBy(_.sourceFile.fileName)
    val entries = List.newBuilder[StdLibFunctionDoc]
    handWritten.foreach { unit =>
      val sf = unit.sourceFile
      try
        ParserPhase.parseOnly(unit) match
          case p: PackageDef =>
            p.statements
              .foreach {
                case t: TypeDef =>
                  val typeContexts = t.defContexts.map(_.contextType.leafName.toLowerCase)
                  t.elems
                    .foreach {
                      case f: FunctionDef =>
                        entries += entryOf(sf, t.name.name, typeContexts, f, commentText(sf, f))
                      case _ =>
                    }
                case t: TopLevelFunctionDef =>
                  val desc =
                    commentText(sf, t) match
                      case "" =>
                        commentText(sf, t.functionDef)
                      case s =>
                        s
                  entries += entryOf(sf, "", Nil, t.functionDef, desc)
                case _ =>
              }
          case _ =>
      catch
        case _: Throwable =>
        // The embedded stdlib always parses; guard anyway so editor tooling can never fail on it
    }
    entries.result()

  end allFunctions

  /** Member function definitions grouped by lowercase function name */
  lazy val memberFunctionsByName: Map[String, List[StdLibFunctionDoc]] = allFunctions
    .filter(_.ownerType.nonEmpty)
    .groupBy(_.name.toLowerCase)

  /** Top-level function definitions (e.g. row_number, ulid_string) grouped by lowercase name */
  lazy val topLevelFunctionsByName: Map[String, List[StdLibFunctionDoc]] = allFunctions
    .filter(_.ownerType.isEmpty)
    .groupBy(_.name.toLowerCase)

  /** Member function definitions grouped by their owner type name */
  lazy val membersByType: Map[String, List[StdLibFunctionDoc]] = allFunctions
    .filter(_.ownerType.nonEmpty)
    .groupBy(_.ownerType)

  /** All definitions (member and top-level) for the given function name, or Nil */
  def functionsNamed(name: String): List[StdLibFunctionDoc] =
    val key = name.toLowerCase
    memberFunctionsByName.getOrElse(key, Nil) ++ topLevelFunctionsByName.getOrElse(key, Nil)

end StdLibIndex
