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

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.SourceFile
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.model.SyntaxTreeNode
import wvlet.lang.model.expr.NameExpr
import wvlet.lang.model.plan.*

/**
  * The result of a go-to-definition request: the 1-based source range of the definition the cursor
  * resolves to. Only same-document definitions are returned, so the editor navigates within the
  * current file.
  *
  * @param startLine
  *   1-based line of the definition range start
  * @param startColumn
  *   1-based column of the definition range start
  * @param endLine
  *   1-based line of the definition range end
  * @param endColumn
  *   1-based column of the definition range end
  */
case class DefinitionResult(startLine: Int, startColumn: Int, endLine: Int, endColumn: Int)

/**
  * Provides go-to-definition for the Wvlet language: given a cursor on a model or type reference,
  * it returns the source range of the defining `model`/`type` statement.
  *
  * Like [[CompletionProvider]] and [[HoverProvider]], this lives in the cross-built `wvlet-lang`
  * module so it runs on both the JVM and Scala.js and can be unit tested on the JVM. Definition
  * requests fire on possibly incomplete source, so the best-effort compile is wrapped defensively:
  * any failure yields `None` rather than throwing.
  *
  * Resolution combines two strategies for robustness:
  *   - The resolved symbol chain: when full typing succeeds, a reference node carries a symbol
  *     whose `tree` is the defining `ModelDef`/`TypeDef`.
  *   - A name fallback: the referenced identifier is matched against the top-level `model`/`type`
  *     names collected from a parse-only pass, so navigation keeps working when full typing fails
  *     (e.g. a later statement has an error).
  *
  * This is single-document only: the language server compiles each document standalone, so
  * definitions resolve within the same file. Cross-file / workspace navigation is future work.
  */
object DefinitionProvider:

  /** A top-level model or type definition collected from a parse-only pass. */
  private case class DefinitionEntry(name: String, node: SyntaxTreeNode)

  /**
    * Compute the definition target for the given source at the given character offset.
    *
    * This never throws: parsing and typing are attempted defensively so that a partially written
    * query does not surface an error to the editor. Returns `None` when the cursor is not on a
    * resolvable model/type reference (e.g. on whitespace, a keyword, the definition itself, or an
    * unknown name).
    *
    * @param content
    *   The full Wvlet source text
    * @param offset
    *   0-based character offset of the cursor
    * @param compiler
    *   A compiler used for the best-effort full typing pass that resolves symbols
    */
  def definition(content: String, offset: Int, compiler: Compiler): Option[DefinitionResult] =
    try
      val unit = CompilationUnit.fromWvletString(content)

      // Collect top-level model/type definitions with a parse-only pass (resilient to parse errors)
      val definitions =
        try
          unit.unresolvedPlan = ParserPhase.parseOnly(unit)
          collectDefinitions(unit.unresolvedPlan)
        catch
          case _: Throwable =>
            Nil

      // Best-effort full typing so that references carry resolved symbols. Compiling re-parses the
      // unit, but the collected definition nodes stay valid: only their spans are used, and the
      // spans index the same content. If typing fails, the unresolved (parse-only) plan below keeps
      // name-based resolution working.
      try
        compiler.compileSingleUnit(unit)
      catch
        case _: Throwable =>
        // Ignore compile errors — fall back to the parse-only plan

      val searchPlan =
        if unit.resolvedPlan.nonEmpty then
          unit.resolvedPlan
        else
          unit.unresolvedPlan
      if searchPlan.isEmpty then
        None
      else
        val sourceFile = unit.sourceFile
        sourceFile.ensureLoaded
        // Candidate nodes covering the cursor, innermost (smallest span) first
        val candidates = searchPlan
          .collectAllNodes
          .filter(n => n.span.exists && n.span.containsInclusive(offset))
          .sortBy(_.span.size)
        candidates
          .iterator
          .flatMap { n =>
            // Resolving a single candidate may fail on an incomplete/malformed AST; skip it so an
            // enclosing candidate can still resolve.
            try
              definitionFor(n, definitions, offset).map(d => toResult(d, sourceFile))
            catch
              case _: Throwable =>
                None
          }
          .nextOption()
    catch
      case _: Throwable =>
        // Go-to-definition is opportunistic: never surface a compile error to the editor
        None

  end definition

  /**
    * Collect the top-level `model` and `type` definitions of the given plan. Like
    * [[CompletionProvider.definitionItems]], this recurses only into `PackageDef` containers, since
    * only top-level definitions are referenceable from other statements.
    */
  private def collectDefinitions(plan: LogicalPlan): List[DefinitionEntry] =
    val buf                        = List.newBuilder[DefinitionEntry]
    def loop(p: LogicalPlan): Unit =
      p match
        case pkg: PackageDef =>
          pkg.statements.foreach(loop)
        case m: ModelDef =>
          buf += DefinitionEntry(m.name.name, m)
        case t: TypeDef =>
          buf += DefinitionEntry(t.name.name, t)
        case _ =>
    loop(plan)
    buf.result()

  /**
    * Resolve the defining `ModelDef`/`TypeDef` for a single candidate node, or `None` if the node
    * is not a model/type reference. The resolved symbol's definition tree is preferred; the
    * collected definition names are used as a fallback when typing did not resolve the symbol.
    *
    * A definition whose span contains the cursor is treated as "already at the definition" and
    * yields `None`, so navigating from the definition itself (or a keyword/name within it) does not
    * jump to itself.
    */
  private def definitionFor(
      node: SyntaxTreeNode,
      definitions: List[DefinitionEntry],
      offset: Int
  ): Option[SyntaxTreeNode] =
    val bySymbol =
      try
        val sym = node.symbol
        if sym.isDefined then
          sym.tree match
            case d: ModelDef =>
              Some(d)
            case d: TypeDef =>
              Some(d)
            case _ =>
              None
        else
          None
      catch
        case _: Throwable =>
          None

    bySymbol
      .orElse {
        referenceName(node).flatMap { name =>
          definitions.find(_.name == name).map(_.node)
        }
      }
      .filter(d => d.span.exists && !d.span.containsInclusive(offset))

  /**
    * The referenced model/type name carried by a node, or `None` if the node is not a name-like
    * reference. Definitions themselves store their name as a compiler `Name` (not a child node), so
    * they are not mistaken for references here.
    */
  private def referenceName(node: SyntaxTreeNode): Option[String] =
    node match
      case m: ModelScan =>
        Some(m.name.name)
      case t: TableRef =>
        Some(t.name.leafName)
      case n: NameExpr =>
        Some(n.leafName)
      case _ =>
        None

  /** Convert a definition node's span into a 1-based [[DefinitionResult]]. */
  private def toResult(node: SyntaxTreeNode, sourceFile: SourceFile): DefinitionResult =
    val span = node.span
    DefinitionResult(
      startLine = sourceFile.offsetToLine(span.start) + 1,
      startColumn = sourceFile.offsetToColumn(span.start),
      endLine = sourceFile.offsetToLine(span.end) + 1,
      endColumn = sourceFile.offsetToColumn(span.end)
    )

end DefinitionProvider
