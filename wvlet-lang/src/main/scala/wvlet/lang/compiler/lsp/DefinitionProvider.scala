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
import wvlet.lang.compiler.LocalFile
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.model.SyntaxTreeNode
import wvlet.lang.model.expr.NameExpr
import wvlet.lang.model.plan.*

/**
  * The result of a go-to-definition request: the 1-based source range of the definition the cursor
  * resolves to. When `path` is empty, the definition is in the requested document itself; when set,
  * it is the local file path of the workspace file that contains the definition, so the editor can
  * navigate across files.
  *
  * @param startLine
  *   1-based line of the definition range start
  * @param startColumn
  *   1-based column of the definition range start
  * @param endLine
  *   1-based line of the definition range end
  * @param endColumn
  *   1-based column of the definition range end
  * @param path
  *   Path of the file containing the definition, or `None` when the definition is in the requested
  *   document
  */
case class DefinitionResult(
    startLine: Int,
    startColumn: Int,
    endLine: Int,
    endColumn: Int,
    path: Option[String] = None
)

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
  *   - The resolved symbol chain: when a reference node carries a symbol whose `tree` is the
  *     defining `ModelDef`/`TypeDef`, the symbol also records its defining compilation unit.
  *   - A name lookup: the referenced identifier is matched against the top-level `model`/`type`
  *     names of the current document (collected from a parse-only pass, so navigation keeps working
  *     when full typing fails) and then against those of the other workspace units loaded by the
  *     compiler.
  *
  * The document is compiled together with the other workspace sources (including the `catalog/`
  * folder), so a definition may live in another file: such results carry the defining file's path.
  * Current-document definitions always take precedence, matching the compiler's shadowing rule for
  * duplicate top-level names.
  */
object DefinitionProvider:

  /**
    * A top-level model or type definition, paired with the compilation unit that contains it so
    * that cross-file targets can report the defining file
    */
  private case class DefinitionEntry(name: String, node: SyntaxTreeNode, unit: CompilationUnit)

  /** A resolved definition node paired with the compilation unit that defines it. */
  private case class DefinitionTarget(node: SyntaxTreeNode, unit: CompilationUnit)

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
    val unit = CompilationUnit.fromWvletString(content)
    try
      // Collect top-level model/type definitions of the current document with a parse-only pass
      // (resilient to parse errors)
      val localDefinitions =
        try
          unit.unresolvedPlan = ParserPhase.parseOnly(unit)
          collectDefinitions(unit.unresolvedPlan, unit)
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

      // Current-document definitions come first so they shadow same-named workspace definitions,
      // matching the compiler's duplicate-name resolution
      val definitions = localDefinitions ++ workspaceDefinitions(compiler, unit)

      val searchPlan =
        if unit.resolvedPlan.nonEmpty then
          unit.resolvedPlan
        else
          unit.unresolvedPlan
      if searchPlan.isEmpty then
        None
      else
        unit.sourceFile.ensureLoaded
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
              definitionFor(n, definitions, offset, unit).flatMap(t => toResult(t, unit))
            catch
              case _: Throwable =>
                None
          }
          .nextOption()
    catch
      case _: Throwable =>
        // Go-to-definition is opportunistic: never surface a compile error to the editor
        None
    finally
      // Evict the transient snapshot so repeated requests on a long-lived compiler do not
      // accumulate stale symbols that shadow the workspace files
      compiler.releaseUnit(unit)
    end try

  end definition

  /**
    * Collect the top-level `model` and `type` definitions of the given plan. Like
    * [[CompletionProvider.definitionItems]], this recurses only into `PackageDef` containers, since
    * only top-level definitions are referenceable from other statements.
    */
  private def collectDefinitions(plan: LogicalPlan, unit: CompilationUnit): List[DefinitionEntry] =
    val buf                        = List.newBuilder[DefinitionEntry]
    def loop(p: LogicalPlan): Unit =
      p match
        case pkg: PackageDef =>
          pkg.statements.foreach(loop)
        case m: ModelDef =>
          buf += DefinitionEntry(m.name.name, m, unit)
        case t: TypeDef =>
          buf += DefinitionEntry(t.name.name, t, unit)
        case _ =>
    loop(plan)
    buf.result()

  /**
    * Collect the top-level `model`/`type` definitions of the workspace units loaded by the
    * compiler, so that references resolve across files. Preset units (the built-in stdlib) are
    * skipped: their in-memory sources have no file the editor could open. A workspace unit that has
    * not been compiled yet (e.g. when the best-effort compile aborted early) is parsed on the fly,
    * and the plan is cached back into the unit — the same assignment [[ParserPhase.run]] performs —
    * so repeated definition requests do not re-parse it. A later compile still re-parses the unit
    * (the parser phase is not marked finished), and [[CompilationUnit.reload]] discards the cached
    * plan when the file changes.
    */
  private def workspaceDefinitions(
      compiler: Compiler,
      currentUnit: CompilationUnit
  ): List[DefinitionEntry] = compiler
    .compilationUnitsInSourcePaths
    .filter(u => !u.isPreset && !(u.sourceFile eq currentUnit.sourceFile))
    .flatMap { u =>
      try
        val plan =
          if u.resolvedPlan.nonEmpty then
            u.resolvedPlan
          else if u.unresolvedPlan.nonEmpty then
            u.unresolvedPlan
          else
            u.unresolvedPlan = ParserPhase.parseOnly(u, isContextUnit = false)
            u.unresolvedPlan
        collectDefinitions(plan, u)
      catch
        case _: Throwable =>
          // A workspace file that fails to parse contributes no definitions
          Nil
    }

  /**
    * Resolve the defining `ModelDef`/`TypeDef` for a single candidate node, or `None` if the node
    * is not a model/type reference. The resolved symbol's definition tree is preferred; the
    * collected definition names (current document first, then the other workspace files) are used
    * as a fallback when typing did not resolve the symbol. Either way the definition may live in
    * another workspace file.
    *
    * A same-document definition whose span contains the cursor is treated as "already at the
    * definition" and yields `None`, so navigating from the definition itself (or a keyword/name
    * within it) does not jump to itself. The check is skipped for cross-file targets: their spans
    * index a different file, so containing the cursor offset is coincidental.
    */
  private def definitionFor(
      node: SyntaxTreeNode,
      definitions: List[DefinitionEntry],
      offset: Int,
      currentUnit: CompilationUnit
  ): Option[DefinitionTarget] =
    val bySymbol =
      try
        val sym = node.symbol
        if sym.isDefined then
          sym.tree match
            case d @ (_: ModelDef | _: TypeDef) =>
              // Symbols not tied to a single source definition report CompilationUnit.empty;
              // treat those as defined in the current document
              val definingUnit = sym.compilationUnit
              val unit         =
                if definingUnit.isEmpty then
                  currentUnit
                else
                  definingUnit
              Some(DefinitionTarget(d, unit))
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
          definitions.find(_.name == name).map(d => DefinitionTarget(d.node, d.unit))
        }
      }
      .filter { t =>
        val sameDocument = t.unit.sourceFile eq currentUnit.sourceFile
        t.node.span.exists && !(sameDocument && t.node.span.containsInclusive(offset))
      }

  end definitionFor

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

  /**
    * Convert a definition target's span into a 1-based [[DefinitionResult]], using the defining
    * unit's source file so that cross-file spans map to the correct lines. Cross-file targets carry
    * the defining file's path; targets in a file the editor cannot open (preset stdlib or other
    * in-memory sources) yield `None`.
    */
  private def toResult(
      target: DefinitionTarget,
      currentUnit: CompilationUnit
  ): Option[DefinitionResult] =
    val sourceFile   = target.unit.sourceFile
    val sameDocument = sourceFile eq currentUnit.sourceFile
    val path         =
      if sameDocument then
        // The definition is in the requested document (which may be an unsaved buffer)
        Some(None)
      else
        sourceFile.file match
          case f: LocalFile =>
            Some(Some(f.path))
          case _ =>
            // Preset stdlib or other non-local sources have no file the editor can navigate to
            None
    path.map { p =>
      sourceFile.ensureLoaded
      val span = target.node.span
      DefinitionResult(
        startLine = sourceFile.offsetToLine(span.start) + 1,
        startColumn = sourceFile.offsetToColumn(span.start),
        endLine = sourceFile.offsetToLine(span.end) + 1,
        endColumn = sourceFile.offsetToColumn(span.end),
        path = p
      )
    }

end DefinitionProvider
