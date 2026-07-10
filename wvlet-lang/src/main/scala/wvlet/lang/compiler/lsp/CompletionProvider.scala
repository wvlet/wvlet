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
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.compiler.parser.WvletToken
import wvlet.lang.model.SyntaxTreeNode
import wvlet.lang.model.plan.*

/**
  * A single completion candidate returned to an editor / language server client.
  *
  * @param label
  *   The text inserted when the item is selected (e.g. a keyword, model name, or column name)
  * @param kind
  *   The LSP `CompletionItemKind` numeric value (see [[CompletionItemKind]])
  * @param detail
  *   A short human-readable description shown next to the label (e.g. a data type or "model")
  */
case class CompletionItem(label: String, kind: Int, detail: String)

/**
  * LSP `CompletionItemKind` numeric values used by Wvlet completions. Only the subset relevant to
  * Wvlet is defined here. See the LSP specification for the full enumeration.
  */
object CompletionItemKind:
  val Field: Int    = 5
  val Variable: Int = 6
  val Class: Int    = 7
  val Function: Int = 3
  val Keyword: Int  = 14
  val Struct: Int   = 22

/**
  * Provides code completion candidates for the Wvlet language.
  *
  * The provider is deliberately platform independent (it lives in the cross-built `wvlet-lang`
  * module) so it can run on both the JVM and Scala.js, and be unit tested on the JVM. It never
  * throws for malformed or incomplete input: each completion tier is attempted independently and
  * degrades gracefully so that at least keyword candidates are always returned.
  *
  * The candidates are produced in three tiers:
  *   1. Keywords — always available.
  *   1. In-file definitions (model / type / val / flow / partial-query names) — extracted with a
  *      parse-only pass that tolerates parse errors.
  *   1. Column names of the query relation enclosing the cursor — extracted after best-effort full
  *      typing.
  */
object CompletionProvider:

  /**
    * Keyword candidates. These are always available regardless of the cursor position.
    */
  def keywordItems: List[CompletionItem] =
    WvletToken
      .keywords
      .map(k => CompletionItem(k.str, CompletionItemKind.Keyword, "keyword"))
      .toList

  /**
    * Find the innermost syntax tree node whose span contains the given character offset.
    *
    * The innermost node is the one with the smallest containing span. Returns None if no node
    * covers the offset (e.g. the offset points into trailing whitespace of an incomplete input).
    *
    * @param plan
    *   The (unresolved or resolved) plan to search
    * @param offset
    *   0-based character offset into the source
    */
  def nodeAt(plan: LogicalPlan, offset: Int): Option[SyntaxTreeNode] =
    plan.collectAllNodes.filter(_.span.containsInclusive(offset)).sortBy(_.span.size).headOption

  /**
    * Extract completion items for the definitions (models, types, vals, flows, ...) declared in the
    * given plan.
    */
  def definitionItems(plan: LogicalPlan): List[CompletionItem] =
    val buf                        = List.newBuilder[CompletionItem]
    def loop(p: LogicalPlan): Unit =
      p match
        case pkg: PackageDef =>
          pkg.statements.foreach(loop)
        case m: ModelDef =>
          buf += CompletionItem(m.name.fullName, CompletionItemKind.Class, "model")
        case t: TypeDef =>
          buf += CompletionItem(t.name.name, CompletionItemKind.Struct, "type")
        case f: TopLevelFunctionDef =>
          buf += CompletionItem(f.functionDef.name.name, CompletionItemKind.Function, "function")
        case v: ValDef =>
          buf += CompletionItem(v.name.name, CompletionItemKind.Variable, "val")
        case p: PartialQueryDef =>
          buf += CompletionItem(p.name.name, CompletionItemKind.Function, "query")
        case fl: FlowDef =>
          buf += CompletionItem(fl.name.name, CompletionItemKind.Function, "flow")
        case _ =>
    loop(plan)
    buf.result()

  end definitionItems

  /**
    * Extract column completion items visible at the given cursor offset. The columns come from the
    * input and output schema of the query relation that most tightly encloses the cursor; when no
    * relation encloses the cursor (e.g. an incomplete pipeline), the widest relation in the plan is
    * used as a fallback.
    */
  def columnItems(plan: LogicalPlan, offset: Int): List[CompletionItem] =
    val relations = plan
      .collectAllNodes
      .collect { case r: Relation =>
        r
      }
    if relations.isEmpty then
      Nil
    else
      val enclosing = relations
        .filter(_.span.containsInclusive(offset))
        .sortBy(_.span.size)
        .headOption
        .getOrElse(relations.maxBy(_.span.size))
      val fields =
        try
          enclosing.inputRelationType.fields ++ enclosing.relationType.fields
        catch
          case _: Throwable =>
            Nil
      fields
        .filter(f => !f.name.isEmpty && f.name.name != "<NoName>")
        .map(f => CompletionItem(f.name.name, CompletionItemKind.Field, f.dataType.typeName.name))
        .distinct

  /**
    * Compute completion candidates for the given source at the given character offset.
    *
    * This is the main entry point. It never throws: parsing and typing are attempted defensively so
    * that partially written queries (e.g. a trailing `from ` or `select foo.`) still yield useful
    * results.
    *
    * @param content
    *   The full Wvlet source text
    * @param offset
    *   0-based character offset of the cursor
    * @param compiler
    *   A compiler used for the best-effort full typing pass that resolves column names
    */
  def complete(content: String, offset: Int, compiler: Compiler): List[CompletionItem] =
    val items = List.newBuilder[CompletionItem]

    // Tier 1: keywords (always)
    items ++= keywordItems

    // Tier 2: in-file definitions via a parse-only pass (resilient to parse errors)
    val parseUnit = CompilationUnit.fromWvletString(content)
    try
      parseUnit.unresolvedPlan = ParserPhase.parseOnly(parseUnit)
      items ++= definitionItems(parseUnit.unresolvedPlan)
    catch
      case _: Throwable =>
      // Ignore parse errors — offer whatever we could extract

    // Tier 3: column names via a best-effort full typing pass
    try
      val typedUnit = CompilationUnit.fromWvletString(content)
      compiler.compileSingleUnit(typedUnit)
      val plan = typedUnit.resolvedPlan
      if plan.nonEmpty then
        items ++= columnItems(plan, offset)
    catch
      case _: Throwable =>
      // Ignore typing errors — column completion is opportunistic

    dedup(items.result())

  end complete

  /**
    * Remove duplicate candidates, keeping the first occurrence of each label. Column and definition
    * candidates (added later) therefore never shadow one another, and keyword duplicates are
    * dropped.
    */
  private def dedup(items: List[CompletionItem]): List[CompletionItem] =
    val seen   = scala.collection.mutable.HashSet.empty[String]
    val result = List.newBuilder[CompletionItem]
    items.foreach { item =>
      if seen.add(item.label) then
        result += item
    }
    result.result()

end CompletionProvider
