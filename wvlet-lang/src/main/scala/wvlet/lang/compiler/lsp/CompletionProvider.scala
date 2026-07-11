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
import wvlet.lang.compiler.typer.BuiltinFunctions
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
    * Extract completion items for the definitions (models, types, vals, flows, ...) declared at the
    * top level of the given plan.
    *
    * This intentionally recurses only into `PackageDef` containers instead of walking the whole
    * tree with `collectAllNodes`: definitions nested inside model/type bodies are not referenceable
    * from other top-level statements, so surfacing them would only add noise (and a full-tree walk
    * would also visit every expression node of the parse-only plan).
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
    * relation encloses the cursor (e.g. a trailing space or newline at the end of the file), the
    * relation nearest to the cursor is used as a fallback so that suggestions come from the query
    * being edited rather than an unrelated definition elsewhere in the file.
    */
  def columnItems(plan: LogicalPlan, offset: Int): List[CompletionItem] =
    val relations = plan
      .collectAllNodes
      .collect {
        case r: Relation if r.span.exists =>
          r
      }
    if relations.isEmpty then
      Nil
    else
      val enclosing = relations
        .filter(_.span.containsInclusive(offset))
        .sortBy(_.span.size)
        .headOption
        .getOrElse {
          // Nearest relation by span distance to the cursor
          relations.minBy { r =>
            if offset < r.span.start then
              r.span.start - offset
            else if offset > r.span.end then
              offset - r.span.end
            else
              0
          }
        }
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

  end columnItems

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
    dotContextAt(content, offset) match
      case Some(dotCtx) =>
        // A member-access context (`alias.`, `schema.`) returns only the members of the
        // qualifier, so typing `.` does not pop the full keyword list
        dedup(memberItems(content, offset, dotCtx, compiler))
      case None =>
        dedup(generalItems(content, offset, compiler))

  private def generalItems(content: String, offset: Int, compiler: Compiler): List[CompletionItem] =
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

    // Tier 3: column names via a best-effort full typing pass, plus definitions from the
    // other workspace files parsed during the same pass (models, table types from an
    // imported catalog, functions)
    val typedUnit = CompilationUnit.fromWvletString(content)
    try
      compiler.compileSingleUnit(typedUnit)
      val plan = typedUnit.resolvedPlan
      if plan.nonEmpty then
        items ++= columnItems(plan, offset)
      items ++= workspaceDefinitionItems(compiler)
    catch
      case _: Throwable =>
      // Ignore typing errors — column completion is opportunistic
    finally
      // Evict the transient snapshot so repeated requests on a long-lived compiler do not
      // accumulate stale symbols that shadow the workspace files
      compiler.releaseUnit(typedUnit)

    // Tier 4: well-known SQL function names
    items ++= builtinFunctionItems

    items.result()

  end generalItems

  /**
    * Definitions declared in the workspace source files (already parsed by the typing pass), e.g.
    * models and schema-bound table types generated by `wvlet catalog import`
    */
  private def workspaceDefinitionItems(compiler: Compiler): List[CompletionItem] =
    try
      compiler
        .compilationUnitsInSourcePaths
        .filterNot(_.isPreset)
        .flatMap(unit => definitionItems(unit.unresolvedPlan))
    catch
      case _: Throwable =>
        Nil

  private lazy val builtinFunctionItems: List[CompletionItem] = BuiltinFunctions
    .allFunctionNames
    .toList
    .sorted
    .map(name => CompletionItem(name, CompletionItemKind.Function, "function"))

  /**
    * The member-access context of the cursor: the identifier chain ending at a `.` right before the
    * cursor (optionally followed by a partially typed member name)
    *
    * @param qualifier
    *   The identifier chain before the dot, e.g. `List("sales")` for `from sales.`
    * @param prefix
    *   The partially typed member name after the dot (may be empty)
    * @param dotOffset
    *   0-based offset of the `.` that ends the qualifier
    */
  private[lsp] case class DotContext(qualifier: List[String], prefix: String, dotOffset: Int)

  /**
    * Detect a member-access context at the cursor. Returns None when the cursor is not right after
    * `qualifier.[prefix]`, or for decimal literals like `1.5`
    */
  private[lsp] def dotContextAt(content: String, offset: Int): Option[DotContext] =
    def isIdentChar(c: Char): Boolean = c.isLetterOrDigit || c == '_'
    val at                            = math.min(math.max(offset, 0), content.length)
    // Scan back over the partially typed member name
    var i = at
    while i > 0 && isIdentChar(content(i - 1)) do
      i -= 1
    if i == 0 || content(i - 1) != '.' then
      None
    else
      val prefix    = content.substring(i, at)
      val dotOffset = i - 1
      // Collect the identifier chain before the dot
      var parts = List.empty[String]
      var j     = dotOffset
      var valid = true
      while valid && j >= 0 && content(j) == '.' do
        var start = j
        while start > 0 && isIdentChar(content(start - 1)) do
          start -= 1
        if start == j then
          // Nothing before the dot (e.g. `..`)
          valid = false
        else
          parts = content.substring(start, j) :: parts
          j = start - 1
      if !valid || parts.isEmpty || parts.forall(_.forall(_.isDigit)) then
        None
      else
        Some(DotContext(parts, prefix, dotOffset))

  end dotContextAt

  /**
    * Completion candidates for a member-access context: the columns of the aliased or scanned
    * relation named by the qualifier, the tables of a schema the qualifier names, or the columns of
    * the relation enclosing the cursor as a fallback
    */
  private def memberItems(
      content: String,
      offset: Int,
      dotCtx: DotContext,
      compiler: Compiler
  ): List[CompletionItem] =
    // Blank out the incomplete member access so the rest of the query types normally
    val end     = math.min(dotCtx.dotOffset + 1 + dotCtx.prefix.length, content.length)
    val cleaned =
      content.substring(0, dotCtx.dotOffset) +
        (" " * (end - dotCtx.dotOffset)) + content.substring(end)

    val typedUnit = CompilationUnit.fromWvletString(cleaned)
    try
      compiler.compileSingleUnit(typedUnit)
      val plan      = typedUnit.resolvedPlan
      val qualifier = dotCtx.qualifier.last
      val columns   =
        if plan.isEmpty then
          Nil
        else
          relationFieldsFor(plan, qualifier)
      if columns.nonEmpty then
        columns
      else
        val tables = boundTableItems(compiler, typedUnit, dotCtx.qualifier)
        if tables.nonEmpty then
          tables
        else if plan.nonEmpty then
          // Fallback: columns of the relation enclosing the cursor (also covers `_.`)
          columnItems(plan, offset)
        else
          Nil
    catch
      case _: Throwable =>
        Nil
    finally
      compiler.releaseUnit(typedUnit)

  end memberItems

  /**
    * The columns of the relation that the given name refers to in the plan: a relation alias (`from
    * orders as o ... o.`), or a scanned table/model name
    */
  private def relationFieldsFor(plan: LogicalPlan, name: String): List[CompletionItem] =
    val nodes  = plan.collectAllNodes
    val fields = nodes
      .collectFirst {
        case a: AliasedRelation if a.alias.leafName == name =>
          a.relationType.fields
      }
      .orElse {
        nodes.collectFirst {
          case t: TableScan if t.name.name == name =>
            t.columns
          case m: ModelScan if m.name.name == name =>
            m.relationType.fields
        }
      }
    fields
      .getOrElse(Nil)
      .filter(f => !f.name.isEmpty && f.name.name != "<NoName>")
      .map(f => CompletionItem(f.name.name, CompletionItemKind.Field, f.dataType.typeName.name))
      .distinct

  /**
    * The table types bound to the schema (or catalog.schema) named by the qualifier, collected from
    * the current document and the workspace sources (e.g. `catalog/` files generated by
    * `wvlet catalog import`)
    */
  private def boundTableItems(
      compiler: Compiler,
      currentUnit: CompilationUnit,
      qualifier: List[String]
  ): List[CompletionItem] =
    def nameParts(e: wvlet.lang.model.expr.Expression): List[String] =
      e match
        case i: wvlet.lang.model.expr.Identifier =>
          List(i.leafName)
        case wvlet.lang.model.expr.DotRef(q, name: wvlet.lang.model.expr.Identifier, _, _) =>
          nameParts(q) match
            case Nil =>
              Nil
            case parts =>
              parts :+ name.leafName
        case _ =>
          Nil
    def bindingMatches(contextParts: List[String]): Boolean =
      contextParts match
        case catalog :: schema :: Nil =>
          qualifier match
            case s :: Nil =>
              s.equalsIgnoreCase(schema)
            case c :: s :: Nil =>
              c.equalsIgnoreCase(catalog) && s.equalsIgnoreCase(schema)
            case _ =>
              false
        case _ =>
          false
    def tableTypes(plan: LogicalPlan): List[CompletionItem] =
      val buf                        = List.newBuilder[CompletionItem]
      def loop(p: LogicalPlan): Unit =
        p match
          case pkg: PackageDef =>
            pkg.statements.foreach(loop)
          case t: TypeDef if t.defContexts.exists(d => bindingMatches(nameParts(d.contextType))) =>
            buf += CompletionItem(t.name.name, CompletionItemKind.Struct, "table")
          case _ =>
      loop(plan)
      buf.result()
    try
      val workspaceUnits = compiler.compilationUnitsInSourcePaths.filterNot(_.isPreset)
      (currentUnit :: workspaceUnits).flatMap(u => tableTypes(u.resolvedPlan))
    catch
      case _: Throwable =>
        Nil

  end boundTableItems

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
