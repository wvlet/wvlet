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
import wvlet.lang.compiler.MethodSymbolInfo
import wvlet.lang.compiler.ModelSymbolInfo
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.analyzer.FunctionInliner
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.RelationType
import wvlet.lang.model.SyntaxTreeNode
import wvlet.lang.model.expr.NameExpr
import wvlet.lang.model.plan.*

/**
  * The result of a hover request: markdown text to render plus the 1-based source range of the
  * hovered node, so the editor can underline exactly the token the information describes.
  *
  * @param content
  *   Markdown text (typically a fenced ```wvlet code block) describing the hovered node
  * @param startLine
  *   1-based line of the range start
  * @param startColumn
  *   1-based column of the range start
  * @param endLine
  *   1-based line of the range end
  * @param endColumn
  *   1-based column of the range end
  */
case class HoverResult(
    content: String,
    startLine: Int,
    startColumn: Int,
    endLine: Int,
    endColumn: Int
)

/**
  * Provides hover (type information / model signature) tooltips for the Wvlet language.
  *
  * Like [[CompletionProvider]], this lives in the cross-built `wvlet-lang` module so it runs on
  * both the JVM and Scala.js and can be unit tested on the JVM. Hover requests fire on possibly
  * incomplete source, so the best-effort compile is wrapped defensively: any failure yields `None`
  * rather than throwing.
  *
  * The information shown depends on the innermost typed node under the cursor:
  *   - A reference to (or definition of) a model shows the model signature: its name, parameters,
  *     and output schema.
  *   - A resolved column/attribute reference shows `name: type`.
  *   - A relation step (from / where / select) shows its output schema.
  *   - A type definition shows its declared fields.
  *
  * When no node under the cursor carries resolved type information, `None` is returned so the
  * editor shows no noisy fallback tooltip.
  */
object HoverProvider:

  /** Maximum number of schema columns shown before the list is truncated. */
  private val MaxSchemaColumns: Int = 30

  /**
    * Compute the hover result for the given source at the given character offset.
    *
    * This never throws: parsing and typing are attempted defensively so that a partially written
    * query does not surface an error to the editor.
    *
    * @param content
    *   The full Wvlet source text
    * @param offset
    *   0-based character offset of the cursor
    * @param compiler
    *   A compiler used for the best-effort full typing pass that resolves types and symbols
    */
  def hover(content: String, offset: Int, compiler: Compiler): Option[HoverResult] =
    // A member name of a value.function call (e.g. the `upper` of `name.upper`) hovers the
    // stdlib signatures and docs. This runs on a parse-only pass first: after typing, stdlib
    // calls are inlined into SQL fragments and their reference nodes no longer exist. Member
    // names not defined in the stdlib then try the user-defined method members (#2018)
    stdlibFunctionHover(content, offset)
      .orElse(userMethodHover(content, offset, compiler))
      .orElse(typedHover(content, offset, compiler))

  private def typedHover(content: String, offset: Int, compiler: Compiler): Option[HoverResult] =
    val unit = CompilationUnit.fromWvletString(content)
    try
      compiler.compileSingleUnit(unit)
      val plan = unit.resolvedPlan
      if plan.isEmpty then
        None
      else
        val sourceFile = unit.sourceFile
        sourceFile.ensureLoaded
        // Candidate nodes covering the cursor, innermost (smallest span) first
        val candidates = plan
          .collectAllNodes
          .filter(n => n.span.exists && n.span.containsInclusive(offset))
          .sortBy(_.span.size)
        candidates
          .iterator
          .flatMap { n =>
            // Rendering a single candidate may fail while resolving its type on an
            // incomplete/malformed AST; skip it so an enclosing candidate can still hover
            try
              markdownFor(n).map(md => toResult(md, n, sourceFile))
            catch
              case _: Throwable =>
                None
          }
          .nextOption()
    catch
      case _: Throwable =>
        // Hover is opportunistic: never surface a compile error to the editor
        None
    finally
      // Evict the transient snapshot so repeated requests on a long-lived compiler do not
      // accumulate stale symbols that shadow the workspace files
      compiler.releaseUnit(unit)
    end try

  end typedHover

  /**
    * Hover result for a stdlib function reference: the member name of a `value.function` access
    * (e.g. `upper` in `name.upper`) or the base name of a top-level call (e.g. `row_number()`).
    * Shows every overload's signature with its doc comment, grouped by the owning type.
    *
    * Fires only when the hovered name is defined in the standard library and the qualifier is not a
    * relation alias or table name (so `t.year` on an alias hovers the column, not the function)
    */
  private def stdlibFunctionHover(content: String, offset: Int): Option[HoverResult] =
    try
      val unit = CompilationUnit.fromWvletString(content)
      val plan = wvlet.lang.compiler.parser.ParserPhase.parseOnly(unit)
      val sf   = unit.sourceFile
      sf.ensureLoaded
      val nodes = plan.collectAllNodes

      // Relation aliases and scanned table names of the file: member access through them is
      // column access, not a stdlib function call
      val relationNames: Set[String] =
        nodes
          .collect {
            case a: AliasedRelation =>
              a.alias.leafName.toLowerCase
            case t: TableRef =>
              t.name.fullName.toLowerCase
          }
          .toSet

      def qualifierName(e: wvlet.lang.model.expr.Expression): String =
        e match
          case n: NameExpr =>
            n.leafName.toLowerCase
          case _ =>
            ""

      // The member name of a DotRef under the cursor (e.g. `upper` of name.upper)
      val memberHit =
        nodes
          .collect {
            case d: wvlet.lang.model.expr.DotRef
                if d.name.span.exists && d.name.span.containsInclusive(offset) &&
                  !relationNames.contains(qualifierName(d.qualifier)) =>
              d.name
          }
          .headOption
      // The base name of a top-level function call under the cursor (e.g. row_number())
      val topLevelHit =
        nodes
          .collect { case f: wvlet.lang.model.expr.FunctionApply =>
            f.base match
              case i: wvlet.lang.model.expr.Identifier
                  if i.span.exists && i.span.containsInclusive(offset) &&
                    StdLibIndex.topLevelFunctionsByName.contains(i.leafName.toLowerCase) =>
                Some(i)
              case _ =>
                None
          }
          .flatten
          .headOption

      memberHit
        .orElse(topLevelHit)
        .flatMap { name =>
          val docs = StdLibIndex.functionsNamed(name.leafName)
          if docs.isEmpty then
            None
          else
            Some(toResult(stdlibMarkdown(docs), name, sf))
        }
    catch
      case _: Throwable =>
        // Hover is opportunistic: never surface a parse error to the editor
        None

  end stdlibFunctionHover

  /**
    * Hover result for a user-defined method reference (#2018): the member name of a `value.method`
    * access where the method is a `def` declared in a user `table` / `trait` / `type` body, own or
    * mixed in through `extends` parents. The definition is found with the same hierarchy walk the
    * compiler uses to resolve method calls. The member location comes from a parse-only pass (typed
    * plans inline user method calls, so their reference nodes no longer exist), then a typed pass
    * resolves the qualifier's declared type
    */
  private def userMethodHover(
      content: String,
      offset: Int,
      compiler: Compiler
  ): Option[HoverResult] =
    try
      val unit       = CompilationUnit.fromWvletString(content)
      val parsedPlan = wvlet.lang.compiler.parser.ParserPhase.parseOnly(unit)
      val sf         = unit.sourceFile
      sf.ensureLoaded
      val memberHit =
        parsedPlan
          .collectAllNodes
          .collect {
            case d: wvlet.lang.model.expr.DotRef
                if d.name.span.exists && d.name.span.containsInclusive(offset) =>
              (d.qualifier, d.name)
          }
          .headOption
      memberHit.flatMap { (qualifier, name) =>
        val qualifierName =
          qualifier match
            case n: NameExpr =>
              n.leafName
            case _ =>
              ""
        if qualifierName.isEmpty then
          None
        else
          val typedUnit = CompilationUnit.fromWvletString(content)
          try
            val result = compiler.compileSingleUnit(typedUnit)
            val plan   = typedUnit.resolvedPlan
            if plan.isEmpty then
              None
            else
              CompletionProvider
                .qualifierTypeName(plan, qualifierName, offset)
                .flatMap { t =>
                  val methods = FunctionInliner
                    .memberMethodsInHierarchy(Name.typeName(t), result.context)
                    .filter(m => m.name.name.equalsIgnoreCase(name.leafName))
                    // Stdlib members are rendered with their docs by stdlibFunctionHover
                    .filterNot(_.compilationUnit.isPreset)
                  if methods.isEmpty then
                    None
                  else
                    Some(toResult(userMethodMarkdown(methods), name, sf))
                }
          finally
            compiler.releaseUnit(typedUnit)
      }
    catch
      case _: Throwable =>
        // Hover is opportunistic: never surface a parse or compile error to the editor
        None

  end userMethodHover

  /**
    * Render user-defined method definitions as markdown: a fenced code block with the owning type
    * and the distinct signatures of the definition's variants
    */
  private def userMethodMarkdown(methods: List[MethodSymbolInfo]): String =
    val header   = s"-- member of ${methods.head.owner.name.name}"
    val sigLines =
      methods.map(m => s"def ${m.name.name}${CompletionProvider.methodSignatureOf(m)}").distinct
    codeBlock((header :: sigLines).mkString("\n"))

  /** Maximum number of owning-type groups rendered in a stdlib function hover */
  private val MaxHoverGroups: Int = 3

  /**
    * Render stdlib function docs as markdown: for each owning type, a fenced code block listing the
    * distinct overload signatures, followed by the function's doc comment
    */
  private def stdlibMarkdown(docs: List[StdLibFunctionDoc]): String =
    val groups = docs
      .groupBy(_.ownerType)
      .toList
      .sortBy((owner, _) => docs.indexWhere(_.ownerType == owner))
    val rendered = groups
      .take(MaxHoverGroups)
      .map { case (owner, defs) =>
        val header =
          if owner.isEmpty then
            "-- function"
          else
            s"-- member of ${owner}"
        // Merge dialect variants of one signature into a single line
        val sigLines =
          defs
            .map { d =>
              val ret =
                if d.returnType.nonEmpty then
                  s": ${d.returnType}"
                else
                  ""
              s"def ${d.signature}${ret}"
            }
            .distinct
        // A dialect variant's comment explains that engine's quirk; prefer the neutral doc
        val desc = defs
          .filter(_.dialects.isEmpty)
          .map(_.description)
          .find(_.nonEmpty)
          .orElse(defs.map(_.description).find(_.nonEmpty))
          .getOrElse("")
        val block = codeBlock((header :: sigLines).mkString("\n"))
        if desc.isEmpty then
          block
        else
          s"${block}\n${desc}"
      }
    val more =
      if groups.size > MaxHoverGroups then
        s"\n\n… ${groups.size - MaxHoverGroups} more"
      else
        ""
    rendered.mkString("\n\n") + more

  end stdlibMarkdown

  /**
    * Build the markdown description for a single node, or `None` if the node carries no useful type
    * information. Node kinds are tried from the most specific (model / type definitions) to the
    * most general (any typed name or relation).
    */
  private def markdownFor(node: SyntaxTreeNode): Option[String] =
    node match
      case m: ModelScan =>
        Some(modelMarkdown(m.name.name, Nil, m.relationType))
      case m: ModelDef =>
        Some(modelMarkdown(m.name.name, m.params, m.relationType))
      case t: TypeDef =>
        Some(typeMarkdown(t))
      case e: NameExpr =>
        modelReferenceMarkdown(e).orElse(columnMarkdown(e))
      case r: Relation =>
        schemaMarkdown(r.relationType).map(body => codeBlock(body))
      case _ =>
        None

  /**
    * If the given name expression resolves to a model symbol, render the model signature. Otherwise
    * `None`.
    */
  private def modelReferenceMarkdown(e: NameExpr): Option[String] =
    try
      e.symbol.symbolInfo match
        case msi: ModelSymbolInfo =>
          msi.dataType match
            case rt: RelationType =>
              Some(modelMarkdown(e.leafName, Nil, rt))
            case _ =>
              None
        case _ =>
          None
    catch
      case _: Throwable =>
        None

  /**
    * Render `name: type` for a name expression that has a resolved, non-relation data type (i.e. a
    * column or scalar attribute reference).
    */
  private def columnMarkdown(e: NameExpr): Option[String] =
    val dt = e.dataType
    if dt.isResolved && !dt.isRelationType && dt != DataType.UnknownType then
      Some(codeBlock(s"${e.leafName}: ${dt.typeDescription}"))
    else
      None

  /** Render a model signature: header (name + parameters) followed by the output schema. */
  private def modelMarkdown(name: String, params: List[DefArg], rt: RelationType): String =
    val header =
      if params.isEmpty then
        s"model ${name}"
      else
        val paramStr = params
          .map(p => s"${p.name.name}: ${p.givenDataType.typeDescription}")
          .mkString(", ")
        s"model ${name}(${paramStr})"
    val fields = schemaLines(rt)
    val body   =
      if fields.isEmpty then
        ""
      else
        fields.mkString("\n", "\n", "")
    codeBlock(s"${header}${body}")

  /** Render a type definition: its name followed by its declared fields. */
  private def typeMarkdown(t: TypeDef): String =
    val fields = t
      .elems
      .collect { case f: FieldDef =>
        s"  ${f.name.name}: ${f.fieldType.fullName}"
      }
    val body =
      if fields.isEmpty then
        ""
      else
        fields.mkString("\n", "\n", "")
    codeBlock(s"type ${t.name.name}${body}")

  /**
    * Render just the schema (column: type lines) of a relation type, or `None` if it has no named
    * fields.
    */
  private def schemaMarkdown(rt: RelationType): Option[String] =
    val lines = schemaLines(rt)
    if lines.isEmpty then
      None
    else
      Some(lines.mkString("\n"))

  /**
    * Produce the indented `column: type` lines for a relation type, dropping unnamed columns and
    * truncating over [[MaxSchemaColumns]] with a trailing "… N more" line.
    */
  private def schemaLines(rt: RelationType): List[String] =
    val fields =
      try
        rt.fields
      catch
        case _: Throwable =>
          Nil
    val named = fields.filter(f => !f.name.isEmpty && f.name.name != "<NoName>")
    val shown = named
      .take(MaxSchemaColumns)
      .map(f => s"  ${f.name.name}: ${f.dataType.typeDescription}")
    if named.size > MaxSchemaColumns then
      shown :+ s"  … ${named.size - MaxSchemaColumns} more"
    else
      shown

  /** Wrap the given body in a fenced ```wvlet code block. */
  private def codeBlock(body: String): String = s"```wvlet\n${body}\n```"

  /** Convert a markdown body and the hovered node's span into a 1-based [[HoverResult]]. */
  private def toResult(
      markdown: String,
      node: SyntaxTreeNode,
      sourceFile: wvlet.lang.compiler.SourceFile
  ): HoverResult =
    val span = node.span
    HoverResult(
      content = markdown,
      startLine = sourceFile.offsetToLine(span.start) + 1,
      startColumn = sourceFile.offsetToColumn(span.start),
      endLine = sourceFile.offsetToLine(span.end) + 1,
      endColumn = sourceFile.offsetToColumn(span.end)
    )

end HoverProvider
