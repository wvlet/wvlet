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
package wvlet.lang.compiler.analyzer

import wvlet.lang.compiler.ValSymbolInfo
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.FlowSymbolInfo
import wvlet.lang.compiler.MethodSymbolInfo
import wvlet.lang.compiler.ModelSymbolInfo
import wvlet.lang.compiler.MultipleSymbolInfo
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.PackageSymbolInfo
import wvlet.lang.compiler.PartialQuerySymbolInfo
import wvlet.lang.compiler.Phase
import wvlet.lang.compiler.QuerySymbol
import wvlet.lang.compiler.RelationAliasSymbolInfo
import wvlet.lang.compiler.SavedRelationSymbolInfo
import wvlet.lang.compiler.Scope
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.TermName
import wvlet.lang.compiler.TypeSymbol
import wvlet.lang.compiler.TypeSymbolInfo
import wvlet.lang.compiler.typer.DuplicateTypeDefinition
import wvlet.lang.compiler.typer.ModelDefCompleter
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.Type.FunctionType
import wvlet.lang.model.Type.ImportType
import wvlet.lang.model.Type.PackageType
import wvlet.lang.model.expr.ArrayConstructor
import wvlet.lang.model.expr.DotRef
import wvlet.lang.model.expr.Identifier
import wvlet.lang.model.expr.NameExpr
import wvlet.lang.model.expr.QualifiedName
import wvlet.lang.model.plan.*
import wvlet.lang.model.DataType
import wvlet.lang.model.Type

/**
  * Assign unique Symbol to PackageDef, Import, TypeDef, and ModelDef nodes, and assign a lazy
  * SymbolInfo to them. Registered Symbols are permanent identifiers throughout the compilation
  * phases for looking up their resulting data types.
  */
object SymbolLabeler extends Phase("symbol-labeler"):
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    // Drop any symbols of a previous compilation of this unit (e.g., after a reload) from the
    // global index before the re-labeling below registers the new definitions
    context.global.symbolIndex.remove(unit)
    label(unit.unresolvedPlan, context)
    unit

  private def label(plan: LogicalPlan, context: Context): Unit =
    if context.isContextCompilationUnit then
      debug(s"Labeling symbols for ${plan.pp}")

    def attachNewSymbol(tree: LogicalPlan, ctx: Context): Symbol =
      val sym = Symbol(ctx.global.newSymbolId, tree.span)
      tree.symbol = sym
      sym.tree = tree
      sym

    def iter(tree: LogicalPlan, ctx: Context): Context =
      tree match
        case p: PackageDef =>
          val packageSymbol = registerPackageSymbol(p.name)(using ctx)
          packageSymbol.tree = p
          val packageCtx = ctx.newContext(packageSymbol)
          p.statements
            .foldLeft(packageCtx) { (prevContext, stmt) =>
              iter(stmt, prevContext)
            }
          packageCtx
        case i: Import =>
          val sym = Symbol.newImportSymbol(ctx.owner, i)(using ctx)
          // Attach the import symbol to the Tree node
          i.symbol = sym
          trace(s"Created import symbol for ${i.symbol}")
          ctx.withImport(i)
        case t: TypeDef =>
          registerTypeDefSymbol(t)(using ctx)
          ctx
        case m: ModelDef =>
          registerModelSymbol(m)(using ctx)
          iter(m.child, ctx)
        case f: FlowDef =>
          registerFlowSymbol(f)(using ctx)
          ctx
        case f: TopLevelFunctionDef =>
          registerTopLevelFunction(f)(using ctx)
          ctx
        case p: PartialQueryDef =>
          registerPartialQueryDef(p)(using ctx)
          ctx
        case v: ValDef =>
          val sym = Symbol(ctx.global.newSymbolId, v.span)
          // For table value constants (val t(id, name) = [[...]]), the declared schema has no
          // column types, so refine them from the first row's values
          val declaredType =
            (v.dataType, v.expr) match
              case (s: SchemaType, arr: ArrayConstructor) if !s.isResolved =>
                RelationRefResolver.refineSchemaFromRows(s, arr.values)
              case _ =>
                v.dataType
          sym.symbolInfo = ValSymbolInfo(
            ctx.owner,
            sym,
            v.name,
            declaredType,
            v.expr,
            ctx.compilationUnit
          )
          v.symbol = sym
          sym.tree = v
          ctx.enterGlobalSymbol(sym)
          ctx
        case s: Save if s.isForTable =>
          iter(s.child, ctx)
          registerSave(s)(using ctx)
          ctx
        case d: Debug =>
          warn(d)
          iter(d.debugExpr, ctx)
          ctx
        case stmt: TopLevelStatement =>
          stmt match
            case q: Query =>
              val sym: Symbol = attachNewSymbol(q, ctx)
              sym.symbolInfo = QuerySymbol(
                ctx.owner,
                sym,
                TermName.of(s"__query_${sym.id}"),
                ctx.compilationUnit
              )
              ctx.enterGlobalSymbol(sym)
              q.traverseOnce {
                case s: SelectAsAlias =>
                  iter(s.child, ctx)
                  registerSelectAsAlias(s)(using ctx)
                case d: Debug =>
                  iter(d.debugExpr, ctx)
              }
              ctx
            case c: Command =>
              val sym = attachNewSymbol(c, ctx)
              ctx.enterGlobalSymbol(sym)
              ctx
            case _ =>
              ctx
        case _ =>
          ctx

    iter(plan, context)

  end label

  private def registerTopLevelFunction(t: TopLevelFunctionDef)(using ctx: Context): Symbol =
    val sym = Symbol(ctx.global.newSymbolId, t.span)
    // Keep the def's own contexts (e.g. the `in duckdb` tag of defs generated by
    // `wvlet catalog import`) so the engine provenance stays available on the symbol
    sym.symbolInfo = MethodSymbolInfo(
      owner = ctx.owner,
      symbol = sym,
      name = t.functionDef.name,
      ft = toFunctionType(t.functionDef, t.functionDef.defContexts),
      body = t.functionDef.expr,
      t.functionDef.defContexts,
      ctx.compilationUnit
    )
    t.symbol = sym
    sym.tree = t
    ctx.enterGlobalSymbol(sym)
    sym

  /**
    * Register a partial query definition symbol. Partial queries are reusable query fragments that
    * can be applied to relations via pipe.
    */
  private def registerPartialQueryDef(p: PartialQueryDef)(using ctx: Context): Symbol =
    val partialQueryName = p.name
    ctx.scope.lookupSymbol(partialQueryName) match
      case Some(s) =>
        // Update the existing symbol (for REPL), and re-enter it so the re-labeled unit stays
        // visible to global symbol lookup
        s.tree = p
        s.symbolInfo = PartialQuerySymbolInfo(
          ctx.owner,
          s,
          partialQueryName,
          p.params,
          p.body,
          ctx.compilationUnit
        )
        ctx.enterGlobalSymbol(s)
        p.symbol = s
        trace(s"Updated existing partial query symbol ${s} for ${partialQueryName}")
        s
      case None =>
        val sym = Symbol(ctx.global.newSymbolId, p.span)
        sym.tree = p
        sym.symbolInfo = PartialQuerySymbolInfo(
          ctx.owner,
          sym,
          partialQueryName,
          p.params,
          p.body,
          ctx.compilationUnit
        )
        // Enter after the SymbolInfo assignment so the symbol is indexed with its name
        ctx.enterGlobalSymbol(sym)
        p.symbol = sym
        trace(s"Created new partial query symbol ${sym} for ${partialQueryName}")
        ctx.scope.add(partialQueryName, sym)
        sym
    end match

  end registerPartialQueryDef

  private def registerSelectAsAlias(s: SelectAsAlias)(using ctx: Context): Symbol =
    val aliasName = s.target.toTermName
    val sym       = Symbol(ctx.global.newSymbolId, s.span)
    sym.symbolInfo = RelationAliasSymbolInfo(ctx.owner, sym, aliasName, ctx.compilationUnit)
    s.symbol = sym
    sym.tree = s.child
    ctx.enterGlobalSymbol(sym)
    sym

  private def registerSave(s: Save)(using ctx: Context): Symbol =
    val targetName = Name.termName(s.targetName)
    val sym        = Symbol(ctx.global.newSymbolId, s.span)
    sym.symbolInfo = SavedRelationSymbolInfo(ctx.owner, sym, targetName, ctx.compilationUnit)
    s.symbol = sym
    sym.tree = s.child
    ctx.enterGlobalSymbol(sym)
    sym

  private def registerPackageSymbol(pkgName: NameExpr)(using ctx: Context): Symbol =
    val pkgOwner: Symbol =
      pkgName match
        case DotRef(parent: QualifiedName, _, _, _) =>
          registerPackageSymbol(parent)
        case i: Identifier =>
          ctx.global.defs.RootPackage
        case _ =>
          throw new IllegalArgumentException(s"Invalid package name: ${name}")

    val pkgLeafName = Name.termName(pkgName.leafName)
    val pkgSymbol   =
      pkgOwner.symbolInfo.declScope.lookupSymbol(pkgLeafName) match
        case Some(s) =>
          s
        case None =>
          val sym        = Symbol(ctx.global.newSymbolId, pkgName.span)
          val pkgSymInfo = PackageSymbolInfo(
            pkgOwner,
            sym,
            pkgLeafName,
            PackageType(pkgLeafName),
            // Create a fresh scope for defining global package
            Scope.newScope(0)
          )
          sym.symbolInfo = pkgSymInfo
          ctx.enterGlobalSymbol(sym)
          // pkgOwner.symbolInfo.declScope.add(pkgLeafName, sym)
          trace(s"Created package symbol for ${pkgName.fullName}, owner ${pkgOwner}")
          pkgOwner.symbolInfo.declScope.add(pkgLeafName, sym)
          sym

    pkgSymbol

  end registerPackageSymbol

  private def toFunctionType(f: FunctionDef, defContexts: List[DefContext]): FunctionType =
    FunctionType(
      name = f.name,
      args = f
        .args
        .map { a =>
          val paramName = a.name
          val paramType = a.dataType
          NamedType(paramName, paramType)
        },
      returnType = f.retType.getOrElse(DataType.UnknownType),
      // TODO resolve qualified name
      contextNames = defContexts.map(x => Name.typeName(x.contextType.leafName))
    )

  /**
    * True when two same-name type definitions carry conflicting table bindings: the bindings differ
    * case-insensitively and at least one side is a two-part `<catalog>.<schema>` binding. Unbound
    * duplicates (including single-part dialect scopes like `in duckdb`) merge intentionally and
    * never conflict
    */
  private def bindingsConflict(a: Option[(String, String)], b: Option[(String, String)]): Boolean =
    def norm(x: Option[(String, String)]) = x.map((c, s) => (c.toLowerCase, s.toLowerCase))
    (a.isDefined || b.isDefined) && norm(a) != norm(b)

  private def bindingString(t: TypeDef): Option[String] = t.tableBinding.map((c, s) => s"${c}.${s}")

  /**
    * Warn when the freshly indexed type definition shares its name with a definition in another
    * (non-preset) compilation unit under a conflicting table binding. Name lookup resolves such
    * duplicates silently by file-name order (#93), so the shadowed schema binding never matches
    */
  private def warnCrossFileDuplicateTypeDefs(t: TypeDef)(using ctx: Context): Unit =
    if !ctx.compilationUnit.isPreset then
      val entries = ctx
        .global
        .symbolIndex
        .visibleEntries(
          t.name,
          ctx.compilationUnit.packageName,
          ctx.importDefs.map(_.importRef.fullName)
        )
      if entries.sizeIs > 1 then
        // visibleEntries is file-name-sorted and includes this unit's fresh entry, so the head
        // is the definition that findSymbolByName actually uses
        val winnerFileName = entries.head.fileName
        entries
          .withFilter(e => !(e.unit eq ctx.compilationUnit) && !e.unit.isPreset)
          .foreach { e =>
            e.symbol.tree match
              case other: TypeDef if bindingsConflict(other.tableBinding, t.tableBinding) =>
                ctx.addTyperError(
                  DuplicateTypeDefinition(
                    typeName = t.name.name,
                    thisBinding = bindingString(t),
                    otherBinding = bindingString(other),
                    otherLocation = e.unit.sourceFile.sourceLocationAt(other.span.start),
                    winnerFileName = winnerFileName,
                    span = t.span
                  )
                )
              case _ =>
          }

  private def registerTypeDefSymbol(t: TypeDef)(using ctx: Context): Symbol =
    val typeName = t.name

    ctx.scope.lookupSymbol(typeName) match
      case Some(sym) =>
        // Symbol is already assigned if context-specific types and functions (e.g., in duckdb, in trino) are defined
        t.symbol = sym
        trace(s"Attach symbol ${sym} to ${t.name} ${t.locationString(using ctx)}")
        // Same-name definitions merge into one symbol on purpose (dialect extensions like
        // `type string in duckdb`), but conflicting table bindings would shadow each other
        sym.tree match
          case prev: TypeDef
              if (prev ne t) && bindingsConflict(prev.tableBinding, t.tableBinding) =>
            ctx.addTyperError(
              DuplicateTypeDefinition(
                typeName = typeName.name,
                thisBinding = bindingString(t),
                otherBinding = bindingString(prev),
                otherLocation = ctx.sourceLocationAt(prev.span),
                winnerFileName = ctx.compilationUnit.sourceFile.fileName,
                span = t.span
              )
            )
          case _ =>
        // Locate the type scope without forcing a pending lazy completion: forcing here would
        // resolve parent types before all compilation units are labeled
        val knownTypeScope =
          sym match
            case ts: TypeSymbol if ts.typeScope.isDefined =>
              ts.typeScope
            case _ =>
              sym.symbolInfo match
                case ts: TypeSymbolInfo =>
                  Some(ts.declScope)
                case _ =>
                  None
        knownTypeScope match
          case Some(typeScope) =>
            t.elems
              .collect { case f: FunctionDef =>
                val ft             = toFunctionType(f, t.defContexts)
                val funSym: Symbol =
                  typeScope.lookupSymbol(f.name) match
                    case Some(funSym) =>
                      funSym
                    case None =>
                      val funSym = Symbol(ctx.global.newSymbolId, f.span)
                      f.symbol = funSym
                      typeScope.add(ft.name, funSym)
                      funSym

                val methodSymbolInfo = MethodSymbolInfo(
                  sym,
                  funSym,
                  f.name,
                  ft,
                  f.expr,
                  t.defContexts ++ f.defContexts,
                  ctx.compilationUnit
                )
                if !funSym.hasSymbolInfo then
                  funSym.symbolInfo = methodSymbolInfo
                else
                  funSym.symbolInfo = MultipleSymbolInfo(methodSymbolInfo, funSym.symbolInfo)
              }
          case _ =>
        end match
        sym
      case None =>
        // Create a new type symbol. Method registration, field parsing, and parent-type
        // resolution are deferred to a completer, so a parent type defined in another
        // compilation unit resolves once all units are labeled, independent of unit order
        val sym       = TypeSymbol(ctx.global.newSymbolId, t.span, ctx.compilationUnit.sourceFile)
        val typeCtx   = ctx.newContext(sym)
        val typeScope = typeCtx.scope
        sym.setTypeScope(typeScope)
        sym.setCompleter(typeName, s => computeTypeDefSymbolInfo(t, s, typeScope)(using ctx))
        sym.tree = t
        // Enter after the completer installation so the symbol is indexed with its name
        ctx.enterGlobalSymbol(sym)
        warnCrossFileDuplicateTypeDefs(t)

        t.symbol = sym
        ctx.scope.add(typeName, sym)
        sym
    end match

  end registerTypeDefSymbol

  /**
    * Compute the TypeSymbolInfo of a type definition: register its methods into the type scope,
    * parse the field columns, and resolve the parent type. Runs on the first access to the type
    * symbol's info (via SymbolCompleter), after all compilation units have been labeled
    */
  private def computeTypeDefSymbolInfo(t: TypeDef, sym: Symbol, typeScope: Scope)(using
      ctx: Context
  ): TypeSymbolInfo =
    val typeName = t.name

    // Register method defs to the type scope
    t.elems
      .collect { case f: FunctionDef =>
        val ft     = toFunctionType(f, t.defContexts)
        val funSym =
          typeScope.lookupSymbol(f.name) match
            case Some(sym) =>
              sym
            case None =>
              val newSym = Symbol(ctx.global.newSymbolId, f.span)
              typeScope.add(ft.name, newSym)
              newSym
        f.symbol = funSym
        val newSymbolInfo = MethodSymbolInfo(
          sym,
          funSym,
          f.name,
          ft,
          f.expr,
          t.defContexts ++ f.defContexts,
          ctx.compilationUnit
        )
        if !funSym.hasSymbolInfo then
          funSym.symbolInfo = newSymbolInfo
        else
          funSym.symbolInfo = MultipleSymbolInfo(newSymbolInfo, funSym.symbolInfo)
        ft
      }

    val columns = t
      .elems
      .collect { case v: FieldDef =>
        // Resolve simple primitive types earlier.
        // TODO: DataType.parse(typeName) for complex types, including UnknownTypes
        val dt = DataType.parse(v.fieldType.fullName, v.params)
        NamedType(v.name, dt)
      }

    // The schema parent is only the extended type (None when the type has no parent); the
    // owner symbol falls back to the enclosing package
    val parentTypeSymbol = t.parent.map(registerParentSymbols)
    val parentTpe        = parentTypeSymbol.map(_.dataType)
    val ownerSymbol      = parentTypeSymbol.getOrElse(ctx.owner)
    val tpe              = SchemaType(parent = parentTpe, typeName, columns)
    val typeParams       = t.params

    trace(s"Completed type symbol ${sym}: ${tpe}")
    TypeSymbolInfo(
      owner = ownerSymbol,
      sym,
      typeName,
      tpe,
      typeParams,
      typeScope,
      ctx.compilationUnit
    )

  end computeTypeDefSymbolInfo

  private def registerParentSymbols(parent: NameExpr)(using ctx: Context): Symbol =
    // TODO support full type path
    val typeName = Name.typeName(parent.leafName)
    // This runs from a completer after all units are labeled, so consult the global scope as
    // well: the parent type may be defined in another compilation unit
    ctx.scope.lookupSymbol(typeName).orElse(ctx.findSymbolByName(typeName)) match
      case Some(s) =>
        s
      case None =>
        val sym = Symbol(ctx.global.newSymbolId, parent.span)
        sym.symbolInfo = TypeSymbolInfo(
          ctx.owner,
          sym,
          typeName,
          DataType.UnknownType,
          Nil,
          ctx.scope,
          ctx.compilationUnit
        )
        parent.symbol = sym
        sym

  private def registerFlowSymbol(f: FlowDef)(using ctx: Context): Symbol =
    val flowName = Name.termName(f.name.name)

    def installSymbolInfo(sym: Symbol): Unit =
      sym.symbolInfo = FlowSymbolInfo(ctx.owner, sym, flowName, ctx.compilationUnit)

    ctx.scope.lookupSymbol(flowName) match
      case Some(s) =>
        // Update the existing flow symbol to avoid duplicates in REPL, and re-enter it so the
        // re-labeled unit stays visible to global symbol lookup
        s.tree = f
        installSymbolInfo(s)
        ctx.enterGlobalSymbol(s)
        f.symbol = s
        trace(s"Updated existing flow symbol ${s} for ${flowName}")
        s
      case None =>
        val sym = Symbol(ctx.global.newSymbolId, f.span)
        sym.tree = f
        installSymbolInfo(sym)
        ctx.enterGlobalSymbol(sym)
        f.symbol = sym
        trace(s"Created a new flow symbol ${sym}")
        ctx.scope.add(flowName, sym)
        sym

  end registerFlowSymbol

  private def registerModelSymbol(m: ModelDef)(using ctx: Context): Symbol =
    val modelName = Name.termName(m.name.name)

    // When the model declares its schema explicitly, the SymbolInfo is known at labeling time.
    // Otherwise, computing the model type requires typing the body, so install a completer that
    // types the definition on demand in this defining context. This avoids reading (and
    // memoizing) the structural relation type of the untyped tree, and makes cross-unit model
    // references independent of the compilation order of the units
    def installSymbolInfo(sym: Symbol): Unit =
      m.givenRelationType match
        case Some(tpe) =>
          sym.symbolInfo = ModelSymbolInfo(ctx.owner, sym, modelName, tpe, ctx.compilationUnit)
        case None =>
          sym.setCompleter(modelName, ModelDefCompleter(m, ctx.owner, modelName, ctx))

    ctx.scope.lookupSymbol(modelName) match
      case Some(s) =>
        // Update the existing model symbol to avoid duplicates in REPL, and re-enter it so
        // the re-labeled unit (whose known symbols were reset on reload) stays visible to
        // global symbol lookup
        s.tree = m
        installSymbolInfo(s)
        ctx.enterGlobalSymbol(s)
        m.symbol = s
        trace(s"Updated existing model symbol ${s} for ${modelName}")
        s
      case None =>
        val sym = Symbol(ctx.global.newSymbolId, m.span)
        sym.tree = m
        installSymbolInfo(sym)
        ctx.enterGlobalSymbol(sym)
        m.symbol = sym
        trace(s"Created a new model symbol ${sym}")
        ctx.scope.add(modelName, sym)
        sym

  end registerModelSymbol

end SymbolLabeler
