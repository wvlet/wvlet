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

import wvlet.lang.compiler.{
  CompilationUnit,
  Context,
  MethodSymbolInfo,
  ModelSymbolInfo,
  MultipleSymbolInfo,
  Name,
  NamedSymbolInfo,
  PackageSymbolInfo,
  Phase,
  RelationAliasSymbolInfo,
  SavedRelationSymbolInfo,
  Scope,
  Symbol,
  TypeSymbol,
  TypeSymbolInfo
}
import wvlet.lang.model.DataType.{NamedType, SchemaType}
import wvlet.lang.model.Type.{FunctionType, ImportType, PackageType}
import wvlet.lang.model.expr.{DotRef, Identifier, NameExpr, QualifiedName}
import wvlet.lang.model.plan.*
import wvlet.lang.model.{DataType, Type}

/**
  * Assign unique Symbol to PackageDef, Import, TypeDef, and ModelDef nodes, and assign a lazy
  * SymbolInfo to them. Registered Symbols are permanent identifiers throughout the compilation
  * phases for looking up their resulting data types.
  */
object SymbolLabeler extends Phase("symbol-labeler"):
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    label(unit.unresolvedPlan, context)
    unit

  private def label(plan: LogicalPlan, context: Context): Unit =
    if context.isContextCompilationUnit then
      debug(s"Labeling symbols for ${plan.pp}")
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
          val sym = Symbol.newImportSymbol(ctx.owner, ImportType(i))(using ctx)
          // Attach the import symbol to the Tree node
          i.symbol = sym
          trace(s"Created import symbol for ${i.symbol}")
          ctx.withImport(i)
        case t: TypeDef =>
          registerTypeDefSymbol(t)(using ctx)
          ctx
        case m: ModelDef =>
          registerModelSymbol(m)(using ctx)
          ctx
        case f: TopLevelFunctionDef =>
          registerTopLevelFunction(f)(using ctx)
          ctx
        case s: SaveToTable =>
          iter(s.child, ctx)
          registerSaveAs(s)(using ctx)
          ctx
        case d: Debug =>
          warn(d)
          iter(d.debugExpr, ctx)
          ctx
        case q: Relation =>
          q.traverseOnce {
            case s: SelectAsAlias =>
              iter(s.child, ctx)
              registerSelectAsAlias(s)(using ctx)
            case d: Debug =>
              iter(d.debugExpr, ctx)
          }
          ctx
        case _ =>
          ctx

    iter(plan, context)

  end label

  private def registerTopLevelFunction(t: TopLevelFunctionDef)(using ctx: Context): Symbol =
    val sym = Symbol(ctx.global.newSymbolId)
    sym.symbolInfo = MethodSymbolInfo(
      symbol = sym,
      owner = ctx.owner,
      name = t.functionDef.name,
      ft = toFunctionType(t.functionDef, Nil),
      body = t.functionDef.expr,
      Nil
    )
    t.symbol = sym
    sym.tree = t
    ctx.compilationUnit.enter(sym)
    sym

  private def registerSelectAsAlias(s: SelectAsAlias)(using ctx: Context): Symbol =
    val aliasName = s.alias.toTermName
    val sym       = Symbol(ctx.global.newSymbolId)
    sym.symbolInfo = RelationAliasSymbolInfo(sym, aliasName)
    s.symbol = sym
    sym.tree = s.child
    ctx.compilationUnit.enter(sym)
    sym

  private def registerSaveAs(s: SaveToTable)(using ctx: Context): Symbol =
    val targetName = s.refName.toTermName
    val sym        = Symbol(ctx.global.newSymbolId)
    sym.symbolInfo = SavedRelationSymbolInfo(sym, targetName)
    s.symbol = sym
    sym.tree = s.child
    ctx.compilationUnit.enter(sym)
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
    val pkgSymbol =
      pkgOwner.symbolInfo.declScope.lookupSymbol(pkgLeafName) match
        case Some(s) =>
          s
        case None =>
          val sym = Symbol(ctx.global.newSymbolId)
          val pkgSymInfo = PackageSymbolInfo(
            sym,
            pkgOwner,
            pkgLeafName,
            PackageType(pkgLeafName),
            // Create a fresh scope for defining global package
            Scope.newScope(0)
          )
          sym.symbolInfo = pkgSymInfo
          ctx.compilationUnit.enter(sym)
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
      contextNames = defContexts.map(x => Name.typeName(x.tpe.leafName))
    )

  private def registerTypeDefSymbol(t: TypeDef)(using ctx: Context): Symbol =
    val typeName = t.name

    ctx.scope.lookupSymbol(typeName) match
      case Some(sym) =>
        // Symbol is already assigned if context-specific types and functions (e.g., in duckdb, in trino) are defined
        t.symbol = sym
        trace(s"Attach symbol ${sym} to ${t.name} ${t.locationString(using ctx)}")
        sym.symbolInfo match
          case ts: TypeSymbolInfo =>
            val typeScope = ts.declScope
            t.elems
              .collect { case f: FunctionDef =>
                val ft = toFunctionType(f, t.defContexts)
                val funSym: Symbol =
                  typeScope.lookupSymbol(f.name) match
                    case Some(funSym) =>
                      funSym
                    case None =>
                      val funSym = Symbol(ctx.global.newSymbolId)
                      f.symbol = funSym
                      typeScope.add(ft.name, funSym)
                      funSym

                val methodSymbolInfo = MethodSymbolInfo(
                  funSym,
                  sym,
                  f.name,
                  ft,
                  f.expr,
                  t.defContexts ++ f.defContexts
                )
                if funSym.symbolInfo == null then
                  funSym.symbolInfo = methodSymbolInfo
                else
                  funSym.symbolInfo = MultipleSymbolInfo(methodSymbolInfo, funSym.symbolInfo)
              }
          case _ =>
        end match
        sym
      case None =>
        // Create a new type symbol
        val sym = TypeSymbol(ctx.global.newSymbolId, ctx.compilationUnit.sourceFile)
        ctx.compilationUnit.enter(sym)
        val typeCtx   = ctx.newContext(sym)
        val typeScope = typeCtx.scope

        // Register method defs to the type scope
        t.elems
          .collect { case f: FunctionDef =>
            val ft = toFunctionType(f, t.defContexts)
            val funSym =
              typeScope.lookupSymbol(f.name) match
                case Some(sym) =>
                  sym
                case None =>
                  val newSym = Symbol(ctx.global.newSymbolId)
                  typeScope.add(ft.name, newSym)
                  newSym
            f.symbol = funSym
            val newSymbolInfo = MethodSymbolInfo(
              funSym,
              sym,
              f.name,
              ft,
              f.expr,
              t.defContexts ++ f.defContexts
            )
            if funSym.symbolInfo == null then
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
            val dt = DataType.parse(v.tpe.fullName, v.params)
            NamedType(v.name, dt)
          }

        val parentSymbol = t.parent.map(registerParentSymbols).orElse(Some(ctx.owner))
        val parentTpe    = parentSymbol.map(_.dataType)
        val tpe          = SchemaType(parent = parentTpe, typeName, columns)
        val typeParams   = t.params

        // Associate TypeSymbolInfo with the symbol
        sym.symbolInfo = TypeSymbolInfo(
          sym,
          owner = parentSymbol.get,
          typeName,
          tpe,
          typeParams,
          typeScope
        )

        trace(s"Created type symbol ${sym}: ${tpe}")
        sym.tree = t

        t.symbol = sym
        ctx.scope.add(typeName, sym)
        sym
    end match

  end registerTypeDefSymbol

  private def registerParentSymbols(parent: NameExpr)(using ctx: Context): Symbol =
    // TODO support full type path
    val typeName = Name.typeName(parent.leafName)
    ctx.scope.lookupSymbol(typeName) match
      case Some(s) =>
        s
      case None =>
        val sym = Symbol(ctx.global.newSymbolId)
        sym.symbolInfo = TypeSymbolInfo(
          sym,
          Symbol.NoSymbol,
          typeName,
          DataType.UnknownType,
          Nil,
          ctx.scope
        )
        parent.symbol = sym
        sym

  private def registerModelSymbol(m: ModelDef)(using ctx: Context): Symbol =
    val modelName = Name.termName(m.name.name)
    ctx.scope.lookupSymbol(modelName) match
      case Some(s) =>
        s
      case None =>
        val sym = Symbol(ctx.global.newSymbolId)
        ctx.compilationUnit.enter(sym)
        sym.tree = m
        val tpe = m.givenRelationType.getOrElse(m.relationType)
        sym.symbolInfo = ModelSymbolInfo(sym, ctx.owner, modelName, tpe)
        m.symbol = sym
        trace(s"Created a new model symbol ${sym}")
        ctx.scope.add(modelName, sym)
        sym

end SymbolLabeler
