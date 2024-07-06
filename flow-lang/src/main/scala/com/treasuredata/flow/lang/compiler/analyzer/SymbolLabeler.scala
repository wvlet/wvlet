package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.compiler.{
  CompilationUnit,
  Context,
  Name,
  NamedSymbolInfo,
  PackageSymbolInfo,
  Phase,
  Scope,
  Symbol,
  TypeSymbol,
  TypeSymbolInfo
}
import com.treasuredata.flow.lang.model.DataType.{NamedType, SchemaType}
import com.treasuredata.flow.lang.model.Type.FunctionType
import com.treasuredata.flow.lang.model.{DataType, Type}
import Type.{ImportType, PackageType}
import com.treasuredata.flow.lang.model.expr.{DotRef, Identifier, NameExpr, QualifiedName}
import com.treasuredata.flow.lang.model.plan.{
  FunctionDef,
  Import,
  LogicalPlan,
  ModelDef,
  PackageDef,
  TypeDef,
  TypeValDef
}

/**
  * Assign unique Symbol to each LogicalPlan and Expression nodes, a and assign a lazy SymbolInfo
  */
object SymbolLabeler extends Phase("symbol-labeler"):
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    label(unit.unresolvedPlan, context)
    unit

  private def label(plan: LogicalPlan, context: Context): Unit =
    def iter(tree: LogicalPlan, ctx: Context): Context =
      tree match
        case p: PackageDef =>
          val packageSymbol = createPackageSymbol(p.name)(using ctx)
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
          createTypeDefSymbol(t)(using ctx)
          ctx
        case m: ModelDef =>
          createModelSymbol(m)(using ctx)
          ctx
        case _ =>
          ctx

    iter(plan, context)

  private def createPackageSymbol(pkgName: NameExpr)(using ctx: Context): Symbol =
    val pkgOwner: Symbol =
      pkgName match
        case DotRef(parent: QualifiedName, _, _, _) =>
          createPackageSymbol(parent)
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
          // pkgOwner.symbolInfo.declScope.add(pkgLeafName, sym)
          trace(s"Created package symbol for ${pkgName.fullName}, owner ${pkgOwner}")
          pkgOwner.symbolInfo.declScope.add(pkgLeafName, sym)
          sym

    pkgSymbol

  end createPackageSymbol

  private def createTypeDefSymbol(t: TypeDef)(using ctx: Context): Symbol =
    val typeName = Name.typeName(t.name.leafName)

    ctx.scope.lookupSymbol(typeName) match
      case Some(s) =>
        s
      case None =>
        // Create a new type symbol
        val sym       = TypeSymbol(ctx.global.newSymbolId, ctx.compilationUnit.sourceFile)
        val typeScope = ctx.newContext(sym)

        // Check type members
        val defs = t
          .elems
          .collect { case f: FunctionDef =>
            FunctionType(
              name = Name.termName(f.name.leafName),
              args = f
                .args
                .map { a =>
                  val paramName = Name.termName(a.name.leafName)
                  val paramType = a.dataType
                  NamedType(paramName, paramType)
                },
              returnType = f.dataType
            )
          }
        val columns = t
          .elems
          .collect { case v: TypeValDef =>
            NamedType(Name.termName(v.name.leafName), v.dataType)
          }

        val parentSymbol = t.parent.map(createParentSymbol).orElse(Some(ctx.owner))
        val parentTpe    = parentSymbol.map(_.dataType)
        val tpe          = SchemaType(parent = parentTpe, typeName, columns, defs)

        // Associate TypeSymbolInfo with the symbol
        sym.symbolInfo = TypeSymbolInfo(sym, owner = parentSymbol.get, typeName, tpe)
        trace(s"Created type symbol ${sym}")
        sym.tree = t

        t.symbol = sym
        ctx.scope.add(typeName, sym)
        sym
    end match

  end createTypeDefSymbol

  private def createParentSymbol(parent: NameExpr)(using ctx: Context): Symbol =
    // TODO support full type path
    val typeName = Name.typeName(parent.leafName)
    ctx.scope.lookupSymbol(typeName) match
      case Some(s) =>
        s
      case None =>
        val sym = Symbol(ctx.global.newSymbolId)
        sym.symbolInfo = TypeSymbolInfo(sym, Symbol.NoSymbol, typeName, DataType.UnknownType)
        parent.symbol = sym
        sym

  private def createModelSymbol(m: ModelDef)(using ctx: Context): Symbol =
    val modelName = Name.termName(m.name.leafName)
    ctx.scope.lookupSymbol(modelName) match
      case Some(s) =>
        s
      case None =>
        val sym = Symbol(ctx.global.newSymbolId)
        sym.tree = m
        sym.symbolInfo = TypeSymbolInfo(sym, ctx.owner, modelName, m.relationType)
        m.symbol = sym
        trace(s"Created a new model symbol ${sym}")
        ctx.scope.add(modelName, sym)
        sym

end SymbolLabeler
