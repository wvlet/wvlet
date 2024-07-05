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
import com.treasuredata.flow.lang.model.DataType.NamedType
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
    label(unit.unresolvedPlan)(using context)
    unit

  private def label(plan: LogicalPlan)(using context: Context): Unit =
    def iter(tree: LogicalPlan): Context =
      tree match
        case p: PackageDef =>
          val packageSymbol = createPackageSymbol(p.name)
          packageSymbol.tree = p
          val packageCtx = context.newContext(packageSymbol)
          p.statements
            .foreach: stmt =>
              label(stmt)(using packageCtx)
          packageCtx
//        case i: Import =>
//          val sym = Symbol.newImportSymbol(context.owner, ImportType(i))
//          i.symbol = sym
//          // context.addImport(i)
//          context
        case t: TypeDef =>
          createTypeDefSymbol(t)
          context
        case m: ModelDef =>
          debug(s"TODO: Labeling model ${m.name}")
          context
        case _ =>
          context
    iter(plan)

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
          debug(s"Created package symbol for ${pkgName.fullName}, owner ${pkgOwner}")
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
        val sym = TypeSymbol(ctx.global.newSymbolId, ctx.compilationUnit.sourceFile)

        val typeScope = ctx.newContext(sym)

        // Check type members
        t.elems
          .foreach {
            case f: FunctionDef =>
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
            case v: TypeValDef =>
              debug(v)
          }
        // Associate TypeSymbolInfo with the symbol
        sym.symbolInfo = TypeSymbolInfo(sym, ctx.owner, typeName, DataType.UnknownType)
        debug(s"Created type symbol ${sym}")
        sym.tree = t

        t.symbol = sym
        ctx.scope.add(typeName, sym)
        sym
    end match

  end createTypeDefSymbol

end SymbolLabeler
