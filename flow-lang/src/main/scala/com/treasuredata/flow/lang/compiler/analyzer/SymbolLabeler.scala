package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.compiler.{
  CompilationUnit,
  Context,
  Name,
  NamedSymbolInfo,
  PackageSymbolInfo,
  Phase,
  Symbol,
  Scope
}
import com.treasuredata.flow.lang.model.{DataType, ImportType, PackageType, Type}
import com.treasuredata.flow.lang.model.expr.{DotRef, Identifier, NameExpr, QualifiedName}
import com.treasuredata.flow.lang.model.plan.{Import, LogicalPlan, PackageDef, TypeDef}

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
          val packageCtx    = context.newContext(packageSymbol)
          p.statements
            .foreach: stmt =>
              label(stmt)(using packageCtx)
          packageCtx
//        case i: Import =>
//          val sym = Symbol.newImportSymbol(context.owner, ImportType(i))
//          i.symbol = sym
//          // context.addImport(i)
//          context
//        case t: TypeDef =>
//          val sym = Symbol.newTypeDefSymbol(context.owner, t.name, DataType.UnknownType)
//          t.symbol = sym
//          context
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

end SymbolLabeler
