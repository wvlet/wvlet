package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.{CompilationUnit, Context, Phase}
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.{ExtensionType, FunctionType, NamedType, SchemaType, UnresolvedType}
import com.treasuredata.flow.lang.model.expr.{ColumnType, Literal}
import com.treasuredata.flow.lang.model.plan.*
import wvlet.log.LogSupport

/**
  * Scan all referenced types in the code, including imported and defined ones
  */
object TypeScanner extends Phase("scan-types") with LogSupport:
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    // Pre-process to collect all schema and types
    TypeScanner.scanTypeDefs(unit.unresolvedPlan, context)
    // Post-process to resolve unresolved types
    TypeScanner.scanTypeDefs(unit.unresolvedPlan, context)
    unit

  private def scanTypeDefs(plan: LogicalPlan, context: Context): Unit =
    plan.traverse {
      case alias: TypeAlias =>
        context.scope.addAlias(alias.alias, alias.sourceTypeName)
      case td: TypeDef =>
        context.scope.addType(scanTypeDef(td, context))
      case tbl: TableDef =>
        context.scope.addTableDef(tbl)
      case q: Query =>
        context.scope.addType(q.relationType)
    }

  private def scanTypeDef(typeDef: TypeDef, context: Context): DataType =
    val typeParams = typeDef.params.collect { case p: TypeParam =>
      val resolvedType: DataType = context.scope.findType(p.value).getOrElse(UnresolvedType(p.value))
      NamedType(p.name, resolvedType)
    }
    // TODO resolve defs
    val defs: Seq[FunctionType] = Seq.empty // typeDef.defs.collect { case tpe: TypeDefDef =>

    val valDefs = typeDef.elems.collect { case v: TypeValDef =>
      val resolvedType = scanDataType(ColumnType(v.tpe, v.nodeLocation), context)
      NamedType(v.name, resolvedType)
    }
    val selfType = typeParams.filter(_.name == "self")

    if valDefs.nonEmpty then SchemaType(typeDef.name, valDefs)
    else
      selfType.size match
        case 0 =>
          throw StatusCode.SYNTAX_ERROR.newException(
            "Missing self parameter in type definition",
            context.compileUnit.toSourceLocation(typeDef.nodeLocation)
          )
        case n if n > 1 =>
          throw StatusCode.SYNTAX_ERROR.newException(
            "Multiple self parameters are found in type definition",
            context.compileUnit.toSourceLocation(typeDef.nodeLocation)
          )
        case 1 =>
          ExtensionType(typeDef.name, selfType.head, defs)

  private def scanDataType(columnType: ColumnType, context: Context): DataType =
    context.scope
      .findType(columnType.tpe)
      .getOrElse(UnresolvedType(columnType.tpe))
