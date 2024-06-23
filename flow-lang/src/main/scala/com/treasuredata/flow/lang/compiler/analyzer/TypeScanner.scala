package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.{CompilationUnit, Context, Phase}
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.{
  ExtensionType,
  FunctionType,
  NamedType,
  SchemaType,
  UnresolvedType
}
import com.treasuredata.flow.lang.model.expr.{ColumnType, Literal}
import com.treasuredata.flow.lang.model.plan.*
import wvlet.log.LogSupport

object PreTypeScan  extends TypeScanner("collect-types")
object PostTypeScan extends TypeScanner("post-type-scan")

/**
  * Scan all defined types in the code, including imported and defined ones
  */
class TypeScanner(phaseName: String) extends Phase(phaseName) with LogSupport:
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    scanTypeDefs(unit.unresolvedPlan, context)
    unit

  protected def scanTypeDefs(plan: LogicalPlan, context: Context): Unit = plan.traverse {
    case alias: TypeAlias =>
      context.scope.addAlias(alias.alias, alias.sourceTypeName)
    case td: TypeDef =>
      context.scope.addType(scanTypeDef(td, context))
    case tbl: TableDef =>
      context.scope.addTableDef(tbl)
    case imp: ImportDef =>
      debug(s"add import ${imp.importRef}")
      context.addImport(imp)
    case m: ModelDef =>
      debug(s"add model ${m.name}")
    case q: Query =>
      context.scope.addType(q.relationType)
  }

  private def scanTypeDef(typeDef: TypeDef, context: Context): DataType =
    // TODO resolve defs
    val defs: Seq[FunctionType] = Seq.empty // typeDef.defs.collect { case tpe: TypeDefDef =>

    // Scan SchemaType parameters
    val valDefs = typeDef
      .elems
      .collect { case v: TypeValDef =>
        val resolvedType = scanDataType(ColumnType(v.tpe, v.nodeLocation), context)
        NamedType(v.name, resolvedType)
      }

    if valDefs.nonEmpty then
      // TODO: Add parent fields
      SchemaType(typeDef.name.fullName, valDefs)
    else
      // TODO: Add parent fields
      ExtensionType(
        typeDef.name.fullName,
        typeDef.parent.map(p => UnresolvedType(p.fullName)),
        defs
      )

  private def scanDataType(columnType: ColumnType, context: Context): DataType = context
    .scope
    .findType(columnType.tpe.fullName)
    .getOrElse(UnresolvedType(columnType.tpe.fullName))
