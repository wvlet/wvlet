package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.{CompilationUnit, Context, Phase}
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.{
  FunctionType,
  NamedType,
  SchemaType,
  UnresolvedRelationType,
  UnresolvedType
}
import com.treasuredata.flow.lang.model.expr.{ColumnType, Literal, Name}
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
    // debug(context.scope.getAllTypes.map(t => s"[${t._1}]: ${t._2.typeDescription}").mkString("\n"))
    unit

  protected def scanTypeDefs(plan: LogicalPlan, context: Context): Unit = plan.traverse {
    case alias: TypeAlias =>
      context.scope.addAlias(alias.alias, alias.sourceTypeName.fullName)
    case td: TypeDef =>
      val dataType = scanTypeDef(td, context)
      context.scope.addType(dataType)
    case tbl: TableDef =>
      context.scope.addTableDef(tbl)
    case imp: Import =>
      // debug(s"add import ${imp.importRef}")
      context.addImport(imp)
    case m: ModelDef =>
      context.scope.addType(m.name, m.relationType)
    case q: Query =>
    // context.scope.addType(q.relationType)
  }

  private def scanTypeDef(typeDef: TypeDef, context: Context): DataType =
    // TODO resolve defs
    val defs: List[FunctionType] = typeDef
      .elems
      .collect { case f: FunctionDef =>
        FunctionType(
          f.name.fullName,
          f.args.map(arg => NamedType(arg.name, arg.dataType)),
          f.retType.getOrElse(DataType.UnknownType)
        )
      }

    val parent = typeDef
      .parent
      .map(p => context.scope.resolveType(p.fullName).getOrElse(UnresolvedType(p.fullName)))

    // Scan SchemaType parameters
    val valDefs = typeDef
      .elems
      .collect { case v: TypeValDef =>
        val resolvedType = scanDataType(ColumnType(v.tpe, v.nodeLocation), context)
        NamedType(v.name, resolvedType)
      }

    // TODO: Add parent fields
    SchemaType(parent, typeDef.name.fullName, valDefs, defs)

  private def scanDataType(columnType: ColumnType, context: Context): DataType = context
    .scope
    .findType(columnType.tpe.fullName)
    .getOrElse(UnresolvedType(columnType.tpe.fullName))

end TypeScanner
