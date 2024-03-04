package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.analyzer.Type.{ExtensionType, NamedType, RecordType, UnresolvedType}
import com.treasuredata.flow.lang.model.expr.ColumnType
import com.treasuredata.flow.lang.model.plan.{FlowPlan, SchemaDef, TypeDef, TypeParam}
import wvlet.log.LogSupport

object TypeScanner extends LogSupport:

  def scanSchemaAndTypes(flow: FlowPlan, context: AnalyzerContext): Unit =
    flow.logicalPlans.collect {
      case schema: SchemaDef =>
        val resolvedSchema = scanSchema(schema, context)
        context.addSchema(resolvedSchema)
      case td: TypeDef =>
        context.addType(scanTypeDef(td, context))
    }

  private def scanSchema(schemaDef: SchemaDef, context: AnalyzerContext): RecordType =
    val resolvedFields = schemaDef.columns.map { field =>
      NamedType(field.columnName.value, scanDataType(field.tpe, context))
    }
    RecordType(schemaDef.name, resolvedFields)

  private def scanDataType(columnType: ColumnType, context: AnalyzerContext): Type =
    context
      .findType(columnType.tpe)
      .getOrElse(UnresolvedType(columnType.tpe))

  private def scanTypeDef(typeDef: TypeDef, context: AnalyzerContext): ExtensionType =
    val typeParams = typeDef.params.collect { case p: TypeParam =>
      val resolvedType: Type = context.findType(p.value).getOrElse(UnresolvedType(p.value))
      NamedType(p.name, resolvedType)
    }
    // TODO resolve defs
    val defs: Seq[Def] = Seq.empty // typeDef.defs.collect { case tpe: TypeDefDef =>

    val selfType = typeParams.filter(_.name == "self")
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
