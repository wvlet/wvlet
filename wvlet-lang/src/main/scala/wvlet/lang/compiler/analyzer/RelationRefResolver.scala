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

import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.ContextLogSupport
import wvlet.lang.compiler.ModelSymbolInfo
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.RelationAliasSymbolInfo
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.ValSymbolInfo
import wvlet.lang.compiler.ContextUtil.*
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.RelationType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * Resolution of table, model, and data-file references into concrete scan nodes. Compilation of
  * `.wv`/`.sql` file imports stays in the Typer because it requires re-running the phase on the
  * referenced unit.
  */
object RelationRefResolver extends ContextLogSupport:

  private def lookupType(name: Name, context: Context): Option[Symbol] = context.findSymbolByName(
    name
  )

  private def lookup(qName: NameExpr, context: Context): Option[Symbol] =
    qName match
      case i: Identifier =>
        lookupType(i.toTermName, context)
      case d: DotRef =>
        // TODO Load table schema from the given qualified name
        None
      case _ =>
        None

  /**
    * Resolve a TableRef into a ModelScan, Values (table value constant), aliased relation, or
    * TableScan using the symbols in scope and the catalog
    */
  def resolveTableRef(ref: TableRef)(using context: Context): Relation =
    lookup(ref.name, context) match
      case Some(sym) if sym.isCompleting =>
        // A reference to a definition whose lazy completion is in progress, i.e. a recursive
        // model reference. Leave it unresolved here so that model expansion can report the
        // recursion with the full reference path
        ref
      case Some(sym) =>
        // Reading symbolInfo forces the lazy completion of the definition (typing it in its
        // defining context), so cross-unit references resolve independently of unit order
        sym.symbolInfo match
          case mi: ModelSymbolInfo =>
            mi.dataType match
              case r: RelationType =>
                ModelScan(TableName(ref.name.fullName), Nil, r, ref.span)
              case _ =>
                ref
          case v: ValSymbolInfo =>
            // Check if this is a table value constant by looking at the dataType
            v.dataType match
              case schemaType: DataType.SchemaType =>
                // Handle table value constants: val t1(id, val) = [[...]]
                // The expression is an ArrayConstructor containing rows (each row is also an
                // ArrayConstructor)
                v.expr match
                  case arr: ArrayConstructor =>
                    // arr.values contains the rows - use them directly. The declared schema
                    // (val t1(id, name) = ...) carries no column types, so refine them from
                    // the first row's values
                    Values(arr.values, refineSchemaFromRows(schemaType, arr.values), arr.span)
                  case other =>
                    ref
              case _ =>
                // Regular val definition, not a table value constant
                ref
          case relAlias: RelationAliasSymbolInfo =>
            // Replace alias to the referenced query
            sym.tree match
              case r: Relation =>
                r
              case _ =>
                ref
          case _ =>
            ref
      case None =>
        // Lookup known types
        val tblType = Name.typeName(ref.name.leafName)
        lookupType(tblType, context).map(_.symbolInfo.dataType) match
          case Some(tpe: SchemaType) =>
            context.logTrace(s"Found a table type for ${tblType}: ${tpe}")
            val tableName = TableName.parse(tblType.toTermName.name)
            TableScan(tableName, tpe, tpe.fields, ref.span)
          case _ =>
            resolveConnectorQualifiedRef(ref, context) match
              case Some(resolved) =>
                resolved
              case None =>
                resolveFromDefaultCatalog(ref, context)
    end match
  end resolveTableRef

  /**
    * Resolve `from <connector>.<table>` / `from <connector>.<schema>.<table>` where the leading
    * identifier names a connector activated by the current profile. Connector names are checked
    * only after symbol lookup (models, CTEs, aliases) has failed, and shadow schema names of the
    * default catalog.
    */
  private def resolveConnectorQualifiedRef(ref: TableRef, context: Context): Option[Relation] =
    ref.name.fullName.split("\\.").toList match
      case connectorName :: rest if rest.nonEmpty =>
        context
          .connectorCatalog(connectorName)
          .flatMap { entry =>
            val schemaAndTable =
              rest match
                case table :: Nil =>
                  Some((entry.defaultSchema, table))
                case schema :: table :: Nil =>
                  Some((schema, table))
                case _ =>
                  // connector.catalog.schema.table (4-part) arrives with cross-connector staging
                  None
            schemaAndTable.flatMap { (schema, table) =>
              entry
                .catalog
                .findTable(schema, table)
                .map { tbl =>
                  context.compilationUnit.referencedConnectors += connectorName
                  val tableName = TableName(Some(entry.catalog.catalogName), Some(schema), table)
                  TableScan(tableName, tbl.schemaType, tbl.schemaType.fields, ref.span)
                }
            }
          }
      case _ =>
        None

  private def resolveFromDefaultCatalog(ref: TableRef, context: Context): Relation =
    val tableName = TableName.parse(ref.name.fullName)
    context
      .catalog
      .findTable(tableName.schema.getOrElse(context.defaultSchema), tableName.name) match
      case Some(tbl) =>
        TableScan(tableName, tbl.schemaType, tbl.schemaType.fields, ref.span)
      case None =>
        context
          .workEnv
          .errorLogger
          .debug(s"Unresolved table ref: ${ref.name.fullName}: ${context.scope.getAllEntries}")
        ref
  end resolveFromDefaultCatalog

  /**
    * Fill in unresolved column types of a table-value-constant schema from the literal values of
    * the first row. Also used by SymbolLabeler so that references through the val's symbol (e.g.
    * alias-qualified columns in joins) see the refined column types
    */
  def refineSchemaFromRows(schema: SchemaType, rows: List[Expression]): SchemaType =
    if schema.isResolved then
      schema
    else
      rows.headOption match
        case Some(row: ArrayConstructor) if row.values.length == schema.columnTypes.length =>
          val refined = schema
            .columnTypes
            .zip(row.values)
            .map { (col, v) =>
              if col.dataType.isResolved then
                col
              else
                DataType.NamedType(col.name, v.dataType)
            }
          schema.copy(columnTypes = refined)
        case _ =>
          schema

  /**
    * Resolve a table function call (e.g. a parameterized model reference) into a ModelScan
    */
  def resolveTableFunctionCall(ref: TableFunctionCall)(using context: Context): Relation =
    lookup(ref.name, context) match
      case Some(sym) if sym.isCompleting =>
        // A recursive parameterized-model reference; leave unresolved (see resolveTableRef)
        ref
      case Some(sym) =>
        val si = sym.symbolInfo
        si.tpe match
          case r: RelationType =>
            context.logTrace(s"Resolved model ref: ${ref.name.fullName} as ${r}")
            ModelScan(TableName(sym.name.name), ref.args, r, ref.span)
          case _ =>
            ref
      case None =>
        context.logTrace(s"Unresolved model ref: ${ref.name.fullName}")
        ref

  /**
    * Attach the model schema to an unresolved ModelScan
    */
  def resolveModelScan(m: ModelScan)(using context: Context): Relation =
    context.findTermSymbolByName(m.name.fullName) match
      case Some(sym) if !sym.isCompleting && sym.isModelDef =>
        // isModelDef forces the lazy completion, so sym.tree is the typed model definition
        sym.tree match
          case md: ModelDef =>
            val newModelScan = m.copy(schema = md.relationType)
            newModelScan.symbol = md.child.symbol
            newModelScan
          case _ =>
            m
      case _ =>
        m

  /**
    * Returns true if the given path points to a data file that can be resolved into a FileScan
    */
  def isDataFilePath(path: String): Boolean =
    path.endsWith(".json") || path.endsWith(".json.gz") || path.endsWith(".parquet") ||
      path.endsWith(".csv")

  /**
    * Resolve a data-file reference (json/parquet/csv) into a FileScan by analyzing the file schema.
    * References to `.wv`/`.sql` files are not handled here.
    */
  def resolveDataFileRef(f: FileRef)(using context: Context): Option[Relation] =
    if f.filePath.endsWith(".json") || f.filePath.endsWith(".json.gz") then
      val file             = context.getDataFile(f.filePath)
      val jsonRelationType = JSONAnalyzer.analyzeJSONFile(file)
      val cols             = jsonRelationType.fields
      Some(FileScan(SingleQuoteString(file, f.span), jsonRelationType, cols, f.span))
    else if f.filePath.endsWith(".parquet") || f.filePath.endsWith(".csv") then
      val file         = context.dataFilePath(f.filePath)
      val relationType = DuckDBAnalyzer.guessSchema(file)
      val cols         = relationType.fields
      Some(FileScan(SingleQuoteString(file, f.span), relationType, cols, f.span))
    else
      None

end RelationRefResolver
