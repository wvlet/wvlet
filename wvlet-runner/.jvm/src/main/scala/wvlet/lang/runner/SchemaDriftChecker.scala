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
package wvlet.lang.runner

import wvlet.lang.catalog.Profile
import wvlet.lang.catalog.SchemaDriftDetector
import wvlet.lang.catalog.SchemaDriftDetector.TableDrift
import wvlet.lang.compiler.analyzer.SymbolLabeler
import wvlet.lang.compiler.analyzer.TableBindings
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.compiler.transform.PreprocessLocalExpr
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.connector.DBConnector
import wvlet.lang.model.plan.PackageDef
import wvlet.lang.model.plan.TypeDef
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.control.Control
import wvlet.uni.log.LogSupport

/**
  * The result of a schema drift check (#1994)
  *
  * @param checkedTables
  *   number of declared tables that exist in the connected catalog and were diffed
  * @param missingTables
  *   declared tables absent from the catalog. Not drift: declared tables materialize automatically
  *   on the first write
  * @param drifted
  *   tables whose catalog schema differs from the declaration
  */
case class SchemaDriftReport(
    checkedTables: Int,
    missingTables: List[String],
    drifted: List[TableDrift]
):
  def hasDrift: Boolean = drifted.nonEmpty

/**
  * Diffs the `table` shape declarations of a project against the connected catalog (#1994). The
  * catalog is always scanned directly through the connector (never through the ConnectorCatalog
  * metadata cache), so the check reflects the current table schemas
  */
object SchemaDriftChecker extends LogSupport:

  /**
    * Check every table declaration under the given source folders against the connected catalog.
    * Declarations without an `in <catalog>.<schema>` binding are checked against the given default
    * catalog/schema, mirroring how a bare table reference resolves
    */
  def check(
      sourceFolders: List[String],
      workEnv: WorkEnv,
      connector: DBConnector,
      defaultCatalog: String,
      defaultSchema: String,
      dbType: DBType
  ): SchemaDriftReport =
    // Declarations are syntactic, so full typing (which would need catalog metadata for query
    // statements) is unnecessary: parse and label symbols so `table x like y` chains resolve
    val compiler = Compiler(
      CompilerOptions(
        sourceFolders = sourceFolders,
        workEnv = workEnv,
        catalog = Some(defaultCatalog),
        schema = Some(defaultSchema),
        dbType = dbType
      ),
      phases = List(List(ParserPhase, PreprocessLocalExpr, SymbolLabeler))
    )
    val compileResult = compiler.compile()

    var checkedTables = 0
    val missingTables = List.newBuilder[String]
    val drifted       = List.newBuilder[TableDrift]

    compiler
      .localCompilationUnits
      .foreach { unit =>
        given ctx: Context = compileResult.context.withCompilationUnit(unit)
        val tableDecls     =
          unit.unresolvedPlan match
            case p: PackageDef =>
              // The legacy `type <name> in <catalog>.<schema>` form also declares a table shape
              // and resolves reads/writes (TableBindings), so it is checked as well
              p.statements
                .collect {
                  case td: TypeDef if !td.isTrait && (td.isTableDef || td.tableBinding.isDefined) =>
                    td
                }
            case _ =>
              Nil
        tableDecls.foreach { td =>
          val declared = TableBindings
            .declaredColumns(td)
            .map(c => c.columnName.leafName -> c.columnType)
          if declared.nonEmpty then
            val (catalogName, schemaName) = td
              .tableBinding
              .getOrElse((defaultCatalog, defaultSchema))
            val tableName = td.name.name
            connector.getTableDef(catalogName, schemaName, tableName) match
              case None =>
                missingTables += s"${catalogName}.${schemaName}.${tableName}"
              case Some(actual) =>
                checkedTables += 1
                val target = SchemaDriftDetector.reshapeTarget(
                  tableName,
                  td.tableBinding,
                  defaultCatalog,
                  defaultSchema
                )
                val drift = SchemaDriftDetector.diff(target, declared, actual.columns, dbType)
                if drift.hasDrift then
                  drifted += drift
        }
      }
    SchemaDriftReport(checkedTables, missingTables.result(), drifted.result())
  end check

  /**
    * Execute the generated `reshape` migrations of the given drifts against the connected catalog
    * (`--apply`, sqldef/`db:migrate` style). The migrations run through the regular compile-and-
    * execute path — exactly what pasting the generated blocks into a script would do — with the
    * project's declarations in scope. Note that `exclude` drops columns (and their data); callers
    * should surface the generated blocks before applying
    */
  def applyMigrations(
      drifts: List[TableDrift],
      sourceFolders: List[String],
      workEnv: WorkEnv,
      connectorProvider: ConnectorProvider,
      profile: Profile,
      defaultCatalog: String,
      defaultSchema: String,
      dbType: DBType
  ): Unit =
    if drifts.nonEmpty then
      val compiler = Compiler(
        CompilerOptions(
          sourceFolders = sourceFolders,
          workEnv = workEnv,
          catalog = Some(defaultCatalog),
          schema = Some(defaultSchema),
          dbType = dbType
        )
      )
      val connector = connectorProvider.getConnector(profile)
      compiler.setDefaultCatalog(connector.getCatalog(defaultCatalog, defaultSchema))
      compiler.setDefaultSchema(defaultSchema)

      val source = drifts.map(SchemaDriftDetector.migrationSource).mkString("\n")
      val unit   = CompilationUnit.fromWvletString(source)
      val result = compiler.compileSingleUnit(unit)
      result.reportAllErrors
      Control.withResource(QueryExecutor(connectorProvider, profile, workEnv)) { executor =>
        executor.executeSingle(unit, result.context)
      }

end SchemaDriftChecker
