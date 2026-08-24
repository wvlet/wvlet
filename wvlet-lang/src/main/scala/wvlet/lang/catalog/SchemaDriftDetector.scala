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
package wvlet.lang.catalog

import wvlet.lang.compiler.DBType
import wvlet.lang.model.DataType

/**
  * Diffs a `table` shape declaration against the actual table schema of a connected catalog and
  * renders the drift as a ready-to-run `reshape` migration block (#1994). All emitted `reshape` ops
  * (`add`, `exclude`, `cast`) have ensure semantics, so a generated block is retry-safe.
  */
object SchemaDriftDetector:

  /** A column whose declared type differs from the type reported by the catalog */
  case class TypeChange(column: String, declaredType: DataType, actualType: DataType)

  /**
    * The drift of one table: the declared columns missing from the catalog (to `add`), the catalog
    * columns absent from the declaration (to `exclude`), and columns whose types differ (to `cast`)
    *
    * @param tableName
    *   the reshape target name, as it should appear in the generated migration
    */
  case class TableDrift(
      tableName: String,
      addColumns: List[(String, DataType)],
      excludeColumns: List[String],
      typeChanges: List[TypeChange]
  ):
    def hasDrift: Boolean = addColumns.nonEmpty || excludeColumns.nonEmpty || typeChanges.nonEmpty

  /**
    * Diff the declared columns against the actual catalog columns. Column names match
    * case-insensitively, following SQL identifier semantics; column order is not compared, as
    * reshape ops cannot reorder columns
    */
  def diff(
      tableName: String,
      declared: Seq[(String, DataType)],
      actual: Seq[Catalog.TableColumn],
      dbType: DBType
  ): TableDrift =
    def norm(name: String): String = name.toLowerCase
    val actualByName               = actual.map(c => norm(c.name) -> c).toMap
    val declaredNames              = declared.map(c => norm(c._1)).toSet

    val addColumns     = declared.filterNot(c => actualByName.contains(norm(c._1))).toList
    val excludeColumns =
      actual
        .collect {
          case c if !declaredNames.contains(norm(c.name)) =>
            c.name
        }
        .toList
    val typeChanges =
      declared
        .flatMap { case (name, declaredType) =>
          actualByName
            .get(norm(name))
            .collect {
              case c if !typesMatch(declaredType, c.dataType, dbType) =>
                TypeChange(name, declaredType, c.dataType)
            }
        }
        .toList
    TableDrift(tableName, addColumns, excludeColumns, typeChanges)

  end diff

  /**
    * True when a declared column type and the type reported by the catalog denote the same storage
    * type. Declared and catalog types both parse through DataTypeParser, but engines widen some
    * types on `create table` (e.g. DuckDB stores an `int` column as BIGINT and reports it back as
    * such), so types also match when they map to the same SQL type for the target engine — the same
    * normalization writes use to materialize declared tables. A declared `any` (the catalog-import
    * fallback for types outside the Wvlet grammar) matches any catalog type
    */
  def typesMatch(declared: DataType, actual: DataType, dbType: DBType): Boolean =
    declared == DataType.AnyType || declared.wvExpr.equalsIgnoreCase(actual.wvExpr) || (
      DataType.toSQLType(declared, dbType).equalsIgnoreCase(DataType.toSQLType(actual, dbType)) &&
        declared.typeParams.size == actual.typeParams.size &&
        declared
          .typeParams
          .zip(actual.typeParams)
          .forall { case (d, a) =>
            typesMatch(d, a, dbType)
          }
    )

  /**
    * The reshape target name of a declaration: the bare declared name when the declaration is
    * unbound or bound to the context catalog/schema, and the fully qualified
    * `<catalog>.<schema>.<name>` otherwise, so the generated block runs against the right location
    * without depending on the session search path
    */
  def reshapeTarget(
      name: String,
      binding: Option[(String, String)],
      defaultCatalog: String,
      defaultSchema: String
  ): String =
    binding match
      case Some((catalog, schema))
          if !(
            catalog.equalsIgnoreCase(defaultCatalog) && schema.equalsIgnoreCase(defaultSchema)
          ) =>
        s"${StaticCatalogExporter.quote(catalog)}.${StaticCatalogExporter.quote(
            schema
          )}.${StaticCatalogExporter.quote(name)}"
      case _ =>
        StaticCatalogExporter.quote(name)

  /** Render one table's drift as a message with a ready-to-paste `reshape` migration block */
  def render(drift: TableDrift): String =
    val quote = StaticCatalogExporter.quote
    val ops   = List.newBuilder[String]
    drift
      .addColumns
      .foreach { case (name, tpe) =>
        ops += s"add ${quote(name)}: ${tpe.wvExpr}"
      }
    drift
      .excludeColumns
      .foreach { name =>
        ops += s"exclude ${quote(name)}"
      }
    drift
      .typeChanges
      .foreach { tc =>
        // The previous type in a trailing comment, so a reviewer sees what the cast changes
        ops +=
          s"cast ${quote(tc.column)} as ${tc.declaredType.wvExpr} -- the catalog has ${tc
              .actualType
              .wvExpr}"
      }
    // Renames cannot be inferred from a diff: they appear as an exclude/add pair, which would
    // drop the column data. Hint at the hand-written alternative
    if drift.addColumns.nonEmpty && drift.excludeColumns.nonEmpty then
      ops +=
        "-- If an exclude/add pair is actually a rename, use `rename <old> as <new>` instead to preserve data"
    val body = ops.result().map(op => s"    ${op}").mkString("\n")
    s"""table ${drift.tableName} has drifted from its declaration. To migrate, run:
       |  reshape ${drift.tableName} {
       |${body}
       |  }""".stripMargin

  end render

end SchemaDriftDetector
