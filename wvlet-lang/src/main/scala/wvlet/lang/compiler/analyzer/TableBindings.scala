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
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.parser.DataTypeParser
import wvlet.lang.model.expr.ColumnDef
import wvlet.lang.model.expr.UnquotedIdentifier
import wvlet.lang.model.plan.FieldDef
import wvlet.lang.model.plan.TypeDef

/**
  * Shared resolution of table names against table shape declarations (`table t [in c.s] = {...}`
  * and the legacy `type t in c.s` form, #1881/#1998). Reads (RelationRefResolver) and writes
  * (append/save/create-table lowering) resolve through the same matching rules, so a declared name
  * never reads from one location while writing to another.
  */
object TableBindings:

  /**
    * True when a reference qualifier (empty, `schema`, or `catalog.schema`) points at the given
    * `in <catalog>.<schema>` binding. Names match case-insensitively, following SQL identifier
    * semantics. A bare reference matches only when the binding is the context's current
    * catalog/schema, mirroring SQL search-path behavior
    */
  def qualifierMatches(qualifier: List[String], binding: (String, String))(using
      ctx: Context
  ): Boolean =
    def sameName(a: String, b: String): Boolean = a.equalsIgnoreCase(b)
    val (catalog, schema)                       = binding
    qualifier match
      case Nil =>
        sameName(schema, ctx.defaultSchema) && sameName(catalog, ctx.catalog.catalogName)
      case s :: Nil =>
        sameName(s, schema)
      case c :: s :: Nil =>
        sameName(c, catalog) && sameName(s, schema)
      case _ =>
        false

  /**
    * A table shape declaration governing a write or DDL target.
    *
    * @param boundName
    *   the binding-qualified table location when the declaration carries `in <catalog>.<schema>`;
    *   None for unbound declarations, so callers keep their default target naming
    * @param columns
    *   the declared columns, used to fill `create table` actions and auto-create targets
    */
  case class DeclaredTarget(boundName: Option[TableName], columns: List[ColumnDef])

  /**
    * The declaration governing the given write/DDL target name, or None when no declaration
    * applies. A declaration applies exactly when a `from` reference of the same name would resolve
    * through it: a bound declaration matches qualifiers pointing at its binding (and bare names
    * only on the current search path); an unbound declaration matches bare names only
    */
  def declarationFor(targetName: String)(using ctx: Context): Option[DeclaredTarget] =
    val target    = TableName.parse(targetName)
    val qualifier = List(target.catalog, target.schema).flatten
    declarationOf(target.name).flatMap { td =>
      val columns = declaredColumns(td)
      if columns.isEmpty then
        None
      else
        td.tableBinding match
          case Some(binding) if qualifierMatches(qualifier, binding) =>
            Some(
              DeclaredTarget(
                Some(TableName(Some(binding._1), Some(binding._2), target.name)),
                columns
              )
            )
          case Some(_) =>
            // Bound to a different location: the declaration does not govern this reference
            None
          case None if qualifier.isEmpty =>
            Some(DeclaredTarget(None, columns))
          case None =>
            None
    }

  /**
    * The `in <catalog>.<schema>` binding of the declaration registered under the given table name,
    * regardless of whether a reference matches it. Used for actionable error messages when a write
    * targets a declared name outside its bound location
    */
  def declaredBindingOf(tableName: String)(using ctx: Context): Option[(String, String)] =
    declarationOf(tableName).flatMap(_.tableBinding)

  private def declarationOf(tableName: String)(using ctx: Context): Option[TypeDef] = ctx
    .findSymbolByName(Name.typeName(tableName))
    .map(_.tree)
    .collect { case td: TypeDef =>
      td
    }

  /**
    * The declared columns of a table declaration, following `table <name> like <source>` chains
    * (#1995) and `extends` mixin parents (#2012) so declared tables auto-create with the full
    * composed shape. Mixed-in columns come before own body columns, and a column reached through
    * multiple mixin paths dedupes to its first occurrence, mirroring SymbolLabeler's composition
    * rules. A reference cycle or an unknown source resolves to no columns (SymbolLabeler reports
    * the error). Also used by the schema drift check (#1994) so drift is computed against the same
    * columns writes materialize
    */
  def declaredColumns(td: TypeDef)(using ctx: Context): List[ColumnDef] =
    def loop(current: TypeDef, visited: Set[String]): List[ColumnDef] =
      val own = current
        .elems
        .collect { case f: FieldDef =>
          ColumnDef(
            UnquotedIdentifier(f.name.name, f.span),
            // Field types carry their parameters separately (e.g. decimal[10,2] parses as
            // fieldType decimal + params [10, 2]), so pass both to keep parameterized types
            DataTypeParser.parse(f.fieldType.fullName, f.params),
            f.span,
            defaultValue = f.body
          )
        }
      val inherited = (current.parents.map(_.leafName) ++ current.likeSource.map(_.leafName))
        .flatMap { src =>
          if visited.contains(src) then
            Nil
          else
            declarationOf(src).map(srcTd => loop(srcTd, visited + src)).getOrElse(Nil)
        }
      inherited ++ own
    val composed = loop(td, Set(td.name.name))
    val seen     = scala.collection.mutable.Set.empty[String]
    composed.filter(c => seen.add(c.columnName.leafName))

end TableBindings
