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
package wvlet.lang.compiler.typer

import wvlet.lang.api.Span
import wvlet.lang.api.SourceLocation
import wvlet.lang.compiler.Context
import wvlet.lang.model.expr.Expression
import wvlet.lang.model.expr.Identifier
import wvlet.lang.model.Type

object TyperError:
  /**
    * Severity of a typing diagnostic. Warnings are reported to the user but never fail the
    * compilation, even with failOnTypeErrors (--strict)
    */
  enum Severity:
    case Warning,
      Error

/**
  * Base trait for typing errors
  */
sealed trait TyperError:
  def message: String
  def span: Span
  def severity: TyperError.Severity                      = TyperError.Severity.Error
  def sourceLocation(using ctx: Context): SourceLocation = ctx.sourceLocationAt(span)

/**
  * Error for unresolved identifiers
  */
case class UnresolvedIdentifier(id: Identifier) extends TyperError:
  def message: String = s"Unresolved identifier: ${id.leafName}"
  def span: Span      = id.span

/**
  * Error for type mismatches
  */
case class TypeMismatch(expected: Type, actual: Type, expr: Expression) extends TyperError:
  def message: String = s"Type mismatch: expected $expected, got $actual"
  def span: Span      = expr.span

/**
  * Error for unresolved table references
  */
case class UnresolvedTable(name: String, span: Span) extends TyperError:
  def message: String = s"Unresolved table: $name"

/**
  * Error for invalid filter conditions
  */
case class InvalidFilterCondition(actualType: Type, expr: Expression) extends TyperError:
  def message: String = s"Filter condition must be boolean, got: $actualType"
  def span: Span      = expr.span

/**
  * Generic typing error
  */
case class GenericTyperError(message: String, span: Span) extends TyperError

/**
  * Warning for a type name defined more than once with conflicting table bindings, e.g., `type
  * orders in mydb.sales` and `type orders in mydb.marketing` generated into different files by
  * `wvlet catalog import`. Name lookup silently resolves through only one of the definitions (in
  * file-name order), so the shadowed binding never matches (#93)
  *
  * @param thisBinding
  *   the `catalog.schema` binding of the definition receiving this warning; None when the
  *   definition has no table binding
  * @param otherBinding
  *   the `catalog.schema` binding of the conflicting definition; None when it has no table binding
  * @param otherLocation
  *   the location of the conflicting definition, precomputed from its own source file (the
  *   ctx-based sourceLocation of this trait can only reach the current unit's file)
  * @param winnerFileName
  *   the file whose definition name lookup actually uses (file-name sort order); may be either side
  * @param span
  *   the span of the type definition in the unit receiving the warning
  */
/**
  * Deprecation warning for table shapes declared with the `type` keyword (#1998). A `type` whose
  * body carries columns and resolves a table reference declares a stored relation, whose canonical
  * spelling is `table <name> [in <catalog>.<schema>] = {...}`; `type` remains for method interfaces
  * and aliases
  *
  * @param binding
  *   the `catalog.schema` binding of the definition, when it carries one
  * @param span
  *   the span of the table reference that resolved through the `type` definition
  */
/**
  * Deprecation warning for method interfaces declared with the `type` keyword (#2001). A `type`
  * whose body carries only `def` members declares a method interface, whose canonical spelling is
  * `trait <name> = {...}`; `type` remains for aliases
  *
  * @param span
  *   the span of the `type` definition
  */
case class MethodInterfaceDeclaredAsType(typeName: String, span: Span) extends TyperError:
  override def severity: TyperError.Severity = TyperError.Severity.Warning

  def message: String =
    s"Method interface '${typeName}' is declared with 'type'. Declare method interfaces as " +
      s"'trait ${typeName} = {...}'; def-only 'type' declarations will be removed in a " +
      s"future release"

case class TableShapeDeclaredAsType(typeName: String, binding: Option[String], span: Span)
    extends TyperError:
  override def severity: TyperError.Severity = TyperError.Severity.Warning

  def message: String =
    val inClause = binding.map(b => s" in ${b}").getOrElse("")
    s"Table shape '${typeName}' is declared with 'type'. Declare stored relations as " +
      s"'table ${typeName}${inClause} = {...}'; relation-shaped 'type' declarations " +
      s"will be removed in a future release"

/**
  * Warning for a `drop table`, `reshape`, or `truncate` target that neither the imported static
  * catalog (`table ... in <catalog>.<schema>` declarations from `wvlet catalog import`) nor a live
  * catalog knows (#1999). A warning rather than an error because the imported snapshot may be stale
  * — the table may have been created after the import
  *
  * @param span
  *   the span of the statement's table reference
  */
case class UnknownStaticCatalogTarget(statement: String, tableName: String, span: Span)
    extends TyperError:
  override def severity: TyperError.Severity = TyperError.Severity.Warning

  def message: String =
    s"Table '${tableName}' targeted by '${statement}' is not in the imported catalog. " +
      s"If the table exists, refresh the snapshot with 'wvlet catalog import'"

case class DuplicateTypeDefinition(
    typeName: String,
    thisBinding: Option[String],
    otherBinding: Option[String],
    otherLocation: SourceLocation,
    winnerFileName: String,
    span: Span
) extends TyperError:
  override def severity: TyperError.Severity = TyperError.Severity.Warning

  private def bindingLabel(binding: Option[String]): String = binding
    .map(b => s"bound to ${b}")
    .getOrElse("no table binding")

  def message: String =
    s"Duplicate type definition '${typeName}' (${bindingLabel(
        thisBinding
      )}): also defined at ${otherLocation.locationString} (${bindingLabel(
        otherBinding
      )}). Only the definition in ${winnerFileName} is used for name lookup."
